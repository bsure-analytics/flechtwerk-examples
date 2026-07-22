"""Generation mix — a co-partitioned join across every series *at the same instant*.

Stage 2. It consumes ``smard-observations`` (both observations and settled markers), all
keyed by the interval instant, and folds them into one **mix** record per interval on
``smard-mix``: total generation, renewables share, an estimated CO₂ intensity, load,
residual load, and price.

**Why this is a co-partitioned join — with time as the key.** GDELT joins Events ⋈
Mentions on ``GlobalEventID``; GTFS joins updates ⋈ profiles on ``trip_id``. Here the
join key is the **quarter-hour instant**: every series' observation for 12:00 shares the
key ``2026-…T12:00:00Z``, so they hash to the same partition → the same task → the same
state bucket, and one ``transform`` sees them one at a time against the accumulating mix.
The state bucket for an interval holds one ``CONTRIBUTIONS`` entry per series that has
reported it — role, source, latest value — and every observation re-emits the mix with
``IS_FINAL = false``: a preliminary picture that fills in as more series report and gets
*corrected* whenever a revision restates a value.

**The settlement lifecycle (the example's point).** An interval keeps accumulating and
revising for as long as it sits inside the 48 h revision window. When ingest's marker
series ages it out, ingest emits a ``settled`` marker for it; this stage then re-emits
the interval one last time with ``IS_FINAL = true`` and **tombstones its join state**
(a falsy ``State()``), so the store stays bounded to the live window (~48 h of past
intervals plus whatever future the day-ahead price reaches) instead of growing one key
per quarter-hour forever. Because the marker is produced strictly after every revision to
its interval, and by the same single ingest process (so production order = offset order),
no revision for an interval can arrive after its marker — the finalization is race-free
for a single ingest instance (running several would need per-series markers).

**Safety net.** An observation whose interval is already older than ``REVISION_WINDOW``
at the time it was fetched is passed straight through as a preliminary row but **builds no
join state** — this lets ingest's one-time bootstrap backfill (up to a week of history)
reach ClickHouse without accreting join keys that will never receive a settle marker, and
kills any post-settle straggler. Mix rows therefore begin ~48 h before startup; the raw
observation history goes back further. One accepted residue: a data gap in the marker
series leaks that one interval's tiny join state (self-limiting) — the documented
trade-off, GDELT-orphan-TTL in spirit.

Event time is the triggering record's ``FETCHED_AT`` (never wall-clock), so
:func:`assemble_mix` is pure and the logic tier drives every branch.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from flechtwerk import Event, IncomingMessage, Message, State, transformer

from .attributes import (
    C_ROLE,
    C_SOURCE,
    C_VALUE,
    CO2_G_PER_KWH,
    CONTRIBUTIONS,
    FETCHED_AT,
    GENERATION,
    INTERVAL_TS,
    IS_FINAL,
    N_SOURCES,
    KIND,
    LOAD_MWH,
    PRICE_EUR_MWH,
    RENEWABLES_SHARE,
    RESIDUAL_LOAD_MWH,
    ROLE,
    SERIES_KEY,
    SOURCE,
    TOTAL_GENERATION_MWH,
    UPDATED_AT,
    VALUE,
)
from .ingest import OBSERVATIONS_TOPIC, REVISION_WINDOW

log = logging.getLogger(__name__)

MIX_TOPIC = "smard-mix"

SOURCE_META: dict[str, tuple[bool, float]] = {
    # (renewable?, lifecycle g CO₂eq/kWh). IPCC AR5 WG3 Annex III medians for the
    # renewables and gas/coal; lignite an engineering estimate; other_* rounded
    # placeholders. ILLUSTRATIVE, not official — see the README's caveat.
    "lignite":            (False, 1050.0),
    "hard_coal":          (False,  820.0),
    "gas":                (False,  490.0),
    "other_conventional": (False,  700.0),
    "pumped_storage":     (False,    0.0),  # stored energy; a 0 factor avoids double counting its charge mix
    "biomass":            (True,   230.0),
    "hydro":              (True,    24.0),
    "wind_onshore":       (True,    11.0),
    "wind_offshore":      (True,    12.0),
    "solar":              (True,    45.0),
    "other_renewable":    (True,    30.0),
}

# non-source roles, mapped to the mix column each lands in.
_SCALAR_ROLE_ATTR = {"load": LOAD_MWH, "residual_load": RESIDUAL_LOAD_MWH, "price": PRICE_EUR_MWH}


def assemble_mix(interval: datetime, contributions: dict[str, dict], updated_at: datetime,
                 *, is_final: bool) -> Event:
    """Project the interval's accumulated contributions into a mix record — pure.

    Sums the generation sources present so far into ``TOTAL_GENERATION_MWH`` (undercounting
    until every source has reported — that is what ``IS_FINAL = false`` means), derives the
    renewables share and the generation-weighted CO₂ intensity over them, and passes
    load / residual load / price through to their columns when present. Absent aggregates
    stay absent (never a fabricated 0 — the GDELT-sink rule).
    """
    record = Event({INTERVAL_TS: interval, IS_FINAL: is_final, UPDATED_AT: updated_at})
    generation: dict[str, float] = {}
    total = renewable = co2_weighted = 0.0
    for entry in contributions.values():
        role, value = entry[C_ROLE], entry[C_VALUE]
        if role == "source":
            source = entry.get(C_SOURCE)
            if source is None:
                continue
            generation[source] = value
            total += value
            renewable_flag, factor = SOURCE_META.get(source, (False, 0.0))
            co2_weighted += value * factor
            if renewable_flag:
                renewable += value
        elif (attr := _SCALAR_ROLE_ATTR.get(role)) is not None:
            record[attr] = value
    record[N_SOURCES] = len(generation)
    if generation:
        record[GENERATION] = generation
    if total > 0:
        record[TOTAL_GENERATION_MWH] = total
        record[RENEWABLES_SHARE] = renewable / total
        record[CO2_G_PER_KWH] = co2_weighted / total  # MWh weights cancel → g/kWh
    return record


def _entry(value: Event) -> dict[str, Any]:
    """The ``CONTRIBUTIONS`` entry for one observation: its role, optional source, value."""
    entry: dict[str, Any] = {C_ROLE: value[ROLE], C_VALUE: value[VALUE]}
    if (source := value.get(SOURCE)) is not None:
        entry[C_SOURCE] = source
    return entry


async def run_mix(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Fold one observation-or-marker into the interval's mix record.

    A ``settled`` marker with state present emits the final record and tombstones the
    bucket; with no state it is a no-op (the interval was bootstrap-aged, or its marker
    replays after the tombstone). An observation is merged and the preliminary mix
    re-emitted; the new bucket state is persisted unless the interval is already past the
    revision window (the safety net — a correcting row still flows, but no join state
    accretes for an interval that will never settle). Pure and I/O-free.
    """
    interval = msg.value[INTERVAL_TS]
    fetched_at = msg.value[FETCHED_AT]

    if msg.value[KIND] == "settled":
        contributions = state.get(CONTRIBUTIONS)
        if not contributions:
            return  # bootstrap-aged interval, or a marker replayed after its tombstone
        yield Message(key=msg.key, topic=MIX_TOPIC,
                      value=assemble_mix(interval, contributions, fetched_at, is_final=True))
        yield State()  # tombstone — the interval can no longer change
        return

    contributions = {k: dict(v) for k, v in (state.get(CONTRIBUTIONS) or {}).items()}
    contributions[msg.value[SERIES_KEY]] = _entry(msg.value)
    yield Message(key=msg.key, topic=MIX_TOPIC,
                  value=assemble_mix(interval, contributions, fetched_at, is_final=False))
    if fetched_at - interval <= REVISION_WINDOW:
        yield State({CONTRIBUTIONS: contributions, UPDATED_AT: fetched_at})
    # else: safety net — the correcting preliminary row is emitted, but no join state is
    # built for an interval too old to ever receive a settle marker.


@transformer(input_topics=[OBSERVATIONS_TOPIC])
async def mix(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in run_mix(state, msg):
        yield item


stage = mix
"""The stage the dispatcher runs (``python -m examples.smard_german_electricity_market mix``)."""

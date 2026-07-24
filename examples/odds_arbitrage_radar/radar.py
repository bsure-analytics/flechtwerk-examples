"""The radar — a per-pair fan-in that watches two venues' quotes for a cross-venue arb.

Stage 2. It consumes ``odds-quotes`` (both venues' quotes, all keyed by the pair key) and
folds them into a per-pair join state — ``LEGS = {venue: latest quote fields}`` — re-running
the arbitrage math on **every** quote update and emitting to ``odds-margins`` (the continuous
"distance to free money" the board plots) and, when a *fresh* net-positive edge appears, to
``odds-signals``.

**Why this is an N-source fan-in — the example's point.** SMARD joins series on *time* and
GTFS joins updates on *trip*; here two independent extractor **processes** produce quotes for
the same pair to the same partitioned topic, keyed alike, so both venues' quotes hash to one
partition → one task → one state bucket. A single ``transform`` sees them one at a time
against the accumulating ``LEGS`` and recomputes a derived condition — a materialized
"watchdog" view, not a stream-to-stream join.

**Event-time staleness (the other point).** The framework has no timers; the pollers own the
clock and stamp each quote's ``FETCHED_AT``. When a quote triggers a computation, the radar
compares its event time against the *other* leg's stored event time: if the other leg is
older than ``STALE_AFTER``, every margin from this update is flagged ``fresh = false`` and
**cannot signal** — a 10-minute-old quote must never manufacture an arb against a fresh one.
A margin still flows (the chart wants continuity); only signalling is gated.

**Lifecycle.** A ``closed`` quote (settled, halted, delisted) tombstones the pair's join
state (a falsy ``State()``), keeping the store bounded to live markets. If the other venue
keeps quoting the pair after that, it rebuilds a one-legged state that computes nothing and
tombstones again on its own close — bounded, and the accepted residue is documented in the
README. A closed quote with no state is a no-op (a replay after the tombstone).

Event time is the triggering quote's ``FETCHED_AT`` (never wall-clock), so :func:`run_radar`
is pure and I/O-free — the logic tier drives every branch.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from flechtwerk import Event, IncomingMessage, Message, State, transformer
from flechtwerk.attribute import DATETIME

from .arbitrage import Quote, compute_margins
from .attributes import (
    COMPUTED_AT,
    DIRECTION,
    EXECUTABLE_SIZE,
    FEE_RATE,
    FEES,
    FETCHED_AT,
    FRESH,
    GROSS_EDGE,
    L_FEE_RATE,
    L_FETCHED_AT,
    L_NO_ASK,
    L_NO_ASK_SIZE,
    L_TITLE,
    L_YES_ASK,
    L_YES_ASK_SIZE,
    LEGS,
    MARGINS_TOPIC,
    NET_EDGE,
    NO_ASK,
    NO_ASK_SIZE,
    PAIR_KEY,
    QUOTES_TOPIC,
    SIGNALS_TOPIC,
    STATUS,
    TITLE,
    VENUE,
    YES_ASK,
    YES_ASK_SIZE,
)

log = logging.getLogger(__name__)

POLYMARKET = "polymarket"
KALSHI = "kalshi"

STALE_AFTER = timedelta(minutes=5)
"""Freshness horizon: a margin is ``fresh`` only if the two legs were fetched within this of
each other. At the 30 s poll cadence that is ~10 polls — generous enough to tolerate a poll
hiccup, tight enough that a wedged extractor's frozen quotes stop signalling quickly."""

MIN_EDGE = 0.0
"""Net-edge threshold for a signal. ``0.0`` = "any profit after fees". Raise it to demand a
margin of safety (slippage, the resolution-mismatch risk the README warns about)."""


def _leg(value: Event) -> dict:
    """The ``LEGS`` entry for one venue's quote: just the fields the margin math needs.

    Stores asks, ask sizes, fee rate, and title; ``fetched_at`` as its DATETIME-encoded ISO
    string (a State nests only JSON scalars). An absent ask/size is simply not stored."""
    entry: dict[str, object] = {
        L_FEE_RATE: value[FEE_RATE],
        L_TITLE: value[TITLE],
        L_FETCHED_AT: DATETIME.encode(value[FETCHED_AT]),
    }
    for attr, key in ((YES_ASK, L_YES_ASK), (NO_ASK, L_NO_ASK),
                      (YES_ASK_SIZE, L_YES_ASK_SIZE), (NO_ASK_SIZE, L_NO_ASK_SIZE)):
        if (v := value.get(attr)) is not None:
            entry[key] = v
    return entry


def _quote(venue: str, entry: dict) -> Quote:
    """Reconstruct an ``arbitrage.Quote`` from a stored ``LEGS`` entry."""
    return Quote(
        venue=venue, title=entry.get(L_TITLE, ""), fee_rate=entry[L_FEE_RATE],
        yes_ask=entry.get(L_YES_ASK), no_ask=entry.get(L_NO_ASK),
        yes_ask_size=entry.get(L_YES_ASK_SIZE), no_ask_size=entry.get(L_NO_ASK_SIZE),
    )


def _margin_record(pair_key: str, title: str, margin, *, fresh: bool, computed_at: datetime) -> Event:
    """Project one ``arbitrage.Margin`` into a wire record (shared by margins and signals)."""
    record = Event({
        PAIR_KEY: pair_key, TITLE: title, DIRECTION: margin.direction,
        YES_ASK: margin.yes_ask, NO_ASK: margin.no_ask,
        GROSS_EDGE: margin.gross_edge, FEES: margin.fees, NET_EDGE: margin.net_edge,
        FRESH: fresh, COMPUTED_AT: computed_at,
    })
    if margin.executable_size is not None:
        record[EXECUTABLE_SIZE] = margin.executable_size
    return record


async def run_radar(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Fold one quote into the pair's join state and emit margins/signals — pure, I/O-free.

    A ``closed`` quote tombstones the bucket (or no-ops if already empty). An active quote is
    merged into ``LEGS[venue]``; if the other venue is present, each computable direction is
    emitted to ``odds-margins`` (with a freshness flag from the two legs' event times), and a
    *fresh* net-positive one additionally to ``odds-signals``. The updated state is yielded
    last, so the messages and the state change commit together."""
    value = msg.value
    venue = value[VENUE]

    if value[STATUS] == "closed":
        if state.get(LEGS):
            log.info("%s closed on %s — tombstoning pair state", msg.key, venue)
            yield State()  # the market can no longer be traded — drop the join state
        return

    legs = {v: dict(entry) for v, entry in (state.get(LEGS) or {}).items()}
    legs[venue] = _leg(value)

    other = KALSHI if venue == POLYMARKET else POLYMARKET
    if other in legs:
        trigger_fetched = value[FETCHED_AT]
        other_fetched = datetime.fromisoformat(legs[other][L_FETCHED_AT])
        fresh = (trigger_fetched - other_fetched) <= STALE_AFTER
        polymarket, kalshi = _quote(POLYMARKET, legs[POLYMARKET]), _quote(KALSHI, legs[KALSHI])
        for margin in compute_margins(polymarket, kalshi):
            record = _margin_record(value[PAIR_KEY], value[TITLE], margin,
                                    fresh=fresh, computed_at=trigger_fetched)
            yield Message(key=msg.key, topic=MARGINS_TOPIC, value=record)
            if fresh and margin.net_edge > MIN_EDGE:
                log.info("SIGNAL %s %s: net_edge=%.4f", msg.key, margin.direction, margin.net_edge)
                yield Message(key=msg.key, topic=SIGNALS_TOPIC, value=record)

    yield State({LEGS: legs})


@transformer(input_topics=[QUOTES_TOPIC])
async def radar(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in run_radar(state, msg):
        yield item


stage = radar
"""The stage the dispatcher runs (``python -m examples.odds_arbitrage_radar radar``)."""

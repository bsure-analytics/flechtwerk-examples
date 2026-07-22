"""Typed attributes for the SMARD German electricity-market example.

Unlike GTFS (an uncontrolled protobuf upstream) or GDELT (headerless TSV), the wire
records here are **ours**: SMARD's raw payload is only ``[epoch_ms, value]`` pairs, so
the stages *construct* every record from scratch. There is no uncontrolled upstream to
spread through — so, deliberately, every field an observation or a mix record carries
is a declared ``Attribute``. The framework's "declare only what you compute with" rule
is about not re-declaring a foreign schema you don't own; here we own the whole schema.

Timestamps are real ``datetime``s at the typed edge (the ``DATETIME`` codec renders
ISO-8601, which ClickHouse ingests directly). Every instant is aware UTC. The
authoritative **event time** is ``FETCHED_AT`` — the server's ``Date`` at the poll that
produced the record — never wall-clock read inside a stage, so every code path stays
drivable from the logic tier.
"""
from typing import Final

from flechtwerk.attribute import ANY, Attribute, BOOL, DATETIME, DICT, FLOAT, INT, STR

# --- Config topic (one record per series to poll; the wire key is "{filter}_{region}_{resolution}") ---

FILTER: Final = Attribute("filter", INT)
"""The SMARD ``filter`` id — the numeric series id in the chart-data URL (e.g. ``4068``
= photovoltaics). Injected by ``setup.py``; any producer may add or change one live."""
REGION: Final = Attribute("region", STR)
"""The SMARD region/bidding-zone in the URL — ``DE`` for generation and load,
``DE-LU`` for the day-ahead price (the German-Luxembourg bidding zone)."""
RESOLUTION: Final = Attribute("resolution", STR)
"""The SMARD time resolution — ``quarterhour`` throughout the demo (``hour`` is an
extension point). Selects both the index file and the week-file naming."""
SERIES_NAME: Final = Attribute("name", STR)
"""Human-readable series name (``Photovoltaics``, ``Grid load``, …) — carried onto every
observation for the dashboard and the corrections feed; not otherwise computed with."""
ROLE: Final = Attribute("role", STR)
"""What the series contributes to the mix: ``source`` (a generation source),
``load``, ``residual_load``, or ``price``. Drives how ``mix`` folds it in."""
SOURCE: Final = Attribute("source", STR, optional=True)
"""For a ``role=source`` series, the canonical generation source
(``solar`` / ``wind_onshore`` / ``lignite`` / … — see ``mix.SOURCE_META``). Absent for
load/price. It keys the renewables-share and CO₂ weighting."""
UNIT: Final = Attribute("unit", STR)
"""The value's unit (``MWh`` for energy per quarter-hour, ``EUR/MWh`` for price) —
carried through for the dashboard; the mix math is unit-aware by ``role``, not by this."""
SETTLE_MARKER: Final = Attribute("settle_marker", BOOL, optional=True)
"""Whether this series drives settlement: exactly one config record sets it (the grid
load, which always has fresh data). As one of its intervals ages out of the revision
window, ``ingest`` emits a ``settled`` marker that finalizes that interval's mix."""

# --- Wire fields (smard-observations: observations and settled markers) ---

KIND: Final = Attribute("kind", STR)
"""``observation`` (a new or corrected data point) or ``settled`` (a punctuation marker
that an interval has aged out of the revision window). The mix stage branches on it, and
the ClickHouse observations views filter to ``observation``."""
SERIES_KEY: Final = Attribute("series_key", STR)
"""The config wire key (``"{filter}_{region}_{resolution}"``) — the series identity on
every observation, the mix join-state sub-key, and the ClickHouse ``(series_key, ts)``
dedup key."""
INTERVAL_TS: Final = Attribute("interval_ts", DATETIME)
"""The quarter-hour instant this record is about (aware UTC) — the message key (rendered
by the ``DATETIME`` codec, so key and attribute coincide byte-for-byte) and the mix join
key. Time *is* the join key here."""
VALUE: Final = Attribute("value", FLOAT)
"""The measured value at ``INTERVAL_TS`` — MWh per quarter-hour, or €/MWh for price.
``float()``-coerced at the edge (SMARD JSON numbers may parse as ``int``; the ``FLOAT``
codec is exact-type)."""
REVISED: Final = Attribute("revised", BOOL)
"""Whether this observation *restates* a value SMARD published before (a correction) as
opposed to a first publication. Drives the corrections-feed audit table."""
PREVIOUS_VALUE: Final = Attribute("previous_value", FLOAT, optional=True)
"""On a revision, the value this observation replaces — the other half of the audit
trail. Absent on a first publication."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""The server ``Date`` of the poll that produced this record (aware UTC) — the event
time, the ReplacingMergeTree version (a later correction wins), and the settle clock the
mix safety-net compares against. Never a wall-clock read inside a stage."""

# --- Ingest per-series resume state ---

BOOTSTRAPPED: Final = Attribute("bootstrapped", BOOL)
"""Set on the first successful poll. An explicit flag (not "window is empty"), so a
stalled or dead series never re-bootstraps and re-floods its whole week file."""
WINDOW: Final = Attribute("window", DICT(FLOAT))
"""The diff baseline: ``{interval_ts_iso: value}`` for every non-null point still inside
the revision window. ≤192 floats per quarter-hour series — small enough to be the whole
resume cursor; a poll diffs the fresh snapshot against it to find new/revised points."""

# --- Mix output (smard-mix) ---

IS_FINAL: Final = Attribute("is_final", BOOL)
"""``False`` for a preliminary mix (still accumulating / still revisable), ``True`` once
a ``settled`` marker has finalized the interval. The ReplacingMergeTree keeps the latest
by ``UPDATED_AT``, and the final always has the newest one."""
TOTAL_GENERATION_MWH: Final = Attribute("total_generation_mwh", FLOAT, optional=True)
"""Sum of the generation sources present so far this interval (MWh per quarter-hour).
Optional: a preliminary mix that has seen only load/price has no generation yet."""
RENEWABLES_SHARE: Final = Attribute("renewables_share", FLOAT, optional=True)
"""Renewable generation ÷ total generation (0–1), over the sources present so far."""
CO2_G_PER_KWH: Final = Attribute("co2_g_per_kwh", FLOAT, optional=True)
"""Generation-weighted average lifecycle CO₂ intensity (g CO₂eq/kWh) — illustrative
factors from ``mix.SOURCE_META``, not an official figure (see the README)."""
LOAD_MWH: Final = Attribute("load_mwh", FLOAT, optional=True)
"""Grid load (Netzlast) at the interval, when the load series has reported it."""
RESIDUAL_LOAD_MWH: Final = Attribute("residual_load_mwh", FLOAT, optional=True)
"""Residual load (load minus renewable infeed) at the interval, when reported."""
PRICE_EUR_MWH: Final = Attribute("price_eur_mwh", FLOAT, optional=True)
"""Day-ahead price (€/MWh) at the interval, when reported — published a day ahead, so a
preliminary mix for a future interval may carry only this."""
GENERATION: Final = Attribute("generation", DICT(FLOAT), optional=True)
"""Per-source generation this interval, ``{source: mwh}`` — the breakdown behind
``TOTAL_GENERATION_MWH``, queryable in ClickHouse as ``payload.generation.<source>``."""
N_SOURCES: Final = Attribute("n_sources", INT)
"""How many generation sources have reported for this interval so far — the explicit
completeness signal. A preliminary mix undercounts until this reaches the full roster;
the dashboard headlines ratios (renewables share, CO₂) only at the complete count, so a
still-filling interval never shows a lopsided fossil-only ratio."""
UPDATED_AT: Final = Attribute("updated_at", DATETIME)
"""Event time of the observation (or settle marker) that produced this mix record — the
ReplacingMergeTree version column."""

# --- Mix join state (per-interval bucket): series_key -> {role, source?, value} ---

CONTRIBUTIONS: Final = Attribute("contributions", DICT(DICT(ANY)))
"""The interval's accumulating join state: one entry per contributing series, holding its
role, optional source, and latest value. Carried forward across observations and cleared
(tombstoned) when the interval settles."""

# Raw keys inside a CONTRIBUTIONS entry (read at the compute site, not declared as
# attributes — they live inside the DICT(DICT(ANY)) and never collide with ours).
C_ROLE: Final = "role"
C_SOURCE: Final = "source"
C_VALUE: Final = "value"

"""Typed attributes and topic names for the Odds Arbitrage Radar example.

Like SMARD (and unlike GTFS/GDELT, which spread a foreign upstream schema), the wire
records here are **ours**: the venue extractors read Polymarket's and Kalshi's raw JSON at
the edge and *construct* a normalized quote from scratch, and the radar constructs the
margin/signal records. There is no foreign schema to spread through — so, deliberately,
every field a quote, a margin, or the join state carries is a declared ``Attribute``. The
framework's "declare only what you compute with" rule is about not re-declaring a schema
you don't own; here we own the whole schema.

Prices are dollars per $1 payout (a binary contract settles at $1 or $0), so every price
is a probability-like ``FLOAT`` in ``(0, 1)``. Sizes are contract counts (``FLOAT`` — the
venues report fractional sizes). The authoritative **event time** is ``FETCHED_AT`` — the
server ``Date`` at the poll that produced the record — never a wall-clock read inside a
stage, so every code path stays drivable from the logic tier. Timestamps are aware-UTC
``datetime``s at the typed edge (the ``DATETIME`` codec renders ISO-8601, which ClickHouse
ingests directly).
"""
from typing import Final

from flechtwerk.attribute import ANY, Attribute, BOOL, DATETIME, DICT, FLOAT, STR

# --- Topics (the wire contract; both extractors and the transformer share it) ---

PAIRS_TOPIC: Final = "odds-pairs"
"""Compacted config topic, one record per curated pair to watch (keyed by the Polymarket
slug), seeded by nobody — a user requests each with ``uv run poe request-pair`` (or any
producer, Kafbat included). Each record names one Polymarket market and the Kalshi market
that resolves the same real-world outcome."""
QUOTES_TOPIC: Final = "odds-quotes"
"""Partitioned quote stream, keyed by the **pair key** (the Polymarket slug). Both venue
extractors produce here, so a pair's Polymarket and Kalshi quotes co-partition onto one
radar task — the N-source fan-in that lets one ``transform`` see both legs against the
accumulating per-pair state."""
MARGINS_TOPIC: Final = "odds-margins"
"""Continuous derived stream: one record per computable arbitrage direction per quote
update — the "distance to free money" the dashboard plots, emitted every poll (fresh or
stale)."""
SIGNALS_TOPIC: Final = "odds-signals"
"""Sparse alert stream: the subset of margin records that are both *fresh* and net-positive
after fees — an actual (paper) cross-venue arbitrage. Usually empty; that is the point."""

# --- Config record (odds-pairs; wire key = polymarket_slug) ---

POLYMARKET_SLUG: Final = Attribute("polymarket_slug", STR)
"""The Polymarket Gamma market ``slug`` — the poll target for the Polymarket extractor and
**the pair key** (the wire key of the config record, and the key of every quote/margin the
pair produces). Duplicated into the config value so a stage reads it without decoding the
Kafka key (SMARD duplicates its key fields the same way)."""
KALSHI_TICKER: Final = Attribute("kalshi_ticker", STR)
"""The Kalshi market ticker (e.g. ``KXMLBGAME-26JUL261410COLMIL-MIL``) — the poll target
for the Kalshi extractor. The Kalshi extractor still keys its output by the Polymarket slug
(the shared pair key), so the two venues' quotes co-partition."""
YES_OUTCOME: Final = Attribute("yes_outcome", STR)
"""The Polymarket outcome string (one of the market's two ``outcomes``) that resolves the
SAME event as Kalshi's YES side — e.g. ``"Milwaukee Brewers"`` when the Kalshi market is
"Colorado vs Milwaukee Winner? / Milwaukee". This is the cross-venue entity resolution the
**user** curates; getting it backwards inverts every margin (see ``request.py``'s guard)."""
POLYMARKET_FEE_RATE: Final = Attribute("polymarket_fee_rate", FLOAT, optional=True)
"""Optional per-pair override of the Polymarket taker-fee rate. Absent → the extractor's
module default (``polymarket.DEFAULT_FEE_RATE``, the sports rate). Polymarket's rate varies
by category (0.00 geopolitics … 0.07 crypto); override when a pair is not a sports market."""
KALSHI_FEE_RATE: Final = Attribute("kalshi_fee_rate", FLOAT, optional=True)
"""Optional per-pair override of the Kalshi taker-fee rate. Absent → the extractor's module
default (``kalshi.DEFAULT_FEE_RATE``, the general-markets 0.07)."""

# --- Quote record (odds-quotes; one per venue per poll, keyed by the pair key) ---

KIND: Final = Attribute("kind", STR)
"""``quote`` on ``odds-quotes`` — the record's self-description (there is one kind today; it
keeps the schema self-documenting and the ClickHouse views explicit, mirroring SMARD)."""
PAIR_KEY: Final = Attribute("pair_key", STR)
"""The pair identity (the Polymarket slug) carried on every quote/margin/signal — the Kafka
key, the radar join-state identity, and the ClickHouse grouping key."""
VENUE: Final = Attribute("venue", STR)
"""Which venue this quote is from: ``polymarket`` or ``kalshi`` (see ``radar.POLYMARKET`` /
``radar.KALSHI``). The radar folds a quote into ``LEGS[venue]``."""
TITLE: Final = Attribute("title", STR)
"""The venue's human title for the market (Polymarket's ``question`` / Kalshi's ``title``)
— carried through for the dashboard; not computed with."""
STATUS: Final = Attribute("status", STR)
"""Normalized market status: ``active`` (tradeable) or ``closed`` (settled, halted, or no
longer accepting orders). A ``closed`` quote tombstones the pair's radar join state."""
YES_BID: Final = Attribute("yes_bid", FLOAT, optional=True)
"""Best bid on the YES side ($/contract) — carried for the dashboard, not used in the arb
math (arbitrage buys at the *ask*). Absent when that side of the book is empty."""
YES_ASK: Final = Attribute("yes_ask", FLOAT, optional=True)
"""Best ask on the YES side ($/contract) — the price to BUY one YES contract. Absent when
the ask side is empty (a one-sided book). Reused on the margin record as the ask paid on
that direction's YES leg."""
NO_BID: Final = Attribute("no_bid", FLOAT, optional=True)
"""Best bid on the NO side ($/contract) — dashboard only. Absent when that side is empty."""
NO_ASK: Final = Attribute("no_ask", FLOAT, optional=True)
"""Best ask on the NO side ($/contract) — the price to BUY one NO contract. Absent when the
ask side is empty. Reused on the margin record as the ask paid on that direction's NO leg."""
YES_ASK_SIZE: Final = Attribute("yes_ask_size", FLOAT, optional=True)
"""Contracts available at ``YES_ASK`` — how much of the YES leg is executable at that price.
Absent when ``YES_ASK`` is."""
NO_ASK_SIZE: Final = Attribute("no_ask_size", FLOAT, optional=True)
"""Contracts available at ``NO_ASK``. For Kalshi this is the *crosswise* size (the NO book
is the YES book viewed from the other side; see ``kalshi.normalize_kalshi``)."""
FEE_RATE: Final = Attribute("fee_rate", FLOAT)
"""The venue's taker-fee rate for this pair, stamped by the extractor (config override or
module default). It rides on the quote so the radar's fee math stays a pure function of its
inputs — never an environment or clock read."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""The server ``Date`` of the poll that produced this quote (aware UTC) — the event time.
The radar compares the two legs' ``FETCHED_AT`` to decide freshness; a margin computed
against a stale other leg is flagged and never signals."""

# --- Margin / signal record (odds-margins, odds-signals) ---
# (PAIR_KEY, TITLE, YES_ASK, NO_ASK reused from the quote schema — same meaning: a price.)

DIRECTION: Final = Attribute("direction", STR)
"""Which cross-venue leg pairing this margin is for: ``polymarket_yes+kalshi_no`` (buy YES
on Polymarket, NO on Kalshi) or ``kalshi_yes+polymarket_no`` (see ``arbitrage``). The two
directions price independently — books are not symmetric — so each is its own record."""
GROSS_EDGE: Final = Attribute("gross_edge", FLOAT)
"""``1 − (yes_ask + no_ask)`` for this direction: the guaranteed profit per $1 payout BEFORE
fees. Positive means the two legs together cost less than the $1 exactly one of them pays."""
FEES: Final = Attribute("fees", FLOAT)
"""Combined taker fees for one contract on each leg, per the parabolic ``rate·p·(1−p)``
formula (see ``arbitrage.fee``) — what stands between the gross edge and real money."""
NET_EDGE: Final = Attribute("net_edge", FLOAT)
"""``gross_edge − fees``: the profit per $1 payout after fees. A *fresh* positive net edge
is an arbitrage signal. Routinely negative even when gross is positive — the demo's point."""
EXECUTABLE_SIZE: Final = Attribute("executable_size", FLOAT, optional=True)
"""``min`` of the two legs' ask sizes — how many contract-pairs the edge is actually good
for at top-of-book. Absent when either leg's size is unknown (depth beyond top-of-book is
out of scope). The margin is real but its scale is unquantified."""
FRESH: Final = Attribute("fresh", BOOL)
"""Whether the two legs were fetched within ``radar.STALE_AFTER`` of each other. A stale
pairing still emits a margin (for the chart) but is barred from signalling — a 10-minute-old
quote must not fabricate an arb against a fresh one."""
COMPUTED_AT: Final = Attribute("computed_at", DATETIME)
"""Event time of the quote that triggered this computation (its ``FETCHED_AT``) — the
ReplacingMergeTree-free append time on the margin/signal tables and the x-axis of the board.
Derived from event time, never wall-clock, so the logic tier drives it deterministically."""

# --- Radar join state (per-pair bucket): venue -> {leg fields} ---

LEGS: Final = Attribute("legs", DICT(DICT(ANY)))
"""The pair's accumulating join state: one entry per venue that has quoted, holding just the
fields the margin math needs (asks, ask sizes, fee rate, title, fetched-at ISO string).
Carried forward across quotes and cleared (tombstoned) when either venue reports the market
closed — like SMARD's ``CONTRIBUTIONS`` bucket, bounded by the market's lifecycle."""

# Raw keys inside a LEGS entry (read at the compute site, not declared as attributes — they
# live inside the DICT(DICT(ANY)) and never collide with ours). fetched_at is stored as its
# DATETIME-encoded ISO string (a State nests only JSON scalars, so it round-trips as text).
L_YES_ASK: Final = "yes_ask"
L_NO_ASK: Final = "no_ask"
L_YES_ASK_SIZE: Final = "yes_ask_size"
L_NO_ASK_SIZE: Final = "no_ask_size"
L_FEE_RATE: Final = "fee_rate"
L_TITLE: Final = "title"
L_FETCHED_AT: Final = "fetched_at"

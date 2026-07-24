"""Odds Arbitrage Radar — a two-venue fan-in that hunts cross-venue arbitrage.

Three host processes over two keyless public prediction-market APIs (Polymarket + Kalshi):

* ``polymarket`` / ``kalshi`` — two independent Extractors. Each polls one venue for every
  curated pair (a config record on the compacted ``odds-pairs`` topic naming a Polymarket
  ``slug`` + the Kalshi ``ticker`` that resolves the same event), normalizes its order book
  to a top-of-book quote, and produces it to ``odds-quotes`` keyed by the **pair** — so both
  venues' quotes for a pair co-partition. Both are stateless snapshot pollers (no cursor).
* ``radar`` — a Transformer that folds those quotes into a per-pair ``{venue: quote}`` state
  and, on every update, recomputes both cross-venue arbitrage directions: a continuous
  margin stream (``odds-margins``, "distance to free money") plus a sparse signal stream
  (``odds-signals``) when a *fresh*, fee-adjusted net-positive edge appears. Quotes carry an
  event time, and a margin computed against a stale other leg can't signal.

Why this example exists: it teaches two shapes the others don't — **N-source fan-in** (many
producers → one keyed, materialized best-price state) and **event-time staleness** (freshness
gating + close-driven state tombstoning). Read-only public data, no order placement. The
arb math is symmetric one-contract-per-leg binaries; fees routinely eat a visible gross edge,
which is the lesson. See ``README.md`` (and the responsible-use note there).
"""
from .arbitrage import Quote, compute_margins, fee
from .attributes import MARGINS_TOPIC, PAIRS_TOPIC, QUOTES_TOPIC, SIGNALS_TOPIC

__all__ = [
    "Quote",
    "compute_margins",
    "fee",
    "MARGINS_TOPIC",
    "PAIRS_TOPIC",
    "QUOTES_TOPIC",
    "SIGNALS_TOPIC",
]

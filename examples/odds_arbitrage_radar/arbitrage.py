"""The arbitrage math — pure, framework-free, the logic tier's playground.

A binary prediction-market contract pays **$1** if its side resolves true and $0 otherwise.
Buy one YES contract on venue A at ask ``a`` and one NO contract on venue B at ask ``b`` for
the *same* real-world event: whichever way it resolves, exactly one of the two contracts
pays $1. So the pair is a guaranteed $1 return for an outlay of ``a + b`` — a **risk-free
profit whenever ``a + b < 1``** (before fees). One contract per leg — no stake-splitting
(that is the decimal-odds bookmaker case; with fixed $1 binaries the hedge is one-for-one).

Two things make it honest:

* **Two directions, priced independently.** "YES on Polymarket + NO on Kalshi" and "YES on
  Kalshi + NO on Polymarket" use different order books and generally have different edges;
  each is computed on its own and only when both of its legs have an ask.
* **Fees, via the same parabolic formula both venues use** — ``fee = rate · p · (1 − p)``
  per contract, peaking at ``p = 0.5`` (1.75 ¢ at Kalshi's 0.07 rate) and vanishing toward
  the extremes. A visible *gross* edge is routinely smaller than the fees, so the **net**
  edge is what a signal is made of. (Kalshi rounds each order's fee up to the cent; we model
  the smooth per-contract rate and note the rounding in the README.)

Everything here is a pure function of prices, sizes, and rates — no clock, no I/O — so the
whole module is driven directly by ``tests/logic_test.py``.
"""
from dataclasses import dataclass

POLYMARKET_YES = "polymarket_yes+kalshi_no"
"""Direction 1: buy YES on Polymarket, NO on Kalshi."""
KALSHI_YES = "kalshi_yes+polymarket_no"
"""Direction 2: buy YES on Kalshi, NO on Polymarket."""


def fee(price: float, rate: float) -> float:
    """The per-contract taker fee at ``price`` under ``rate`` — ``rate · p · (1 − p)``.

    Symmetric about 0.5 (where it peaks) and zero at ``p ∈ {0, 1}``: a near-certain contract
    is nearly free to trade, a coin-flip is dearest. Both venues share this shape; only the
    ``rate`` differs (and rides on the quote, so it is passed in, never assumed)."""
    return rate * price * (1.0 - price)


@dataclass(frozen=True, slots=True)
class Quote:
    """One venue's top-of-book for a pair, reduced to what the arb math needs.

    Asks are the buy prices (arbitrage takes liquidity); sizes are the contracts available
    at those asks. Any of the four may be ``None`` (a one-sided or empty book) — a direction
    that needs a missing ask simply isn't computed."""
    venue: str
    title: str
    fee_rate: float
    yes_ask: float | None
    no_ask: float | None
    yes_ask_size: float | None
    no_ask_size: float | None


@dataclass(frozen=True, slots=True)
class Margin:
    """One direction's arbitrage economics, per $1 payout."""
    direction: str
    yes_ask: float
    no_ask: float
    gross_edge: float
    fees: float
    net_edge: float
    executable_size: float | None


def _margin(direction: str, yes_ask: float, yes_rate: float, yes_size: float | None,
            no_ask: float, no_rate: float, no_size: float | None) -> Margin:
    """Assemble one direction's ``Margin`` from its two legs' asks, rates, and sizes."""
    gross_edge = 1.0 - (yes_ask + no_ask)
    fees = fee(yes_ask, yes_rate) + fee(no_ask, no_rate)
    executable_size = min(yes_size, no_size) if yes_size is not None and no_size is not None else None
    return Margin(direction, yes_ask, no_ask, gross_edge, fees, gross_edge - fees, executable_size)


def compute_margins(polymarket: Quote, kalshi: Quote) -> list[Margin]:
    """Both computable arbitrage directions between the two venues' quotes — pure.

    A direction is emitted only when both of its legs have an ask (buying is impossible
    otherwise). With complete two-sided books both directions appear; a one-sided book on
    either venue yields one, or none. The caller (the radar) decides freshness and whether a
    positive net edge signals — this function is price arithmetic only."""
    margins: list[Margin] = []
    if polymarket.yes_ask is not None and kalshi.no_ask is not None:
        margins.append(_margin(
            POLYMARKET_YES,
            polymarket.yes_ask, polymarket.fee_rate, polymarket.yes_ask_size,
            kalshi.no_ask, kalshi.fee_rate, kalshi.no_ask_size,
        ))
    if kalshi.yes_ask is not None and polymarket.no_ask is not None:
        margins.append(_margin(
            KALSHI_YES,
            kalshi.yes_ask, kalshi.fee_rate, kalshi.yes_ask_size,
            polymarket.no_ask, polymarket.fee_rate, polymarket.no_ask_size,
        ))
    return margins

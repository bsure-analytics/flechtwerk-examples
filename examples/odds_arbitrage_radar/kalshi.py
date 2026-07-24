"""Kalshi quotes — an ``Extractor`` that snapshots one binary market's top-of-book.

Stage 1b. An ``odds-pairs`` config record names a Kalshi market by ``ticker``. Each poll is a
single keyless GET (``/markets/<ticker>`` → ``{"market": {…}}``) — Kalshi's market object
carries top-of-book directly, so no separate order-book call is needed.

**The complement book.** Kalshi runs one order book per market and exposes it from both
sides: ``no_ask = 1 − yes_bid`` and ``no_bid = 1 − yes_ask``. The two sides' sizes are
therefore **crosswise** — the size resting at ``no_ask`` is ``yes_bid_size_fp`` (the same
resting order, seen from the NO side), and the size at ``no_bid`` is ``yes_ask_size_fp``.
For arbitrage we buy at the ask, so we keep ``yes_ask`` with ``yes_ask_size_fp`` and
``no_ask`` with the crosswise ``yes_bid_size_fp``.

Prices come as dollar **strings** (``"0.7600"``); ``float()`` them. A side with no resting
order reports ``"0.0000"`` — treated as *absent* (a $0 ask is not a real offer), so that
side contributes no ask/size, exactly like Polymarket's empty book.

**No cursor** (stateless snapshot; yields only ``Message``s). **Let it crash:** an unknown
ticker 404s and the poll raises — the supervisor restarts, and ``request.py`` validates the
ticker at request time. The output is keyed by the **pair key** (the Polymarket slug in the
same config record), not the ticker, so both venues' quotes co-partition onto one radar task.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from .attributes import (
    FEE_RATE,
    FETCHED_AT,
    KALSHI_FEE_RATE,
    KALSHI_TICKER,
    KIND,
    NO_ASK,
    NO_ASK_SIZE,
    NO_BID,
    PAIR_KEY,
    PAIRS_TOPIC,
    POLYMARKET_SLUG,
    QUOTES_TOPIC,
    STATUS,
    TITLE,
    VENUE,
    YES_ASK,
    YES_ASK_SIZE,
    YES_BID,
)

log = logging.getLogger(__name__)

VENUE_NAME = "kalshi"

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
"""Kalshi's public trade API (no auth for market-data reads; ~30 req/s public limit). The
demo constant; injectable for tests via ``KalshiQuotes(base_url=…)``."""

DEFAULT_FEE_RATE = 0.07
"""Kalshi's general-markets taker-fee rate (``fee = round_up(0.07 · C · p · (1−p))``). Some
series differ; override per pair with ``kalshi_fee_rate``. We model the smooth per-contract
rate and note Kalshi's per-order cent-rounding in the README. Makers can earn rebates on
some series; the radar models the taker cost only."""


def _price(raw: str | None) -> float | None:
    """A Kalshi dollar-string → float, or ``None`` for the ``"0.0000"`` no-liquidity sentinel.

    A resting price is in ``(0, 1]``; Kalshi reports ``0.0000`` when a side has no order, so
    a non-positive parse means "absent", not "free" — never fabricate a $0 ask."""
    if raw is None:
        return None
    price = float(raw)
    return price if price > 0.0 else None


def _size(raw: str | None) -> float | None:
    """A Kalshi size-string → float (``None`` if absent)."""
    return None if raw is None else float(raw)


def normalize_kalshi(market: dict, *, fee_rate: float, pair_key: str, fetched_at: datetime) -> Event:
    """Project a Kalshi market object into one normalized quote ``Event`` — pure.

    Keeps YES/NO best bid and ask, the YES ask size, and the **crosswise** NO ask size
    (``yes_bid_size_fp``). Omits any side whose price is the ``0.0000`` sentinel — and its
    size with it — so an empty side is absent, never a fabricated 0."""
    quote = Event({
        KIND: "quote", PAIR_KEY: pair_key, VENUE: VENUE_NAME,
        TITLE: market["title"],
        STATUS: "active" if market.get("status") == "active" else "closed",
        FEE_RATE: fee_rate, FETCHED_AT: fetched_at,
    })
    yes_bid, yes_ask = _price(market.get("yes_bid_dollars")), _price(market.get("yes_ask_dollars"))
    no_bid, no_ask = _price(market.get("no_bid_dollars")), _price(market.get("no_ask_dollars"))
    if yes_bid is not None:
        quote[YES_BID] = yes_bid
    if yes_ask is not None:
        quote[YES_ASK] = yes_ask
        if (size := _size(market.get("yes_ask_size_fp"))) is not None:
            quote[YES_ASK_SIZE] = size
    if no_bid is not None:
        quote[NO_BID] = no_bid
    if no_ask is not None:
        quote[NO_ASK] = no_ask
        # Crosswise: the order resting at no_ask is the yes-side bid, so its size is yes_bid_size_fp.
        if (size := _size(market.get("yes_bid_size_fp"))) is not None:
            quote[NO_ASK_SIZE] = size
    return quote


class KalshiQuotes(Extractor):
    """Polls each configured pair's Kalshi market and emits one normalized quote.

    Subclasses ``Extractor`` to own the ``httpx`` client; tests inject a ``MockTransport``."""

    config_topics = [PAIRS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 base_url: str = BASE_URL, quotes_topic: str = QUOTES_TOPIC) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._topic = quotes_topic

    async def __aenter__(self) -> "KalshiQuotes":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(30.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit one Kalshi quote for the pair — a stateless snapshot (no ``State``)."""
        assert self._client is not None, "client is opened in __aenter__ or injected"
        pair_key = config[POLYMARKET_SLUG]
        ticker = config[KALSHI_TICKER]
        # Explicit None check, not `or`: a 0.0 override is falsy and must not fall through.
        fee_rate = config.get(KALSHI_FEE_RATE)
        if fee_rate is None:
            fee_rate = DEFAULT_FEE_RATE

        response = await self._client.get(f"{self._base_url}/markets/{ticker}")
        response.raise_for_status()
        market = response.json()["market"]
        fetched_at = self._fetched_at(response)

        quote = normalize_kalshi(market, fee_rate=fee_rate, pair_key=pair_key, fetched_at=fetched_at)
        log.info("kalshi %s (pair %s): status=%s yes_ask=%s no_ask=%s",
                 ticker, pair_key, quote[STATUS], quote.get(YES_ASK), quote.get(NO_ASK))
        yield Message(key=pair_key, topic=self._topic, value=quote)

    @staticmethod
    def _fetched_at(response: httpx.Response) -> datetime:
        """The server ``Date`` as aware UTC — the event-time clock (tests pin it via the
        ``Date`` response header; falls back to now if absent)."""
        raw = response.headers.get("Date")
        if raw:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        return datetime.now(timezone.utc)  # pragma: no cover — live feed always sends Date


stage = KalshiQuotes()
"""The stage the dispatcher runs (``python -m examples.odds_arbitrage_radar kalshi``)."""

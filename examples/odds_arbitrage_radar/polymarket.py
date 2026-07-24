"""Polymarket quotes — an ``Extractor`` that snapshots one binary market's order books.

Stage 1a. An ``odds-pairs`` config record names a Polymarket market by ``slug`` and which of
its two outcomes is the YES side (the one that resolves the same event as Kalshi's YES).
Each poll is a stateless snapshot — three keyless GETs:

1. **Gamma** (``/markets?slug=…``) → the market's metadata. Its ``outcomes`` and
   ``clobTokenIds`` are JSON arrays delivered *as strings* (double-encoded), one CLOB token
   id per outcome in the same order. We ``json.loads`` both and pick the YES and NO tokens.
2. **CLOB** (``/book?token_id=…``), once per token → each outcome's live order book. Levels
   arrive **unsorted**, so best bid = max bid price and best ask = min ask price (see
   ``best_of_book``). Either side may be empty (a one-sided book); the missing ask simply
   makes that side absent on the quote.

Gamma also exposes its own ``bestBid``/``bestAsk``/``outcomePrices`` and ``takerBaseFee``
fields, but their exact semantics are under-documented (midpoint-ish; fee units unclear), so
we deliberately **do not** trust them for the arb math — the CLOB books are the source of
truth, and the fee rate comes from config or the module default.

**No cursor.** A snapshot poll keeps no state — every poll re-derives the current book from
scratch — so this extractor yields only ``Message``s, never ``State`` (the stateless-source
rule; the trailing page commits without a cursor). **Let it crash:** an unknown slug (Gamma
returns ``[]``), a ``negRisk`` multi-outcome market, or a ``yes_outcome`` that names no
outcome raises — the supervisor restarts, and ``request.py`` validates at request time so it
is rare.
"""
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from .attributes import (
    FEE_RATE,
    FETCHED_AT,
    KIND,
    NO_ASK,
    NO_BID,
    PAIR_KEY,
    PAIRS_TOPIC,
    POLYMARKET_FEE_RATE,
    POLYMARKET_SLUG,
    QUOTES_TOPIC,
    STATUS,
    TITLE,
    VENUE,
    YES_ASK,
    YES_BID,
    YES_ASK_SIZE,
    NO_ASK_SIZE,
    YES_OUTCOME,
)

log = logging.getLogger(__name__)

VENUE_NAME = "polymarket"

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
"""Polymarket's Gamma market-metadata API (no auth, no key). The demo constant; injectable
for tests via ``PolymarketQuotes(gamma_base_url=…)``."""
CLOB_BASE_URL = "https://clob.polymarket.com"
"""Polymarket's CLOB order-book API (no auth, no key for reads). Injectable like Gamma."""

DEFAULT_FEE_RATE = 0.05
"""Polymarket's taker-fee rate for **sports** markets (the demo's staple). Polymarket's rate
is category-dependent — 0.00 (geopolitics), 0.04 (finance/politics), 0.05 (sports/culture/
weather/economics), 0.07 (crypto) — so override it per pair with ``polymarket_fee_rate``
when a pair is not a sports market. Makers pay nothing; the radar models the taker cost."""


def best_of_book(levels: list[dict], *, side: str) -> tuple[float, float] | None:
    """Best (price, size) on one side of a CLOB book — ``max`` price for bids, ``min`` for
    asks — or ``None`` if the side is empty. Pure.

    CLOB levels are ``{"price": str, "size": str}`` in no guaranteed order, so we reduce over
    the whole list rather than trusting position. "Best" is the price a taker meets first:
    the highest someone will buy at (bid) or the lowest someone will sell at (ask)."""
    if not levels:
        return None
    priced = [(float(l["price"]), float(l["size"])) for l in levels]
    return max(priced) if side == "bid" else min(priced)


def yes_no_tokens(market: dict, yes_outcome: str) -> tuple[str, str]:
    """Resolve ``(yes_token_id, no_token_id)`` for a binary market — pure.

    Raises ``ValueError`` on the shapes the radar can't handle: a ``negRisk`` multi-outcome
    market, a market that isn't exactly two-outcome, or a ``yes_outcome`` that names none of
    the outcomes. These are loud on purpose — a bad pair should fail at the request or the
    poll, not silently mis-price."""
    if market.get("negRisk"):
        raise ValueError(f"{market.get('slug')!r} is a negRisk (multi-outcome) market — out of scope")
    outcomes = json.loads(market["outcomes"])
    tokens = json.loads(market["clobTokenIds"])
    if len(outcomes) != 2 or len(tokens) != 2:
        raise ValueError(f"{market.get('slug')!r} is not a binary market: outcomes={outcomes}")
    try:
        yes_idx = outcomes.index(yes_outcome)
    except ValueError:
        raise ValueError(f"yes_outcome {yes_outcome!r} is not one of {outcomes}") from None
    return tokens[yes_idx], tokens[1 - yes_idx]


def polymarket_status(market: dict) -> str:
    """Normalize Gamma's several status booleans to ``active`` | ``closed`` — pure.

    Tradeable only when the market is active, still accepting orders, and neither closed nor
    archived; anything else is ``closed`` (which tombstones the radar's join state)."""
    tradeable = (market.get("active") and market.get("acceptingOrders")
                 and not market.get("closed") and not market.get("archived"))
    return "active" if tradeable else "closed"


def normalize_polymarket(market: dict, books: dict[str, dict], *, yes_outcome: str,
                         fee_rate: float, pair_key: str, fetched_at: datetime) -> Event:
    """Project Gamma metadata + the two CLOB books into one normalized quote ``Event`` — pure.

    ``books`` maps token id → its ``/book`` response. Picks the YES/NO tokens by outcome,
    reads best bid/ask (and ask size) off each book, and stamps the pair identity, venue,
    fee rate, and event time. A side with no ask contributes no ask/size attribute — never a
    fabricated 0 (the SMARD/GDELT rule)."""
    yes_token, no_token = yes_no_tokens(market, yes_outcome)
    yes_book, no_book = books[yes_token], books[no_token]
    quote = Event({
        KIND: "quote", PAIR_KEY: pair_key, VENUE: VENUE_NAME,
        TITLE: market["question"], STATUS: polymarket_status(market),
        FEE_RATE: fee_rate, FETCHED_AT: fetched_at,
    })
    _put_side(quote, yes_book, bid_attr=YES_BID, ask_attr=YES_ASK, ask_size_attr=YES_ASK_SIZE)
    _put_side(quote, no_book, bid_attr=NO_BID, ask_attr=NO_ASK, ask_size_attr=NO_ASK_SIZE)
    return quote


def _put_side(quote: Event, book: dict, *, bid_attr, ask_attr, ask_size_attr) -> None:
    """Fold one book's best bid/ask (+ ask size) into the quote, omitting an empty side."""
    if (bid := best_of_book(book.get("bids", []), side="bid")) is not None:
        quote[bid_attr] = bid[0]
    if (ask := best_of_book(book.get("asks", []), side="ask")) is not None:
        quote[ask_attr] = ask[0]
        quote[ask_size_attr] = ask[1]


class PolymarketQuotes(Extractor):
    """Polls each configured pair's Polymarket market and emits one normalized quote.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed in
    ``__aexit__``); tests inject a ``MockTransport`` client serving the fixtures."""

    config_topics = [PAIRS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 gamma_base_url: str = GAMMA_BASE_URL, clob_base_url: str = CLOB_BASE_URL,
                 quotes_topic: str = QUOTES_TOPIC) -> None:
        super().__init__()
        self._client = client
        self._gamma = gamma_base_url.rstrip("/")
        self._clob = clob_base_url.rstrip("/")
        self._topic = quotes_topic

    async def __aenter__(self) -> "PolymarketQuotes":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(30.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit one Polymarket quote for the pair — a stateless snapshot (no ``State``)."""
        assert self._client is not None, "client is opened in __aenter__ or injected"
        slug = config[POLYMARKET_SLUG]
        yes_outcome = config[YES_OUTCOME]
        # Explicit None check, not `or`: a legitimate 0.0 override (geopolitics markets are
        # fee-free) is falsy and must not fall through to the default.
        fee_rate = config.get(POLYMARKET_FEE_RATE)
        if fee_rate is None:
            fee_rate = DEFAULT_FEE_RATE

        response = await self._client.get(f"{self._gamma}/markets", params={"slug": slug})
        response.raise_for_status()
        markets = response.json()
        if not markets:
            raise RuntimeError(f"no Polymarket market for slug {slug!r}")
        market = markets[0]
        fetched_at = self._fetched_at(response)

        yes_token, no_token = yes_no_tokens(market, yes_outcome)
        books = {token: await self._book(token) for token in (yes_token, no_token)}
        quote = normalize_polymarket(market, books, yes_outcome=yes_outcome,
                                     fee_rate=fee_rate, pair_key=slug, fetched_at=fetched_at)
        log.info("polymarket %s: status=%s yes_ask=%s no_ask=%s",
                 slug, quote[STATUS], quote.get(YES_ASK), quote.get(NO_ASK))
        yield Message(key=slug, topic=self._topic, value=quote)

    async def _book(self, token_id: str) -> dict:
        response = await self._client.get(f"{self._clob}/book", params={"token_id": token_id})
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _fetched_at(response: httpx.Response) -> datetime:
        """The server ``Date`` as aware UTC — the event-time clock (tests pin it via the
        ``Date`` response header; falls back to now if absent)."""
        raw = response.headers.get("Date")
        if raw:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        return datetime.now(timezone.utc)  # pragma: no cover — live feed always sends Date


stage = PolymarketQuotes()
"""The stage the dispatcher runs (``python -m examples.odds_arbitrage_radar polymarket``)."""

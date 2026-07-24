"""Request (or retire) a pair to watch — validated, then written to ``odds-pairs``.

    uv run poe request-pair <polymarket-slug> <kalshi-ticker> "<yes outcome>"
    uv run poe request-pair retire <polymarket-slug>

The ops step that replaces a hard-coded seed (the ADS-B ``request-region`` pattern): a pair
is a human claim that one Polymarket market and one Kalshi market resolve the *same* event,
with ``yes_outcome`` naming the Polymarket outcome that lines up with Kalshi's YES side. That
claim can't be inferred safely (the two venues may word or settle "the same" event
differently — the resolution-mismatch caveat in the README), so it is curated here.

**Validated before writing.** It fetches both venues live and refuses to write a pair that
can't work: an unknown slug, a ``negRisk`` or non-binary Polymarket market, or a
``yes_outcome`` that names no outcome. It prints both top-of-books and the current margins,
and **warns loudly if a resting net edge exceeds 3 %** — a real one that large is vanishingly
rare, so it almost always means ``yes_outcome`` is mapped to the wrong side (which inverts
every margin). The record is keyed by the Polymarket slug on the compacted config topic, so
re-requesting a slug updates it and ``retire`` writes a tombstone that removes it. Any
producer works too (Kafbat UI included) — this is just the convenient, checked one.

Read-only: this validates and writes a config record. It never places an order.
"""
import asyncio
import json
import sys

import httpx
from aiokafka import AIOKafkaProducer

from flechtwerk import Event

from examples._setup import quiet_fresh_topic_produce_race

from .arbitrage import Quote, compute_margins
from .attributes import (
    FEE_RATE,
    KALSHI_TICKER,
    NO_ASK,
    NO_ASK_SIZE,
    PAIRS_TOPIC,
    POLYMARKET_SLUG,
    TITLE,
    YES_ASK,
    YES_ASK_SIZE,
    YES_OUTCOME,
)
from .kalshi import BASE_URL as KALSHI_BASE_URL, DEFAULT_FEE_RATE as KALSHI_FEE, KalshiQuotes, normalize_kalshi
from .polymarket import (
    CLOB_BASE_URL,
    DEFAULT_FEE_RATE as POLY_FEE,
    GAMMA_BASE_URL,
    PolymarketQuotes,
    normalize_polymarket,
    yes_no_tokens,
)

BOOTSTRAP_SERVERS = "localhost:9092"

SANITY_EDGE = 0.03
"""A resting net edge above this is almost certainly a flipped ``yes_outcome`` mapping, not a
real 3 %+ risk-free arb sitting unclaimed on two public venues. Warn, don't block — a genuine
in-game spike can briefly clear it, and the operator may know better."""


def _quote_from_event(venue: str, ev: Event) -> Quote:
    """An ``arbitrage.Quote`` from a normalized quote ``Event`` (for the validation preview)."""
    return Quote(venue=venue, title=ev[TITLE], fee_rate=ev[FEE_RATE],
                 yes_ask=ev.get(YES_ASK), no_ask=ev.get(NO_ASK),
                 yes_ask_size=ev.get(YES_ASK_SIZE), no_ask_size=ev.get(NO_ASK_SIZE))


async def _preview(slug: str, ticker: str, yes_outcome: str) -> None:
    """Fetch both venues live, validate the pair, and print the current books + margins.

    Raises (with a clear message) if the pair can't work; prints a loud warning on an
    implausibly large resting edge."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
        gamma = await client.get(f"{GAMMA_BASE_URL}/markets", params={"slug": slug})
        gamma.raise_for_status()
        markets = gamma.json()
        if not markets:
            raise SystemExit(f"No Polymarket market for slug {slug!r} — check the slug on polymarket.com.")
        market = markets[0]
        yes_token, no_token = yes_no_tokens(market, yes_outcome)  # raises on negRisk / non-binary / bad outcome
        books = {}
        for token in (yes_token, no_token):
            book = await client.get(f"{CLOB_BASE_URL}/book", params={"token_id": token})
            book.raise_for_status()
            books[token] = book.json()
        poly_ev = normalize_polymarket(market, books, yes_outcome=yes_outcome, fee_rate=POLY_FEE,
                                       pair_key=slug, fetched_at=PolymarketQuotes._fetched_at(gamma))

        km = await client.get(f"{KALSHI_BASE_URL}/markets/{ticker}")
        km.raise_for_status()
        kalshi_ev = normalize_kalshi(km.json()["market"], fee_rate=KALSHI_FEE, pair_key=slug,
                                     fetched_at=KalshiQuotes._fetched_at(km))

    print(f"Polymarket : {poly_ev[TITLE]}")
    print(f"             yes({yes_outcome}) ask={poly_ev.get(YES_ASK)}  no ask={poly_ev.get(NO_ASK)}")
    print(f"Kalshi     : {kalshi_ev[TITLE]}")
    print(f"             yes ask={kalshi_ev.get(YES_ASK)}  no ask={kalshi_ev.get(NO_ASK)}")
    margins = compute_margins(_quote_from_event("polymarket", poly_ev), _quote_from_event("kalshi", kalshi_ev))
    if not margins:
        print("No computable direction right now (a book side is empty) — the pair is still valid.")
    for m in margins:
        print(f"  {m.direction:26s} gross={m.gross_edge:+.4f} fees={m.fees:.4f} net={m.net_edge:+.4f}"
              + (f" size={m.executable_size:g}" if m.executable_size is not None else ""))
    if any(m.net_edge > SANITY_EDGE for m in margins):
        print(f"\n  ⚠️  A resting net edge exceeds {SANITY_EDGE:.0%}. That is almost never a real arb —")
        print(f"      double-check that yes_outcome ({yes_outcome!r}) is the outcome that matches")
        print("      Kalshi's YES side. A flipped mapping inverts every margin.")


async def request_pair(slug: str, ticker: str, yes_outcome: str) -> None:
    """Validate, preview, then write the config record keyed by the Polymarket slug."""
    await _preview(slug, ticker, yes_outcome)
    record = Event({POLYMARKET_SLUG: slug, KALSHI_TICKER: ticker, YES_OUTCOME: yes_outcome})
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            await producer.send_and_wait(PAIRS_TOPIC, key=slug.encode(),
                                         value=json.dumps(record.raw).encode())
        print(f"\nRequested pair {slug!r} ↔ {ticker!r} (yes = {yes_outcome!r})")
    finally:
        await producer.stop()


async def retire(slug: str) -> None:
    """Write a tombstone (null value) for a pair, keyed by slug — removes it from the config.

    Compacted-topic tombstone: the extractors' config bootstrap treats an empty value as a
    deletion, so the pair drops out of every instance's active set on the next config drain."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(PAIRS_TOPIC, key=slug.encode(), value=None)
        print(f"Retired pair {slug!r} (tombstone written)")
    finally:
        await producer.stop()


def main() -> None:
    argv = sys.argv[1:]
    if len(argv) == 2 and argv[0] == "retire":
        asyncio.run(retire(argv[1]))
        return
    if len(argv) == 3:
        asyncio.run(request_pair(argv[0], argv[1], argv[2]))
        return
    sys.exit('usage: python -m examples.odds_arbitrage_radar.request '
             '<polymarket-slug> <kalshi-ticker> "<yes outcome>"\n'
             '   or: python -m examples.odds_arbitrage_radar.request retire <polymarket-slug>')


if __name__ == "__main__":
    main()

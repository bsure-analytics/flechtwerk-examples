"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker, no
network — with the venues served from the committed fixtures via an ``httpx.MockTransport``
whose ``Date`` header (the event-time clock) the test controls:

- **polymarket / kalshi** drive the real ``ExtractorRunner``/``poll_one``: one poll produces
  exactly one quote to ``odds-quotes`` keyed by the pair, matching the normalization; a bad
  target (empty Gamma result / a Kalshi 404) makes the poll raise (let-it-crash, surfaced by
  the runner).
- **radar** drives ``TransformerRunner.process_batch`` over co-partitioned quotes: a batch of
  a Polymarket then a Kalshi quote for one pair accumulates the join state and emits margins;
  a contrived fresh arb also emits a signal; a later ``closed`` quote tombstones the state.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest

from flechtwerk import Event
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.odds_arbitrage_radar.attributes import (
    FEE_RATE,
    FETCHED_AT,
    KIND,
    LEGS,
    MARGINS_TOPIC,
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
from examples.odds_arbitrage_radar.kalshi import KalshiQuotes
from examples.odds_arbitrage_radar.polymarket import PolymarketQuotes
from examples.odds_arbitrage_radar.radar import radar

FIXTURES = Path(__file__).parent / "fixtures"
POLY_MARKET = json.loads((FIXTURES / "poly_market.json").read_text())
POLY_BOOK_COLORADO = json.loads((FIXTURES / "poly_book_colorado.json").read_text())
POLY_BOOK_MILWAUKEE = json.loads((FIXTURES / "poly_book_milwaukee.json").read_text())
KALSHI_MARKET = json.loads((FIXTURES / "kalshi_market.json").read_text())

UTC = timezone.utc
PAIR = "mlb-col-mil-2026-07-24"
TICKER = "KXMLBGAME-26JUL261410COLMIL-MIL"
_T = datetime(2026, 7, 23, 20, 16, tzinfo=UTC)

GAMMA = "http://poly.test/gamma"
CLOB = "http://poly.test/clob"
KALSHI_BASE = "http://kalshi.test/v2"

PAIR_CONFIG = {"polymarket_slug": PAIR, "kalshi_ticker": TICKER, "yes_outcome": "Milwaukee Brewers"}


# --- extractor harness (mirrors the SMARD / GTFS runner tests) ---

def _extractor_runner(stage: Extractor, key: str, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "odds-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=key, value=json.dumps(config), topic=stage.config_topics[0])])
    runner.create_restore_consumer = lambda: FakeKafkaConsumer()
    runner.create_token_producer = lambda token: producer
    runner.extractor = stage
    runner.inner_store = inner
    runner.observer = Observer()
    runner.poll_interval = timedelta(0)
    runner.num_tokens = 1
    runner.tokens = frozenset({0})
    store = ChangelogStateStore()
    store.inner = inner
    store.producer = FakeKafkaProducer()
    store.topic = runner.changelog_topic
    runner.tasks[0] = TokenTask(asyncio.Lock(), producer, store)
    return runner, producer


def _handler(clock: dict, *, gamma_markets):
    """A MockTransport handler serving Gamma, CLOB, and Kalshi from the fixtures, stamping
    each response with the ``Date`` the test currently wants (the event-time clock)."""
    tokens = json.loads(POLY_MARKET[0]["clobTokenIds"])
    books = {tokens[0]: POLY_BOOK_COLORADO, tokens[1]: POLY_BOOK_MILWAUKEE}

    def handle(request: httpx.Request) -> httpx.Response:
        headers = {"Date": format_datetime(clock["date"], usegmt=True)}
        path = request.url.path
        if path.endswith("/gamma/markets"):
            return httpx.Response(200, json=gamma_markets, headers=headers)
        if path.endswith("/clob/book"):
            token = request.url.params["token_id"]
            return httpx.Response(200, json=books[token], headers=headers)
        if path.endswith(f"/markets/{TICKER}"):
            return httpx.Response(200, json=KALSHI_MARKET, headers=headers)
        return httpx.Response(404, headers=headers)
    return handle


def _client(clock: dict, *, gamma_markets=POLY_MARKET) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(_handler(clock, gamma_markets=gamma_markets)))


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(p["value"]) for t, p in producer.sent if t == topic]


async def _poll(stage: Extractor, key: str, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    runner, producer = _extractor_runner(stage, key, config)
    await runner.load_initial_configs()
    return runner, producer


# --- polymarket extractor ---

async def test_polymarket_poll_emits_one_quote_keyed_by_pair() -> None:
    clock = {"date": _T}
    stage = PolymarketQuotes(client=_client(clock), gamma_base_url=GAMMA, clob_base_url=CLOB)
    runner, producer = await _poll(stage, PAIR, PAIR_CONFIG)
    async with stage:
        await runner.poll_one(runner.entries[PAIR])
    quotes = _sent(producer, QUOTES_TOPIC)
    assert len(quotes) == 1
    q = quotes[0]                                               # a plain JSON dict → wire-name keys
    assert q["kind"] == "quote" and q["venue"] == "polymarket" and q["pair_key"] == PAIR
    assert q["yes_ask"] == 0.69 and q["no_ask"] == 0.32        # YES = Milwaukee book
    keys = {p["key"].decode() for t, p in producer.sent if t == QUOTES_TOPIC}
    assert keys == {PAIR}                                       # keyed by the pair, not the token


async def test_polymarket_poll_raises_on_unknown_slug() -> None:
    clock = {"date": _T}
    stage = PolymarketQuotes(client=_client(clock, gamma_markets=[]), gamma_base_url=GAMMA, clob_base_url=CLOB)
    runner, _ = await _poll(stage, PAIR, PAIR_CONFIG)
    async with stage:
        with pytest.raises(RuntimeError, match="no Polymarket market"):
            await runner.poll_one(runner.entries[PAIR])


# --- kalshi extractor ---

async def test_kalshi_poll_emits_one_quote_keyed_by_pair() -> None:
    clock = {"date": _T}
    stage = KalshiQuotes(client=_client(clock), base_url=KALSHI_BASE)
    runner, producer = await _poll(stage, PAIR, PAIR_CONFIG)
    async with stage:
        await runner.poll_one(runner.entries[PAIR])
    quotes = _sent(producer, QUOTES_TOPIC)
    assert len(quotes) == 1
    q = quotes[0]                                               # a plain JSON dict → wire-name keys
    assert q["venue"] == "kalshi" and q["pair_key"] == PAIR     # pair key = slug, not ticker
    assert q["yes_ask"] == 0.76 and q["no_ask"] == 0.30 and q["no_ask_size"] == 997.0


async def test_kalshi_poll_raises_on_unknown_ticker() -> None:
    clock = {"date": _T}
    bad = {**PAIR_CONFIG, "kalshi_ticker": "KXNOPE-404"}
    stage = KalshiQuotes(client=_client(clock), base_url=KALSHI_BASE)
    runner, _ = await _poll(stage, PAIR, bad)
    async with stage:
        with pytest.raises(httpx.HTTPStatusError):
            await runner.poll_one(runner.entries[PAIR])


# --- radar: TransformerRunner.process_batch over co-partitioned quotes ---

def _make_module(records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "odds-radar"
    mod.client_id = "odds-radar"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = radar
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


async def _process(mod: _FlechtwerkModule) -> None:
    await mod.runner.process_batch(await mod.runner.consumer.getmany(timeout_ms=1000))


def _quote(venue: str, *, yes_ask=None, no_ask=None, yes_size=None, no_size=None,
           fee_rate=0.05, status="active", fetched_at=_T) -> str:
    ev = Event({KIND: "quote", PAIR_KEY: PAIR, VENUE: venue, TITLE: "Col @ Mil",
                STATUS: status, FEE_RATE: fee_rate, FETCHED_AT: fetched_at})
    for attr, v in ((YES_ASK, yes_ask), (NO_ASK, no_ask), (YES_ASK_SIZE, yes_size), (NO_ASK_SIZE, no_size)):
        if v is not None:
            ev[attr] = v
    return json.dumps(ev.raw)


async def test_radar_batch_emits_margin_and_signal_then_tombstones() -> None:
    # Batch 1: a Polymarket then a Kalshi quote for the pair — a contrived fresh arb.
    mod = _make_module([
        make_record(key=PAIR, value=_quote("polymarket", yes_ask=0.40, yes_size=100.0),
                    topic=QUOTES_TOPIC, offset=0),
        make_record(key=PAIR, value=_quote("kalshi", no_ask=0.40, no_size=250.0, fee_rate=0.07),
                    topic=QUOTES_TOPIC, offset=1),
    ])
    await _process(mod)
    producer = mod.runner.tasks[0].producer
    margins = _sent(producer, MARGINS_TOPIC)
    signals = _sent(producer, SIGNALS_TOPIC)
    assert len(margins) == 1 and margins[0]["net_edge"] > 0
    assert len(signals) == 1 and signals[0]["net_edge"] > 0     # fresh + net-positive
    stored = await mod.runner.tasks[0].store.get(PAIR)          # a State (Record) → Attribute keys
    assert stored is not None and set(stored[LEGS]) == {"polymarket", "kalshi"}

    # Batch 2: a closed quote tombstones the pair's join state.
    mod.consumer.records = [make_record(key=PAIR, value=_quote("kalshi", status="closed"),
                                        topic=QUOTES_TOPIC, offset=2)]
    await _process(mod)
    assert await mod.runner.tasks[0].store.get(PAIR) is None    # bucket tombstoned


async def test_radar_single_leg_emits_no_margin() -> None:
    mod = _make_module([
        make_record(key=PAIR, value=_quote("polymarket", yes_ask=0.40), topic=QUOTES_TOPIC, offset=0),
    ])
    await _process(mod)
    producer = mod.runner.tasks[0].producer
    assert _sent(producer, MARGINS_TOPIC) == [] and _sent(producer, SIGNALS_TOPIC) == []
    assert await mod.runner.tasks[0].store.get(PAIR) is not None   # state built, awaiting the other venue

"""Tier 3 — integration. The fan-in over a real broker, and the ClickHouse schema applied
against a real server.

- ``test_radar_join_over_the_broker`` produces a Polymarket-shaped and a Kalshi-shaped quote
  for one pair — same key, contrived to a fresh net-positive arb — and runs the real
  ``radar`` transformer. It proves the fan-in end to end: both venues' quotes land on the
  same partition/task (real key-hash co-partitioning), accumulate into the per-pair join
  state, and emit a margin plus a signal, both read back under ``read_committed``.
- ``test_clickhouse_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and
  asserts the three queues, three target tables, and three materialized views are created —
  catching any DDL error the Docker-free tiers can't.
"""
import asyncio
import json
from contextlib import suppress
from datetime import datetime, timezone

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Event
from flechtwerk.module import Flechtwerk

from examples.odds_arbitrage_radar.attributes import (
    FEE_RATE,
    FETCHED_AT,
    KIND,
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
from examples.odds_arbitrage_radar.radar import radar
from examples.odds_arbitrage_radar.setup import apply_clickhouse_schema

pytestmark = pytest.mark.integration

UTC = timezone.utc
PAIR = "mlb-col-mil-2026-07-24"
_T = datetime(2026, 7, 23, 20, 16, tzinfo=UTC)


def _quote(venue: str, *, yes_ask=None, no_ask=None, yes_size=None, no_size=None,
           fee_rate=0.05) -> bytes:
    ev = Event({KIND: "quote", PAIR_KEY: PAIR, VENUE: venue, TITLE: "Col @ Mil",
                STATUS: "active", FEE_RATE: fee_rate, FETCHED_AT: _T})
    for attr, v in ((YES_ASK, yes_ask), (NO_ASK, no_ask), (YES_ASK_SIZE, yes_size), (NO_ASK_SIZE, no_size)):
        if v is not None:
            ev[attr] = v
    return json.dumps(ev.raw).encode()


async def test_radar_join_over_the_broker(kafka_bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics([NewTopic(t, num_partitions=8, replication_factor=1)
                                       for t in (QUOTES_TOPIC, MARGINS_TOPIC, SIGNALS_TOPIC)])
    finally:
        await admin.close()

    app = Flechtwerk.of(application_id="odds-radar-it", bootstrap_servers=kafka_bootstrap,
                        client_id="odds-radar-it-0", stage=radar)
    consumer = AIOKafkaConsumer(MARGINS_TOPIC, SIGNALS_TOPIC, bootstrap_servers=kafka_bootstrap,
                                auto_offset_reset="earliest", group_id=None, isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        # Two same-pair quotes (contrived to a fresh net-positive arb): Polymarket YES @ 0.40,
        # Kalshi NO @ 0.40 → gross 0.20, well clear of fees.
        key = PAIR.encode()
        await producer.send_and_wait(QUOTES_TOPIC, key=key, value=_quote("polymarket", yes_ask=0.40, yes_size=100.0))
        await asyncio.sleep(1.0)
        await producer.send_and_wait(QUOTES_TOPIC, key=key, value=_quote("kalshi", no_ask=0.40, no_size=250.0, fee_rate=0.07))

        margins: list[dict] = []
        signals: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 60.0
        while not signals:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never emitted a signal: margins={margins}")
            for tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for r in records:
                    (signals if tp.topic == SIGNALS_TOPIC else margins).append(json.loads(r.value))

        assert margins and margins[0]["net_edge"] > 0
        assert margins[0]["direction"] == "polymarket_yes+kalshi_no"
        assert signals[0]["fresh"] is True and signals[0]["net_edge"] == pytest.approx(0.20 - (0.05 * 0.4 * 0.6 + 0.07 * 0.4 * 0.6))
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await producer.stop()
        await consumer.stop()


async def test_clickhouse_schema_applies(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    async with httpx.AsyncClient(base_url=clickhouse["base_url"], timeout=30.0, params={
        "user": clickhouse["user"], "password": clickhouse["password"], "database": clickhouse["database"],
    }) as client:
        response = await client.post("/", content="SELECT name FROM system.tables WHERE database = 'flechtwerk'")
        response.raise_for_status()
        tables = set(response.text.split())
    assert {"odds_quotes_queue", "odds_quotes", "odds_quotes_mv",
            "odds_margins_queue", "odds_margins", "odds_margins_mv",
            "odds_signals_queue", "odds_signals", "odds_signals_mv"} <= tables

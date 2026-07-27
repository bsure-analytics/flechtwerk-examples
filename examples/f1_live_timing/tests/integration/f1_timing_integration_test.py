"""Tier 3 — integration. The whole pipeline over a real broker, and the schema on a real server.

- ``test_the_tape_flows_through_both_stages`` runs **both** stages against ephemeral Kafka with
  the archive served from the committed fixture session by an ``httpx.MockTransport``: ingest
  reads the tape from byte 0 and produces ``f1-timing``, the board consumes it and produces
  ``f1-status`` / ``f1-events`` / ``f1-telemetry``, and the test reads the far end under
  ``read_committed``. It proves what the Docker-free tiers cannot: that a session's records
  co-partition by path onto one board task, that same-key serial processing preserves the tape
  order the watermark merge established, and that anchor → backfill → complete survives real
  transactions.
- ``test_the_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and asserts every
  object exists — catching any DDL error the Docker-free tiers can't see.
- ``test_the_materialized_views_project_real_board_records`` is the interesting ClickHouse test.
  The Kafka-engine queues cannot be repointed at an ephemeral broker (ClickHouse answers ``ALTER
  TABLE … MODIFY SETTING kafka_broker_list`` with 501, and the engine's settings are fixed at
  creation), so instead each view's **own SELECT** — read back out of ``system.tables``, with the
  queue swapped for a ``Memory`` table — is run over the board's real output records. That
  exercises precisely what can break in this schema: the ``kind`` filters, the ``::`` casts, the
  ``Nullable`` promotions, and the timestamp parsers. The Kafka-engine wiring itself is proven by
  the live end-to-end run the README documents, and its DDL by the test above.
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Event, IncomingMessage, Message, State
from flechtwerk.module import Flechtwerk

from examples.f1_live_timing import tape
from examples.f1_live_timing.attributes import (
    EVENT_TIME,
    EVENTS_TOPIC,
    FEED,
    OFFSET_MS,
    PAYLOAD,
    SESSION,
    SESSIONS_TOPIC,
    STATUS_TOPIC,
    TELEMETRY_TOPIC,
    TIMING_TOPIC,
)
from examples.f1_live_timing.ingest import SESSION_KIND, TapeIngest
from examples.f1_live_timing.setup import FOREVER, PARTITIONS, apply_clickhouse_schema
from examples.f1_live_timing.tests.runner_test import FIXTURES, INGESTED, SESSION_PATH, T0, Archive
from examples.f1_live_timing.timing import run_board, timing

pytestmark = pytest.mark.integration

SESSION_KEY = 11342
TOPICS = (SESSIONS_TOPIC, TIMING_TOPIC, STATUS_TOPIC, EVENTS_TOPIC, TELEMETRY_TOPIC)
PROBE = "flechtwerk.f1_probe"

QUEUES = ("f1_status_queue", "f1_events_queue", "f1_telemetry_queue")
TARGETS = ("f1_standings", "f1_weather", "f1_clock", "f1_heartbeats", "f1_sessions", "f1_drivers",
           "f1_laps", "f1_pit_stops", "f1_track_status", "f1_race_control", "f1_overtakes",
           "f1_championship", "f1_car_telemetry", "f1_positions")
EXPECTED_OBJECTS = {*QUEUES, *TARGETS, *(f"{name}_mv" for name in TARGETS)}


# --- both stages over a real broker ---

async def _create_topics(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may exist on the session broker
            await admin.create_topics([
                NewTopic(topic, num_partitions=PARTITIONS, replication_factor=1,
                         topic_configs=({"cleanup.policy": "compact"} if topic == SESSIONS_TOPIC
                                        else {"retention.ms": FOREVER}))
                for topic in TOPICS])
    finally:
        await admin.close()


async def _seed_session(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            SESSIONS_TOPIC, key=SESSION_PATH.encode(),
            value=json.dumps({"kind": SESSION_KIND, "path": SESSION_PATH, "year": 2026,
                              "telemetry": True}).encode())
    finally:
        await producer.stop()


async def test_the_tape_flows_through_both_stages(kafka_bootstrap: str) -> None:
    await _create_topics(kafka_bootstrap)
    await _seed_session(kafka_bootstrap)

    archive = Archive()
    ingest = Flechtwerk.of(application_id="f1-ingest-it", bootstrap_servers=kafka_bootstrap,
                           client_id="f1-ingest-it-0", poll_interval=timedelta(milliseconds=200),
                           stage=TapeIngest(archive.client, base_url="http://archive.test/static"))
    board = Flechtwerk.of(application_id="f1-timing-it", bootstrap_servers=kafka_bootstrap,
                          client_id="f1-timing-it-0", stage=timing)
    tasks = [asyncio.create_task(ingest.run()), asyncio.create_task(board.run())]
    try:
        consumer = AIOKafkaConsumer(STATUS_TOPIC, EVENTS_TOPIC, TELEMETRY_TOPIC,
                                    bootstrap_servers=kafka_bootstrap, group_id=None,
                                    auto_offset_reset="earliest",
                                    isolation_level="read_committed")
        await consumer.start()
        collected: dict[str, list[dict]] = {topic: [] for topic in
                                            (STATUS_TOPIC, EVENTS_TOPIC, TELEMETRY_TOPIC)}
        try:
            # The last thing the fixture tape produces is the session's "Ends" dimension row, so
            # waiting for it is the tightest signal that the whole chain ran to completion.
            deadline = asyncio.get_running_loop().time() + 120.0
            while not any(row["kind"] == "session" and row.get("status") == "Ends"
                          for row in collected[EVENTS_TOPIC]):
                for task in tasks:
                    if task.done():
                        task.result()
                if asyncio.get_running_loop().time() > deadline:
                    pytest.fail(f"the tape never reached its end: "
                                f"{ {k: len(v) for k, v in collected.items()} }")
                for tp, records in (await consumer.getmany(timeout_ms=500)).items():
                    for record in records:
                        collected[tp.topic].append(json.loads(record.value))
        finally:
            await consumer.stop()
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    events = collected[EVENTS_TOPIC]
    assert {row["kind"] for row in events} >= {
        "session", "driver", "track_period", "lap", "pit", "race_control", "championship"}
    assert all(row["session_key"] == SESSION_KEY for rows in collected.values() for row in rows)
    assert all(row["session"] == SESSION_PATH for rows in collected.values() for row in rows)

    laps = [row for row in events if row["kind"] == "lap"]
    vsc = [row for row in laps if row["track_status"] == "VSCDeployed"]
    assert vsc and all(row["clean"] is False for row in vsc)
    assert any(row["clean"] is True for row in laps)

    # Same-key serial processing kept the tape order: the continuous stream's event times are
    # non-decreasing, which is the property every as-of dashboard query depends on.
    statuses = [row["event_time"] for row in collected[STATUS_TOPIC]]
    assert statuses == sorted(statuses)

    periods = [row for row in events if row["kind"] == "track_period"]
    assert any("ended_at" not in row for row in periods)     # an open period, for the annotation
    assert any("ended_at" in row for row in periods)         # ... and its closed successor
    assert collected[TELEMETRY_TOPIC]


# --- the ClickHouse schema, and what its views compute ---

async def test_the_schema_applies(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    async with _client(clickhouse) as client:
        tables = set((await _sql(client, "SELECT name FROM system.tables "
                                         "WHERE database = 'flechtwerk'")).split())
    assert EXPECTED_OBJECTS <= tables


async def test_the_materialized_views_project_real_board_records(
        clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    records = await _board_records()

    async with _client(clickhouse) as client:
        await _sql(client, f"DROP TABLE IF EXISTS {PROBE}")
        await _sql(client, f"CREATE TABLE {PROBE} (message JSON) ENGINE = Memory")
        body = "\n".join(json.dumps({"message": record}) for record in records)
        await _sql(client, f"INSERT INTO {PROBE} FORMAT JSONEachRow\n{body}")
        assert int((await _sql(client, f"SELECT count() FROM {PROBE}")).strip()) == len(records)

        # Every view must project at least one row, and none may throw on the kinds it filters
        # out — the whole reason the views select on `message.kind` rather than by topic.
        for target in TARGETS:
            rows = await _rows(client, await _projection(client, f"{target}_mv"))
            assert rows, f"{target}_mv projected nothing"
            assert all(row["session_key"] == SESSION_KEY for row in rows), target

        laps = {(row["racing_number"], row["lap"]): row
                for row in await _rows(client, await _projection(client, "f1_laps_mv"))}
        # One assertion, five mechanisms: offset parsing, t0 anchoring, cross-feed ordering, the
        # flag state machine, and the view's own casts.
        vsc = laps[("1", 3)]
        assert vsc["track_status"] == "VSCDeployed" and vsc["clean"] is False
        assert vsc["lap_ms"] == 105000
        assert laps[("1", 5)]["clean"] is True               # the one clean lap
        assert laps[("1", 2)]["sector3_ms"] == 23042

        standings = await _rows(client, await _projection(client, "f1_standings_mv"))
        assert {row["racing_number"] for row in standings} == {"1", "16", "81"}
        # NULL, not 0.0: an unknown gap must never read as "level with the leader".
        assert any(row["gap_s"] is None for row in standings)
        assert any(row["gap_s"] == 1.234 for row in standings)
        assert any(row["gap_laps"] == 1 and row["gap_s"] is None for row in standings)

        session_rows = await _rows(client, await _projection(client, "f1_sessions_mv"))
        # The UTC bounds the board computed once so no panel has to: 15:00 local at +02:00.
        assert session_rows[0]["start_utc"].startswith("2026-07-26 13:00:00")
        assert session_rows[0]["end_utc"].startswith("2026-07-26 15:00:00")
        assert session_rows[0]["label"] == "Hungarian Grand Prix — Race (2026)"

        periods = await _rows(client, await _projection(client, "f1_track_status_mv"))
        assert any(row["label"] == "VSCDeployed" and row["severity"] == 3 for row in periods)
        assert any(row["ended_at"] is None for row in periods)   # an open period stays NULL-ended

        telemetry = await _rows(client, await _projection(client, "f1_car_telemetry_mv"))
        # The upstream's own 0–104 scale, stored verbatim rather than rescaled to a percentage.
        assert any(row["throttle"] == 104 and row["brake"] == 104 for row in telemetry)
        positions = await _rows(client, await _projection(client, "f1_positions_mv"))
        assert any(row["x"] < 0 for row in positions)            # Int32, because X goes negative

        await _sql(client, f"DROP TABLE {PROBE}")


async def _board_records() -> list[dict]:
    """The board's real output records for the fixture session — no Kafka, no ClickHouse.

    Drives the tape through ``run_board`` exactly as the transformer does, which is what makes
    these the same JSON the Kafka engine would be handed in production.
    """
    fetches = [tape.frame((FIXTURES / f"{feed}.jsonStream").read_bytes(), feed=feed, start=0,
                          final=True) for feed in sorted(INGESTED)]
    t0_ms = int(T0.timestamp() * 1000)
    state, records = State(), []
    for line in tape.merge(fetches, bound=None, limit=10_000):
        value = Event({SESSION: SESSION_PATH, FEED: line.feed, OFFSET_MS: line.offset_ms,
                       EVENT_TIME: tape.event_time(t0_ms, line.offset_ms)})
        value.raw[PAYLOAD] = line.payload
        message = IncomingMessage(key=SESSION_PATH, offset=0, partition=0, timestamp=None,
                                  topic=TIMING_TOPIC, value=value)
        async for item in run_board(state, message):
            if isinstance(item, State):
                state = item
            elif isinstance(item, Message):
                records.append(json.loads(json.dumps(item.value.raw)))
    return records


def _client(clickhouse: dict[str, str]) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=clickhouse["base_url"], timeout=60.0, params={
        "user": clickhouse["user"], "password": clickhouse["password"],
        "database": clickhouse["database"]})


async def _sql(client: httpx.AsyncClient, query: str) -> str:
    response = await client.post("/", content=query)
    response.raise_for_status()
    return response.text


async def _rows(client: httpx.AsyncClient, query: str) -> list[dict]:
    """A projection's rows as dicts — JSONEachRow so assertions read by column name."""
    text = await _sql(client, f"SELECT * FROM ({query}) FORMAT JSONEachRow")
    return [json.loads(line) for line in text.splitlines() if line]


async def _projection(client: httpx.AsyncClient, view: str) -> str:
    """A materialized view's own SELECT, rewired from its Kafka queue to the probe table.

    Reading it back out of ``system.tables`` rather than re-stating it here is the point: the
    query under test is byte-for-byte the one ``clickhouse.sql`` installed, so the test cannot
    drift from the schema it is checking.
    """
    # TSVRaw, not the default TabSeparated: the latter escapes every quote in the stored SQL
    # (`CAST(x, \'UInt32\')`), which then fails to parse when fed back in.
    query = (await _sql(client, "SELECT as_select FROM system.tables "
                                f"WHERE database = 'flechtwerk' AND name = '{view}' "
                                "FORMAT TSVRaw")).strip()
    assert query, f"{view} has no as_select — did the schema change shape?"
    for queue in QUEUES:
        query = query.replace(f"flechtwerk.{queue}", PROBE)
    return query

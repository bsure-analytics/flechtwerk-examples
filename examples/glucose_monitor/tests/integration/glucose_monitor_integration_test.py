"""Tier 3 — integration. The ingest over a real broker, and the schema on a real server.

- ``test_ingest_backs_up_over_the_broker`` produces an account record **encrypted** to
  ``glucose-accounts`` (the typed write runs under the fixture keyring), starts the real
  ``GlucoseIngest`` with that keyring and a ``MockTransport`` LibreLinkUp, and reads
  ``glucose-raw`` back under ``read_committed``. It proves the shape end to end over real Kafka:
  the stage reads the compacted config topic, decrypts the credentials on read, signs in, commits
  one raw record per followed patient, and leaves its session on the changelog as one ciphertext
  token — nothing of it in the clear.
- ``test_the_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and asserts every
  object exists.
- ``test_the_view_projects_real_readings`` runs the materialized view's **own SELECT** — read back
  out of ``system.tables``, its Kafka queue swapped for a ``Memory`` table (the F1 technique: a
  Kafka-engine queue cannot be repointed at an ephemeral broker) — over the records the readings
  stage really emits for the fixture. That exercises what can break here: the ``::`` casts, the
  ``Nullable`` promotions, and the timestamp parsers.
"""
import asyncio
import hashlib
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from flechtwerk import Config, Event, IncomingMessage, Message, State
from flechtwerk.attribute import Record
from flechtwerk.module import Flechtwerk
from flechtwerk.testing import fixture_keyring, installed_keyring

from examples.glucose_monitor.attributes import (
    ACCOUNT,
    ACCOUNTS_TOPIC,
    EMAIL,
    FETCHED_AT,
    METADATA,
    PASSWORD,
    PATIENT_ID,
    RAW_TOPIC,
    REGION,
    RESPONSE,
)
from examples.glucose_monitor.ingest import GlucoseIngest
from examples.glucose_monitor.readings import GlucoseReadings
from examples.glucose_monitor.setup import apply_clickhouse_schema

pytestmark = pytest.mark.integration

GRAPH = json.loads((Path(__file__).parents[1] / "fixtures" / "graph.json").read_text())
PATIENT = GRAPH["data"]["connection"]["patientId"]
NOW = datetime(2026, 10, 29, 7, 14, 0, tzinfo=timezone.utc)
USER_ID = "4a5b6c7d-0000-4000-8000-0123456789ab"
PROBE = "flechtwerk.glucose_readings_probe"


async def test_ingest_backs_up_over_the_broker(kafka_bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics([
                NewTopic(ACCOUNTS_TOPIC, num_partitions=8, replication_factor=1,
                         topic_configs={"cleanup.policy": "compact"}),
                NewTopic(RAW_TOPIC, num_partitions=8, replication_factor=1)])
    finally:
        await admin.close()

    def libre(request: httpx.Request) -> httpx.Response:
        ticket = {"token": "integration-ticket", "expires": int(NOW.timestamp()) + 21_600}
        if request.url.path == "/llu/auth/login":
            # Decrypted on read: the stage sends the plaintext the typed write sealed.
            assert json.loads(request.content) == {"email": "it@example.org", "password": "it-secret"}
            return httpx.Response(200, json={"status": 0, "data": {
                "user": {"id": USER_ID}, "authTicket": ticket}})
        assert request.headers["account-id"] == hashlib.sha256(USER_ID.encode()).hexdigest()
        headers = {"Date": format_datetime(NOW, usegmt=True)}
        if request.url.path == "/llu/connections":
            return httpx.Response(200, headers=headers, json={
                "status": 0, "data": [GRAPH["data"]["connection"]], "ticket": ticket})
        return httpx.Response(200, headers=headers, json={**GRAPH, "ticket": ticket})

    stage = GlucoseIngest(httpx.AsyncClient(transport=httpx.MockTransport(libre)), clock=lambda: NOW)
    app = Flechtwerk.of(application_id="glucose-ingest-it", bootstrap_servers=kafka_bootstrap,
                        client_id="glucose-ingest-it-0", poll_interval=timedelta(milliseconds=200),
                        stage=stage, keyring=fixture_keyring())
    consumer = AIOKafkaConsumer(RAW_TOPIC, bootstrap_servers=kafka_bootstrap,
                                auto_offset_reset="earliest", group_id=None,
                                isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = None
    try:
        with installed_keyring():  # the typed write encrypts email and password
            account = Config({ACCOUNT: "it", EMAIL: "it@example.org", PASSWORD: "it-secret",
                              REGION: "de"}).raw
        assert account["password"].startswith("flenc:jwe:") and "it-secret" not in json.dumps(account)
        await producer.send_and_wait(ACCOUNTS_TOPIC, key=b"it", value=json.dumps(account).encode())

        task = asyncio.create_task(app.run())
        raw: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 60.0
        while not raw:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("never saw a backed-up graph response")
            for records in (await consumer.getmany(timeout_ms=500)).values():
                for record in records:
                    assert record.key == PATIENT.encode()
                    raw.append(json.loads(record.value))

        assert raw[0]["response"] == GRAPH["data"]
        assert raw[0]["metadata"] == {"account": "it", "patient_id": PATIENT,
                                      "fetched_at": "2026-10-29T07:14:00Z"}
        assert "integration-ticket" not in json.dumps(raw[0])

        # The session rests on the real changelog as one ciphertext token, nothing in the clear.
        changelog = AIOKafkaConsumer("glucose-ingest-it-changelog", bootstrap_servers=kafka_bootstrap,
                                     auto_offset_reset="earliest", group_id=None,
                                     isolation_level="read_committed")
        await changelog.start()
        try:
            stored = [record.value for records in (await changelog.getmany(timeout_ms=5000)).values()
                      for record in records if record.value]
        finally:
            await changelog.stop()
        assert len(stored) == 1 and stored[0].startswith(b'{"session":"flenc:jwe:')
        assert b"integration-ticket" not in stored[0] and b"libreview" not in stored[0]
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await producer.stop()
        await consumer.stop()


# --- the ClickHouse schema, and what its view computes ---

async def test_the_schema_applies(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    async with _client(clickhouse) as client:
        tables = set((await _sql(client, "SELECT name FROM system.tables "
                                         "WHERE database = 'flechtwerk'")).split())
        columns = await _sql(client, "SELECT name FROM system.columns WHERE database = 'flechtwerk' "
                                     "AND table = 'glucose_readings'")
    assert {"glucose_readings_queue", "glucose_readings", "glucose_readings_mv"} <= tables
    assert "__time" in columns.split()


async def test_the_view_projects_real_readings(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    records = await _readings_records()

    async with _client(clickhouse) as client:
        await _sql(client, f"DROP TABLE IF EXISTS {PROBE}")
        await _sql(client, f"CREATE TABLE {PROBE} (message JSON) ENGINE = Memory")
        body = "\n".join(json.dumps({"message": record}) for record in records)
        await _sql(client, f"INSERT INTO {PROBE} FORMAT JSONEachRow\n{body}")

        rows = await _rows(client, await _projection(client, "glucose_readings_mv"))
        assert len(rows) == len(records)
        live = next(row for row in rows if row["kind"] == "live")
        assert live["patient_id"] == PATIENT
        assert live["measured_at"].startswith("2026-10-29 07:13:41")
        assert live["mg_dl"] == 203 and live["trend_arrow"] == 4
        assert live["utc_offset_min"] == 60 and live["is_high"] is False
        history = [row for row in rows if row["kind"] == "history"]
        # NULL, not 0: a history point has no arrow, and 0 would read as "falling fast".
        assert history and all(row["trend_arrow"] is None for row in history)
        assert all(row["fetched_at"].startswith("2026-10-29 07:14:00") for row in rows)

        await _sql(client, f"DROP TABLE {PROBE}")


async def _readings_records() -> list[dict]:
    """The readings stage's real output for the fixture — no Kafka, no ClickHouse."""
    value = Event({METADATA: Record({ACCOUNT: "it", PATIENT_ID: PATIENT, FETCHED_AT: NOW}),
                   RESPONSE: Record.wrap(GRAPH["data"])})
    message = IncomingMessage(key=PATIENT, offset=0, partition=0, timestamp=None, topic=RAW_TOPIC,
                              value=value)
    records = []
    async for item in GlucoseReadings().transform(message, State()):
        if isinstance(item, Message):
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
    text = await _sql(client, f"SELECT * EXCEPT payload FROM ({query}) FORMAT JSONEachRow")
    return [json.loads(line) for line in text.splitlines() if line]


async def _projection(client: httpx.AsyncClient, view: str) -> str:
    """The materialized view's own SELECT, rewired from its Kafka queue to the probe table —
    byte for byte the query ``clickhouse.sql`` installed, so the test cannot drift from it."""
    # TSVRaw, not the default TabSeparated: the latter escapes every quote in the stored SQL.
    query = (await _sql(client, "SELECT as_select FROM system.tables "
                                f"WHERE database = 'flechtwerk' AND name = '{view}' "
                                "FORMAT TSVRaw")).strip()
    assert query, f"{view} has no as_select — did the schema change shape?"
    return query.replace("flechtwerk.glucose_readings_queue", PROBE)

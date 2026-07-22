"""Tier 3 — integration. The co-partitioned delay join against a real broker, and the
ClickHouse schema applied against a real server.

- ``test_delay_join_over_the_broker`` produces a trip's profile and then, a beat later,
  a live update — to the two topics keyed identically by ``trip_id`` — and runs the real
  ``delays`` transformer. It proves the join end to end: the update lands on the same
  partition/task as its profile (real key-hash co-partitioning), reads the stored profile,
  and emits a delay record read back under ``read_committed``.
- ``test_clickhouse_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and
  asserts the queue, the two target tables, and the two materialized views are created —
  catching any DDL error the Docker-free tiers can't.
"""
import asyncio
import json
import zipfile
from contextlib import suppress
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.module import Flechtwerk

from examples.gtfs_delay_monitor.delays import DELAYS_TOPIC, build_delay_state, classify, delays
from examples.gtfs_delay_monitor.ingest import UPDATES_TOPIC, decode_feed
from examples.gtfs_delay_monitor.loader import PROFILES_TOPIC, build_profiles
from examples.gtfs_delay_monitor.setup import apply_clickhouse_schema

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _live_pair() -> tuple[dict, dict, str, str]:
    """A (profile.raw, update.raw, trip_id, feed_ts_iso) for a mid-journey fixture trip."""
    profiles = dict(build_profiles((FIXTURES / "fv_sample.zip").read_bytes(), "v1"))
    feed_ts, updates = decode_feed((FIXTURES / "rt_sample.pb").read_bytes())
    for tid, update in updates:
        if tid in profiles and build_delay_state(profiles[tid], update, feed_ts) is not None:
            return profiles[tid].raw, update.raw, tid, feed_ts.isoformat()
    raise AssertionError("no mid-journey fixture trip")


async def test_delay_join_over_the_broker(kafka_bootstrap: str) -> None:
    profile_raw, update_raw, trip_id, _ = _live_pair()

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics([NewTopic(t, num_partitions=8, replication_factor=1)
                                       for t in (PROFILES_TOPIC, UPDATES_TOPIC, DELAYS_TOPIC)])
    finally:
        await admin.close()

    app = Flechtwerk.of(application_id="gtfs-delays-it", bootstrap_servers=kafka_bootstrap,
                        client_id="gtfs-delays-it-0", stage=delays)
    consumer = AIOKafkaConsumer(DELAYS_TOPIC, bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest",
                                group_id=None, isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        # Profile first (stored as state), then — after the stage has surely processed it —
        # the live update, which joins the stored profile into a delay record.
        await producer.send_and_wait(PROFILES_TOPIC, key=trip_id.encode(), value=json.dumps(profile_raw).encode())
        await asyncio.sleep(2.0)
        await producer.send_and_wait(UPDATES_TOPIC, key=trip_id.encode(), value=json.dumps(update_raw).encode())

        record: dict = {}
        deadline = asyncio.get_running_loop().time() + 60.0
        while record.get("trip_id") != trip_id:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never emitted a delay record for {trip_id}: {record}")
            for _tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for r in records:
                    record = json.loads(r.value)

        assert record["trip_id"] == trip_id
        assert record["status"] == classify(record["delay_s"])            # self-consistent bucket
        assert 45.0 < record["lat"] < 56.0 and 5.0 < record["lon"] < 16.0  # placed at a German station
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
    assert {"gtfs_train_delays_queue", "gtfs_train_delays", "gtfs_train_delays_mv",
            "gtfs_delay_history", "gtfs_delay_history_mv"} <= tables

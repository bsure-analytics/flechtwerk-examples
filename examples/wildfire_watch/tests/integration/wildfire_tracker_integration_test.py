"""Tier 3 — integration. Clustering over a real broker, and the schema against a real server.

- ``test_tracker_clusters_over_the_broker`` produces three detections (two within the link
  distance of each other, one far away) plus a ``sweep`` for one region key and runs the real
  ``tracker`` transformer. It proves the whole shape end to end over real Kafka: the region's
  records co-partition onto one task, same-key serial processing keeps the
  detections-before-sweep order, clustering accumulates in the changelog-backed join state, and
  the sweep produces one status per fire — all read back under ``read_committed``. A second
  sweep past the extinction timeout then retires both fires.
- ``test_clickhouse_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and asserts
  every object is created — three Kafka queues, five target tables, and the five materialized
  views (note ``wildfire-status`` feeds two tables, and ``wildfire-detections`` feeds two) — catching
  any DDL error the Docker-free tiers can't.
"""
import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Event
from flechtwerk.module import Flechtwerk

from examples.wildfire_watch.attributes import (
    ACQUIRED_AT,
    CONFIDENCE,
    DAYNIGHT,
    DETECTION_ID,
    DETECTIONS_TOPIC,
    EVENTS_TOPIC,
    FETCHED_AT,
    FRP,
    INSTRUMENT,
    KIND,
    LAT,
    LON,
    NEW_DETECTIONS,
    REGION,
    SATELLITE,
    SCAN,
    STATUS_TOPIC,
    SWEEP_AT,
    TRACK,
)
from examples.wildfire_watch.setup import apply_clickhouse_schema
from examples.wildfire_watch.tracker import ACTIVE, EXTINGUISHED, IGNITION, tracker
from examples.wildfire_watch.tracking import EXTINGUISH_AFTER

pytestmark = pytest.mark.integration

UTC = timezone.utc
REGION_SLUG = "alentejo-portugal"
_T = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)


def _detection(identity: str, lat: float, lon: float, *, frp: float = 10.0) -> bytes:
    return json.dumps(Event({
        KIND: "detection", REGION: REGION_SLUG, DETECTION_ID: identity,
        LAT: lat, LON: lon, ACQUIRED_AT: _T, SATELLITE: "N20", INSTRUMENT: "VIIRS",
        CONFIDENCE: "n", SCAN: 0.4, TRACK: 0.4, DAYNIGHT: "D", FETCHED_AT: _T, FRP: frp,
    }).raw).encode()


def _sweep(sweep_at: datetime, new_detections: int = 0) -> bytes:
    return json.dumps(Event({KIND: "sweep", REGION: REGION_SLUG,
                             SWEEP_AT: sweep_at, NEW_DETECTIONS: new_detections}).raw).encode()


async def test_tracker_clusters_over_the_broker(kafka_bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics([NewTopic(topic, num_partitions=8, replication_factor=1)
                                       for topic in (DETECTIONS_TOPIC, STATUS_TOPIC, EVENTS_TOPIC)])
    finally:
        await admin.close()

    app = Flechtwerk.of(application_id="wildfire-tracker-it", bootstrap_servers=kafka_bootstrap,
                        client_id="wildfire-tracker-it-0", stage=tracker)
    consumer = AIOKafkaConsumer(STATUS_TOPIC, EVENTS_TOPIC, bootstrap_servers=kafka_bootstrap,
                                auto_offset_reset="earliest", group_id=None,
                                isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        key = REGION_SLUG.encode()
        # Two pixels ~500 m apart (one fire) and one ~55 km away (a second fire).
        await producer.send_and_wait(DETECTIONS_TOPIC, key=key,
                                     value=_detection("aaa", 37.0, -7.0, frp=42.0))
        await producer.send_and_wait(DETECTIONS_TOPIC, key=key,
                                     value=_detection("bbb", 37.0045, -7.0))
        await producer.send_and_wait(DETECTIONS_TOPIC, key=key,
                                     value=_detection("ccc", 37.5, -7.0))
        await producer.send_and_wait(DETECTIONS_TOPIC, key=key,
                                     value=_sweep(_T + timedelta(minutes=5), new_detections=3))

        statuses: list[dict] = []
        events: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 60.0
        while len(statuses) < 2:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never saw two statuses: statuses={statuses} events={events}")
            for tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for record in records:
                    (statuses if tp.topic == STATUS_TOPIC else events).append(json.loads(record.value))

        # Two ignitions, one per fire — the two nearby pixels made a single fire.
        assert [e["kind"] for e in events] == [IGNITION, IGNITION]
        assert {e["fire_id"] for e in events} == {"F-aaa", "F-ccc"}
        by_fire = {s["fire_id"]: s for s in statuses}
        assert set(by_fire) == {"F-aaa", "F-ccc"}
        assert all(s["status"] == ACTIVE for s in statuses)
        assert by_fire["F-aaa"]["detections"] == 2 and by_fire["F-aaa"]["frp_max"] == 42.0
        assert by_fire["F-ccc"]["detections"] == 1
        assert by_fire["F-aaa"]["as_of"] == "2026-07-25T18:05:00Z"

        # A sweep past the timeout retires both fires and tombstones the region's state.
        await producer.send_and_wait(
            DETECTIONS_TOPIC, key=key, value=_sweep(_T + EXTINGUISH_AFTER + timedelta(minutes=1)))
        extinguished: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 60.0
        while len(extinguished) < 2:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never extinguished both fires: {extinguished}")
            for tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for record in records:
                    value = json.loads(record.value)
                    if tp.topic == EVENTS_TOPIC and value["kind"] == EXTINGUISHED:
                        extinguished.append(value)

        assert {e["fire_id"] for e in extinguished} == {"F-aaa", "F-ccc"}
        assert all(e["first_seen"] == "2026-07-25T18:00:00Z" for e in extinguished)
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
        "user": clickhouse["user"], "password": clickhouse["password"],
        "database": clickhouse["database"],
    }) as client:
        response = await client.post(
            "/", content="SELECT name FROM system.tables WHERE database = 'flechtwerk'")
        response.raise_for_status()
        tables = set(response.text.split())
    assert {
        # three Kafka queues
        "wildfire_detections_queue", "wildfire_status_queue", "wildfire_events_queue",
        # five target tables — wildfire-detections feeds two, wildfire-status feeds two
        "wildfire_detections", "wildfire_sweeps", "wildfire_status", "wildfire_active", "wildfire_events",
        # and one materialized view each
        "wildfire_detections_mv", "wildfire_sweeps_mv", "wildfire_status_mv", "wildfire_active_mv",
        "wildfire_events_mv",
    } <= tables

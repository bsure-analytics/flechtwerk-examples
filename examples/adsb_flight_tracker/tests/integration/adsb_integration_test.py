"""Tier 3 — integration. Ephemeral Kafka via testcontainers, real runners.

Runs the actual ``Flechtwerk.of(...).run()`` for **both** pipeline stages against a
real broker (the shared session-scoped Kafka fixture from the repo-root
``conftest.py``) — ingest → ``adsb.raw`` → enrich — with only the HTTP feed and the
enrichment services stubbed. It proves the whole path end to end: config
bootstrap, the raw hand-off, per-page transactions, live-cached enrichment, and —
across two polls — an enriched position, a departure tombstone, and derived
``emergency``/``going_dark`` events landing under ``read_committed``.
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.attribute import Record
from flechtwerk.module import Flechtwerk

from examples.adsb_flight_tracker.attributes import (
    AIRCRAFT_TYPE_NAME,
    AIRLINE,
    AIRLINE_WIKI,
    NEAREST_PLACE,
    OVER_COUNTRY,
    TYPE_WIKI,
)
from examples.adsb_flight_tracker.enrich import AIRCRAFT_TOPIC, CELLS_TOPIC, EVENTS_TOPIC, AdsbEnrich
from examples.adsb_flight_tracker.ingest import CONFIG_TOPIC, RAW_TOPIC, AdsbIngest

pytestmark = pytest.mark.integration

PRESENT = {"now": 1_700_000_000_000, "ac": [
    {"hex": "a11111", "flight": "TES1  ", "lat": 51.5, "lon": -0.4, "alt_baro": 30000, "gs": 420.0, "t": "A320", "squawk": "7700"},
    {"hex": "b22222", "flight": "TES2  ", "lat": 51.4, "lon": -0.5, "alt_baro": 31000, "gs": 400.0},
]}
DEPARTED = {"now": 1_700_000_005_000, "ac": [
    {"hex": "a11111", "flight": "TES1  ", "lat": 51.6, "lon": -0.3, "alt_baro": 30000, "gs": 430.0, "t": "A320", "squawk": "7700"},
]}  # b22222 has left the feed (it was airborne → a "going_dark" event)


class _FakeEnricher:
    """Deterministic enrichment — no live Wikidata/Nominatim in CI."""

    async def airline(self, icao: str) -> Record:
        return Record({AIRLINE: f"Airline {icao}", AIRLINE_WIKI: f"https://en.wikipedia.org/wiki/{icao}"})

    async def aircraft_type(self, designator: str) -> Record:
        return Record({AIRCRAFT_TYPE_NAME: f"Type {designator}", TYPE_WIKI: f"https://en.wikipedia.org/wiki/{designator}"})

    async def geocode(self, lat: float, lon: float) -> Record:
        return Record({OVER_COUNTRY: "United Kingdom", NEAREST_PLACE: "London"})


class _FakeGeocoder:
    """The seeded region carries lat/lon, so ``enrich_config`` never forward-geocodes —
    this keeps the ingest stage offline and asserts that coords-present path."""

    async def locate(self, query: str) -> tuple[float, float]:
        raise AssertionError("the seeded region carries lat/lon — no forward geocode expected")


def _stub_client() -> httpx.AsyncClient:
    """First poll sees both aircraft; every later poll sees only a11111."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=PRESENT if calls["n"] == 1 else DEPARTED)

    return httpx.AsyncClient(base_url="https://api.adsb.lol", transport=httpx.MockTransport(handler))


async def _create_topics(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(CONFIG_TOPIC, num_partitions=8, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
            *(NewTopic(topic, num_partitions=8, replication_factor=1)
              for topic in (RAW_TOPIC, AIRCRAFT_TOPIC, EVENTS_TOPIC, CELLS_TOPIC)),
        ])
    finally:
        await admin.close()


async def _seed_region(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            CONFIG_TOPIC,
            key=b"london",
            value=json.dumps({"name": "london", "lat": 51.47, "lon": -0.45, "radius": 100}).encode(),
        )
    finally:
        await producer.stop()


def _pipeline(bootstrap: str) -> tuple[Flechtwerk, Flechtwerk]:
    suffix = uuid4().hex[:8]
    ingest_stage = AdsbIngest()
    ingest_stage.client = _stub_client()
    ingest_stage.geocoder = _FakeGeocoder()  # seeded config carries coords → never invoked
    ingest = Flechtwerk.of(
        application_id=f"adsb-ingest-{suffix}",
        bootstrap_servers=bootstrap,
        client_id=f"adsb-ingest-{suffix}-0",
        poll_interval=timedelta(milliseconds=200),
        stage=ingest_stage,
    )
    enrich = Flechtwerk.of(
        application_id=f"adsb-enrich-{suffix}",
        bootstrap_servers=bootstrap,
        client_id=f"adsb-enrich-{suffix}-0",
        stage=AdsbEnrich(enricher=_FakeEnricher()),
    )
    return ingest, enrich


async def test_ingest_then_enrich_land_positions_and_events(kafka_bootstrap: str) -> None:
    await _create_topics(kafka_bootstrap)
    await _seed_region(kafka_bootstrap)
    ingest, enrich = _pipeline(kafka_bootstrap)

    consumer = AIOKafkaConsumer(
        AIRCRAFT_TOPIC, EVENTS_TOPIC,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",  # never observe an aborted page
    )
    await consumer.start()
    tasks = [asyncio.create_task(ingest.run()), asyncio.create_task(enrich.run())]
    try:
        positions: dict[str, dict] = {}
        tombstoned: set[str] = set()
        events: set[str] = set()
        deadline = asyncio.get_running_loop().time() + 90.0

        def done() -> bool:
            return (positions.get("a11111", {}).get("emergency") == 1
                    and "airline" in positions.get("a11111", {})
                    and "b22222" in tombstoned
                    and {"emergency", "going_dark"} <= events)

        while not done():
            for task in tasks:
                if task.done():
                    task.result()  # surface a crash instead of timing out
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"incomplete: positions={positions}, tombstoned={tombstoned}, events={events}")
            batch = await consumer.getmany(timeout_ms=500)
            for topic_partition, records in batch.items():
                for record in records:
                    value = json.loads(record.value)
                    if topic_partition.topic == AIRCRAFT_TOPIC:
                        if value["is_deleted"] == 1:
                            tombstoned.add(value["hex"])
                        else:
                            positions[value["hex"]] = value
                    else:
                        events.add(value["event_type"])

        assert positions["a11111"]["airline"] == "Airline TES"     # live-cached enrichment applied
        assert positions["a11111"]["aircraft_type_name"] == "Type A320"
        assert positions["a11111"]["over_country"] == "United Kingdom"
        assert positions["a11111"]["emergency"] == 1
        assert "b22222" in tombstoned                              # departure tombstoned
        assert {"emergency", "going_dark"} <= events               # derived events landed
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        await consumer.stop()

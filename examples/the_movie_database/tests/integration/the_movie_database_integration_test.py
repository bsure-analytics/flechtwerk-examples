"""Tier 3 — integration. The catalog over a real broker, and the schema against a real server.

- ``test_catalog_versions_over_the_broker`` produces the ``movie`` feed record **encrypted** to
  ``tmdb-feeds`` (the typed write runs under the fixture keyring), starts the real ``TmdbCatalog``
  with that keyring and a ``MockTransport`` serving the Fight Club fixture, then produces one
  change record keyed ``movie:550``. It proves the shape end to end over real Kafka: the stage
  reads the compacted config topic, decrypts the token on read, fetches, projects, and emits one
  version plus one row per credit, all read back under ``read_committed``.
- ``test_clickhouse_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and asserts
  every object is created — five Kafka queues, eight tables and eight materialized views (each
  entity topic feeds a history and a ``_current`` table) — catching any DDL error the Docker-free
  tiers can't.
"""
import asyncio
import json
from contextlib import suppress
from datetime import date, datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from flechtwerk import Config, Event
from flechtwerk.module import Flechtwerk
from flechtwerk.testing import fixture_keyring, installed_keyring

from examples.the_movie_database.attributes import (
    ACCESS_TOKEN,
    CHANGED_ON,
    CHANGES_TOPIC,
    CREDITS_TOPIC,
    FEED,
    FEEDS_TOPIC,
    LISTING,
    MOVIES_TOPIC,
    PEOPLE_TOPIC,
    TMDB_ID,
    TV_TOPIC,
)
from examples.the_movie_database.catalog import TmdbCatalog
from examples.the_movie_database.setup import apply_clickhouse_schema

pytestmark = pytest.mark.integration

MOVIE = json.loads((Path(__file__).parents[1] / "fixtures" / "movie_550.json").read_text())


async def test_catalog_versions_over_the_broker(kafka_bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics(
                [NewTopic(FEEDS_TOPIC, num_partitions=8, replication_factor=1,
                          topic_configs={"cleanup.policy": "compact"})]
                + [NewTopic(topic, num_partitions=8, replication_factor=1)
                   for topic in (CHANGES_TOPIC, MOVIES_TOPIC, TV_TOPIC, PEOPLE_TOPIC, CREDITS_TOPIC)])
    finally:
        await admin.close()

    def serve_the_movie(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer integration-token"   # decrypted on read
        return httpx.Response(200, json=MOVIE, headers={
            "Date": format_datetime(datetime(2026, 10, 23, 10, 0, tzinfo=timezone.utc), usegmt=True)})

    stage = TmdbCatalog(httpx.AsyncClient(transport=httpx.MockTransport(serve_the_movie)),
                        base_url="http://tmdb.test/3")
    app = Flechtwerk.of(application_id="tmdb-catalog-it", bootstrap_servers=kafka_bootstrap,
                        client_id="tmdb-catalog-it-0", stage=stage, keyring=fixture_keyring())
    consumer = AIOKafkaConsumer(MOVIES_TOPIC, CREDITS_TOPIC, bootstrap_servers=kafka_bootstrap,
                                auto_offset_reset="earliest", group_id=None,
                                isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = None
    try:
        with installed_keyring():  # the typed write encrypts access_token
            feed = Config({FEED: "movie", ACCESS_TOKEN: "integration-token"}).raw
        assert feed["access_token"].startswith("flenc:jwe:")
        await producer.send_and_wait(FEEDS_TOPIC, key=b"movie", value=json.dumps(feed).encode())

        task = asyncio.create_task(app.run())
        change = Event({FEED: "movie", TMDB_ID: 550, CHANGED_ON: date(2026, 10, 22), LISTING: "closed"})
        await producer.send_and_wait(CHANGES_TOPIC, key=b"movie:550",
                                     value=json.dumps(change.raw).encode())

        cast, crew = MOVIE["credits"]["cast"], MOVIE["credits"]["crew"]
        movies: list[dict] = []
        credits: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 60.0
        while not movies or len(credits) < len(cast) + len(crew):
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never saw the version: movies={len(movies)} credits={len(credits)}")
            for tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for record in records:
                    assert record.key == b"movie:550"
                    (movies if tp.topic == MOVIES_TOPIC else credits).append(json.loads(record.value))

        assert len(movies) == 1
        assert movies[0]["version"] == 1 and movies[0]["title"] == "Fight Club"
        assert movies[0]["media_type"] == "movie" and movies[0]["version_at"] == "2026-10-23T10:00:00Z"
        assert len(credits) == len(cast) + len(crew)
        assert all(c["media_id"] == 550 and c["version"] == 1 for c in credits)
    finally:
        if task is not None:
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
        # five Kafka queues
        "tmdb_changes_queue", "tmdb_movies_queue", "tmdb_tv_queue", "tmdb_people_queue",
        "tmdb_credits_queue",
        # eight tables — each entity topic feeds a history and a current table
        "tmdb_changes", "tmdb_movies", "tmdb_movies_current", "tmdb_tv", "tmdb_tv_current",
        "tmdb_people", "tmdb_people_current", "tmdb_credits",
        # and one materialized view each
        "tmdb_changes_mv", "tmdb_movies_mv", "tmdb_movies_current_mv", "tmdb_tv_mv",
        "tmdb_tv_current_mv", "tmdb_people_mv", "tmdb_people_current_mv", "tmdb_credits_mv",
    } <= tables

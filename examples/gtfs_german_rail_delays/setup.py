"""One-shot setup for the GTFS delay-monitor pipeline — idempotent, safe to re-run.

    uv run poe setup-gtfs

Creates the config topics and the pipeline topics, seeds the two feed configs (which
static feed the loader parses and which realtime feed ingest polls), and applies the
ClickHouse schema. Nothing about the *pipeline data* is seeded — the schedule and the
delays are the live gtfs.de feeds; only the two config records that point the stages at
them are written here. Each stage existence-checks its topics at startup and fails fast
if any is missing, so this is the ops step the framework assumes is done up front.

Scope is **long-distance** (the ``fv_free`` static feed, ``route_type=2``): a tiny feed,
an exact ``trip_id`` join with the national realtime feed, and a recognizable ICE/IC map.
Point ``STATIC_FEED_URL`` at ``rv_free``/``de_full`` (and widen ``loader.RAIL_ROUTE_TYPES``)
to cover regional/local — the README's extension point.
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from examples._setup import quiet_fresh_topic_produce_race

from .delays import DELAYS_TOPIC
from .ingest import DEFAULT_FEED_URL, RT_FEEDS_CONFIG_TOPIC, UPDATES_TOPIC
from .loader import PROFILES_TOPIC, STATIC_SOURCES_CONFIG_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

STATIC_FEED_URL = "https://download.gtfs.de/germany/fv_free/latest.zip"
"""The long-distance static GTFS feed (gtfs.de, DELFI, CC-BY). The demo constant seeded
onto ``gtfs-static-sources``; any producer may update it live (e.g. to a regional feed)."""


async def create_topics() -> None:
    """Create the compacted config/dimension topics and the partitioned data topics.

    ``gtfs-trip-profiles`` (the schedule dimension) and ``gtfs-trip-updates`` (live
    delays) share a partition count and are both keyed by ``trip_id``, so a trip's
    profile and its updates co-partition onto one task — that is the delay join."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (STATIC_SOURCES_CONFIG_TOPIC, 8, {"cleanup.policy": "compact"}),  # static feeds (seeded here)
            (RT_FEEDS_CONFIG_TOPIC, 8, {"cleanup.policy": "compact"}),        # realtime feeds (seeded here)
            (PROFILES_TOPIC, 8, {"cleanup.policy": "compact"}),              # trip profiles (dimension)
            (UPDATES_TOPIC, 8, {}),                                          # co-partitioned with profiles
            (DELAYS_TOPIC, 8, {}),
        ]
        new = [NewTopic(name, num_partitions=parts, replication_factor=1, topic_configs=configs)
               for name, parts, configs in specs if name not in existing]
        if new:
            await admin.create_topics(new)
            print(f"Created topics: {[t.name for t in new]}")
        else:
            print("Topics already present")
    finally:
        await admin.close()


async def seed_configs() -> None:
    """Seed the two feed configs, keyed on their compacted config topics (idempotent)."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            await producer.send_and_wait(STATIC_SOURCES_CONFIG_TOPIC, key=b"fernverkehr",
                                         value=json.dumps({"url": STATIC_FEED_URL}).encode())
            await producer.send_and_wait(RT_FEEDS_CONFIG_TOPIC, key=b"germany-free",
                                         value=json.dumps({"url": DEFAULT_FEED_URL}).encode())
    finally:
        await producer.stop()
    print("Seeded configs: fernverkehr (static) + germany-free (realtime)")


async def apply_clickhouse_schema(base_url: str = CLICKHOUSE_URL, *, database: str = "flechtwerk",
                                  user: str = "default", password: str = "") -> None:
    """Apply ``clickhouse.sql`` over the HTTP interface (reused by the integration test)."""
    raw = (Path(__file__).parent / "clickhouse.sql").read_text()
    body = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
    statements = [statement.strip() for statement in body.split(";") if statement.strip()]
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0,
                                 params={"user": user, "password": password, "database": database}) as client:
        for statement in statements:
            (await client.post("/", content=statement)).raise_for_status()
    print(f"Applied {len(statements)} ClickHouse statements")


async def main() -> None:
    await create_topics()
    await seed_configs()
    await apply_clickhouse_schema()
    print('GTFS delay-monitor setup complete — run "uv run poe trains" (setup + all three stages)')


if __name__ == "__main__":
    asyncio.run(main())

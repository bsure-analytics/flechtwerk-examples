"""One-shot setup for the GDELT news-stories pipeline — idempotent, safe to re-run.

    uv run poe setup-gdelt

Creates the config topics and the pipeline topics, seeds the feed configs (which feeds
ingest polls) and the bundled outlet table, and applies the ClickHouse schema. Nothing about
the *pipeline data* is seeded — the feeds are the live GDELT firehose; only the static outlet
lookup (``outlets.csv``) is seeded onto the ``gdelt-outlets`` config topic here, since it
needs no polling stage. Each stage existence-checks its topics at startup and fails fast if
any is missing, so this is the ops step the framework assumes is done up front.

``INCLUDE_TRANSLATION`` (default on) decides whether the machine-translated non-English feed
is polled too — European coverage is a stated motivation; it roughly doubles volume, still
small. Turn it off by seeding only ``english`` (edit here, or delete the translation config
record from ``gdelt-feeds`` with any Kafka tool).
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from examples._setup import quiet_fresh_topic_produce_race

from .coverage import COVERAGE_TOPIC
from .ingest import EVENTS_RAW_TOPIC, FEEDS_CONFIG_TOPIC, GKG_RAW_TOPIC, MENTIONS_RAW_TOPIC
from .outlets import OUTLETS_CSV, OUTLETS_TOPIC, outlet_messages
from .stories import STORIES_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

INCLUDE_TRANSLATION = False
"""Poll the machine-translated feed alongside the English one.

Default **off**. Turning it on roughly doubles per-slice volume and adds machine-translated
GKG entities that are markedly noisier (junk "entities" like "a court"), which both strains
the single-bucket clustering state (see ``stories.MAX_CLUSTERS``) and muddies clusters. The
English feed alone is the clean, in-comfort-zone default; flip this to poll both (and shard
the clustering by a blocking key if you do — see the README's single-partition note)."""


async def create_topics() -> None:
    """Create the compacted config topics and the partitioned pipeline topics.

    events-raw and mentions-raw are co-partitioned (same partition count, both keyed by
    GlobalEventID) so the join sees an event and its mentions on one task; gkg-raw is
    single-partition so all articles reach one clustering task (see ``stories.py``).
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (FEEDS_CONFIG_TOPIC, 8, {"cleanup.policy": "compact"}),   # feeds to poll (seeded here)
            (OUTLETS_TOPIC, 8, {"cleanup.policy": "compact"}),        # outlet table (seeded here)
            (EVENTS_RAW_TOPIC, 8, {}),
            (MENTIONS_RAW_TOPIC, 8, {}),                             # co-partitioned with events-raw
            (GKG_RAW_TOPIC, 1, {}),                                 # single-partition clustering input
            (COVERAGE_TOPIC, 8, {}),
            (STORIES_TOPIC, 8, {}),
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
    """Seed the feeds to poll and the bundled outlet table (keyed on their compacted topics).

    The outlet table is static bundled data, so it is produced straight to ``gdelt-outlets``
    here (reusing :func:`.outlets.outlet_messages`) rather than via a polling stage; keyed by
    domain on a compacted topic, so re-running setup idempotently overwrites each entry.
    """
    feeds = ["english"] + (["translation"] if INCLUDE_TRANSLATION else [])
    outlets = list(outlet_messages(OUTLETS_CSV.read_text()))
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for feed in feeds:
                await producer.send_and_wait(FEEDS_CONFIG_TOPIC, key=feed.encode(),
                                             value=json.dumps({"feed": feed}).encode())
            for message in outlets:
                await producer.send_and_wait(OUTLETS_TOPIC, key=message.key.encode(),
                                             value=json.dumps(message.value.raw).encode())
    finally:
        await producer.stop()
    print(f"Seeded feeds {feeds} and {len(outlets)} outlets")


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
    print('GDELT setup complete — run "uv run poe run-gdelt" (or the whole thing: "uv run poe gdelt")')


if __name__ == "__main__":
    asyncio.run(main())

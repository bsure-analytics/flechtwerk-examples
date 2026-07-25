"""One-shot setup for the Wildfire Watch pipeline — idempotent, safe to re-run.

    uv run poe setup-wildfire

Creates the compacted config topic and the three data topics, and applies the ClickHouse schema.
**Nothing is seeded** — a watch region is a human's choice of where to look (the ADS-B /
Odds pattern), and a default would just burn NASA's quota on a place you don't care about.
Request regions after setup with ``uv run poe request-wildfire`` (or any producer to
``wildfire-regions``, Kafbat UI included).

All four topics share a partition count so a region's config, detections, sweeps, status
snapshots, and events co-partition by the region slug — one tracker task owns a region's whole
fire lifecycle, and same-key serial processing keeps each poll's detections-before-sweep order
intact end to end. The config topic is log-compacted so the latest record per region (or a
tombstone) wins.

Running the ingest stage needs a free NASA ``FIRMS_MAP_KEY``; this setup step does not.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .attributes import DETECTIONS_TOPIC, EVENTS_TOPIC, REGIONS_TOPIC, STATUS_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

PARTITIONS = 8


async def create_topics() -> None:
    """Create the compacted config topic and the three partitioned data topics.

    ``wildfire-detections``, ``wildfire-status``, and ``wildfire-events`` share the config topic's
    partition count and are all keyed by the **region slug**, so everything about one region
    lands on one tracker task. The region is the join key here — not time, and not the fire:
    fire identity is *discovered* by the tracker, so it cannot be a partition key."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (REGIONS_TOPIC, PARTITIONS, {"cleanup.policy": "compact"}),  # watch regions (requested, not seeded)
            (DETECTIONS_TOPIC, PARTITIONS, {}),                          # hotspot pixels + sweep markers
            (STATUS_TOPIC, PARTITIONS, {}),                              # per-fire status heartbeats
            (EVENTS_TOPIC, PARTITIONS, {}),                              # ignition / merged / extinguished
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
    await apply_clickhouse_schema()
    print("Wildfire Watch setup complete.")
    print()
    print("1. Get a free, instant NASA MAP_KEY (needed by the ingest stage only):")
    print("     https://firms.modaps.eosdis.nasa.gov/api/map_key/")
    print("     export FIRMS_MAP_KEY=...")
    print("2. Request a region to watch, e.g.:")
    print('     uv run poe request-wildfire "Alentejo, Portugal"')
    print('     uv run poe request-wildfire "Attica, Greece"')
    print('3. Then "uv run poe wildfire" (setup + both stages).')


if __name__ == "__main__":
    asyncio.run(main())

"""One-shot setup for the ADS-B flight tracker — idempotent, safe to re-run.

    uv run poe setup-adsb

Creates the config topic and the four pipeline topics, seeds one region to poll,
and applies the ClickHouse schema (``clickhouse.sql``). This is the ops step the
framework assumes is done up front: each stage existence-checks its topics at
startup and fails fast if any is missing. Configuration is injected here, not read
from the environment — edit ``REGIONS`` (or write more records to ``adsb.regions``
with any producer, Kafbat UI included) to poll elsewhere.
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from examples._setup import quiet_fresh_topic_produce_race

from .enrich import AIRCRAFT_TOPIC, CELLS_TOPIC, EVENTS_TOPIC
from .ingest import CONFIG_TOPIC, RAW_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

REGIONS = [
    # Londong is reliably busy. A region may give lat/lon (and optionally radius) explicitly, or carry just a name and
    # let ingest forward-geocode it — see ingest.enrich_config.
    # radius defaults to 100 nm (clamped to adsb.lol's max) when omitted.
    {"name": "London"},
]


async def create_topics() -> None:
    """Create the compacted config topic and the four pipeline topics if absent."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        new = [
            NewTopic(name, num_partitions=8, replication_factor=1, topic_configs=configs)
            for name, configs in (
                (CONFIG_TOPIC, {"cleanup.policy": "compact"}),
                (RAW_TOPIC, {}),
                (AIRCRAFT_TOPIC, {}),
                (CELLS_TOPIC, {}),
                (EVENTS_TOPIC, {}),
            )
            if name not in existing
        ]
        if new:
            await admin.create_topics(new)
            print(f"Created topics: {[t.name for t in new]}")
        else:
            print("Topics already present")
    finally:
        await admin.close()


async def seed_regions() -> None:
    """Write one config record per region (wire key = region name)."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for region in REGIONS:
                await producer.send_and_wait(
                    CONFIG_TOPIC,
                    key=region["name"].encode(),
                    value=json.dumps(region).encode(),
                )
        print(f"Seeded regions: {[r['name'] for r in REGIONS]}")
    finally:
        await producer.stop()


async def apply_clickhouse_schema() -> None:
    """Apply ``clickhouse.sql`` over the HTTP interface, one statement at a time."""
    raw = (Path(__file__).parent / "clickhouse.sql").read_text()
    body = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
    statements = [statement.strip() for statement in body.split(";") if statement.strip()]
    async with httpx.AsyncClient(base_url=CLICKHOUSE_URL, timeout=30.0) as client:
        for statement in statements:
            (await client.post("/", content=statement)).raise_for_status()
    print(f"Applied {len(statements)} ClickHouse statements")


async def main() -> None:
    await create_topics()
    await seed_regions()
    await apply_clickhouse_schema()
    print("ADS-B setup complete — run: uv run poe run-adsb (then run-adsb-enrich, run-adsb-conflict)")


if __name__ == "__main__":
    asyncio.run(main())

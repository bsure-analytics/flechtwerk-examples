"""One-shot setup for the ADS-B flight tracker — idempotent, safe to re-run.

    uv run poe setup-adsb

Creates the config topics and the pipeline topics and applies the ClickHouse schema
(``clickhouse.sql``, including the two reverse-geocoding polygon dictionaries). This is
the ops step the framework assumes is done up front: each stage existence-checks its
topics at startup and fails fast if any is missing.

**Nothing is seeded.** There is no hard-coded region — after setup, request the region
you care about with the CLI:

    uv run poe request-region "London"

which writes one config record to ``adsb-regions`` (see ``request.py``). The boundary
loader needs no seeding: it downloads the world map at startup and each country's fine
map on demand as enrich detects traffic (see ``boundaries.py``).
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .boundaries import COUNTRIES_TOPIC
from .enrich import AIRCRAFT_TOPIC, CELLS_TOPIC, EVENTS_TOPIC
from .ingest import CONFIG_TOPIC, RAW_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"


async def create_topics() -> None:
    """Create the compacted config topics (regions + countries) and the pipeline topics."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        new = [
            NewTopic(name, num_partitions=8, replication_factor=1, topic_configs=configs)
            for name, configs in (
                (CONFIG_TOPIC, {"cleanup.policy": "compact"}),     # regions to poll (user-requested)
                (COUNTRIES_TOPIC, {"cleanup.policy": "compact"}),  # countries to map (enrich-requested)
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
    await apply_clickhouse_schema()
    print('ADS-B setup complete — run "uv run poe run-adsb", then request a region: '
          'uv run poe request-region "London"')


if __name__ == "__main__":
    asyncio.run(main())

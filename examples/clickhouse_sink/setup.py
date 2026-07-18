"""One-shot setup for the ClickHouse sink — idempotent, safe to re-run.

    uv run poe setup-sink

Ensures the input topic exists and applies the dedup-enabled positions table.
The sink consumes `adsb.aircraft`, so run example 1's pipeline (`run-adsb`) to
feed it — or write to `adsb.aircraft` with any producer.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .sink import CLICKHOUSE_URL, DATABASE, INPUT_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"


async def ensure_input_topic() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        if INPUT_TOPIC not in set(await admin.list_topics()):
            await admin.create_topics([NewTopic(INPUT_TOPIC, num_partitions=8, replication_factor=1)])
            print(f"Created input topic: {INPUT_TOPIC}")
        else:
            print(f"Input topic already present: {INPUT_TOPIC}")
    finally:
        await admin.close()


async def apply_schema(base_url: str = CLICKHOUSE_URL, *, database: str = DATABASE,
                       user: str = "default", password: str = "") -> None:
    """Apply ``clickhouse.sql`` over the HTTP interface (reused by the integration test)."""
    raw = (Path(__file__).parent / "clickhouse.sql").read_text()
    body = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
    statements = [statement.strip() for statement in body.split(";") if statement.strip()]
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0,
                                 params={"user": user, "password": password, "database": database}) as client:
        for statement in statements:
            (await client.post("/", content=statement)).raise_for_status()
    print(f"Applied {len(statements)} ClickHouse statement(s)")


async def main() -> None:
    await ensure_input_topic()
    await apply_schema()
    print("Sink setup complete — run: uv run poe run-sink (with run-adsb producing adsb.aircraft)")


if __name__ == "__main__":
    asyncio.run(main())

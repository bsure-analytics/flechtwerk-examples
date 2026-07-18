"""One-shot setup for the fermentation monitor.

    uv run poe setup-fermentation

Creates the config/reading/alert topics, seeds one config record per batch (the
MQTT topic filter each subscribes to), and applies the ClickHouse schema.
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from examples._setup import quiet_fresh_topic_produce_race

from .bridge import CONFIG_TOPIC, READINGS_TOPIC
from .monitor import ALERT_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

BATCHES = {"batch-42": "normal", "batch-43": "stall"}
"""Batch name → simulator behaviour (the names are the config source of truth)."""


async def create_topics(bootstrap: str = BOOTSTRAP_SERVERS) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        new = []
        if CONFIG_TOPIC not in existing:
            new.append(NewTopic(CONFIG_TOPIC, num_partitions=8, replication_factor=1,
                                topic_configs={"cleanup.policy": "compact"}))
        new += [NewTopic(t, num_partitions=8, replication_factor=1)
                for t in (READINGS_TOPIC, ALERT_TOPIC) if t not in existing]
        if new:
            await admin.create_topics(new)
    finally:
        await admin.close()


async def seed_batches(bootstrap: str = BOOTSTRAP_SERVERS) -> None:
    """One config record per batch: its MQTT topic filter + name (wire key = name)."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for name in BATCHES:
                config = {"topic": f"ispindel/{name}", "name": name}
                await producer.send_and_wait(CONFIG_TOPIC, key=name.encode(), value=json.dumps(config).encode())
    finally:
        await producer.stop()


async def apply_schema(base_url: str = CLICKHOUSE_URL) -> None:
    raw = (Path(__file__).parent / "clickhouse.sql").read_text()
    body = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
    statements = [s.strip() for s in body.split(";") if s.strip()]
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        for statement in statements:
            (await client.post("/", content=statement)).raise_for_status()


async def main() -> None:
    await create_topics()
    await seed_batches()
    await apply_schema()
    print(f"Seeded batches {list(BATCHES)} — run: uv run poe run-fermentation "
          "(and run-fermentation-monitor), then uv run poe simulate-fermentation")


if __name__ == "__main__":
    asyncio.run(main())

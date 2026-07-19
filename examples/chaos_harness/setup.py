"""One-shot setup for the chaos harness.

    uv run poe setup-chaos

Creates the input/output topics, produces a fixed run of input numbers, and
applies the ClickHouse sink for the verifier's query. Re-running re-produces the
input (so the harness has fresh work).
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from examples._setup import quiet_fresh_topic_produce_race

from .transformer import INPUT_TOPIC, OUTPUT_TOPIC, STATE_KEY

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"
RECORD_COUNT = 100_000
"""Enough work that the transformer is always mid-run when the harness kills it."""


async def create_topics(bootstrap: str = BOOTSTRAP_SERVERS) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        # Deliberately 1 partition, overriding the stack's 8-partition default:
        # every record shares one key to drive a single global counter, so a
        # global 1..N `seq` is only well-defined on one serial partition. (The
        # other examples key by hex/batch and take the 8-partition default.)
        new = [NewTopic(t, num_partitions=1, replication_factor=1)
               for t in (INPUT_TOPIC, OUTPUT_TOPIC) if t not in existing]
        if new:
            await admin.create_topics(new)
    finally:
        await admin.close()


async def reset(bootstrap: str = BOOTSTRAP_SERVERS) -> None:
    """Wipe the data topics and every prior run's state changelog.

    Each ``run-chaos`` uses a fresh ``application_id``, so its consumer group has
    no stale offsets and its changelog starts empty — the counter from 1. This
    just clears accumulated input/output and old changelog topics.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        stale = ({INPUT_TOPIC, OUTPUT_TOPIC}
                 | {t for t in existing if t.startswith("chaos-harness") and t.endswith("-changelog")})
        stale &= existing
        if stale:
            await admin.delete_topics(list(stale))
            await asyncio.sleep(2.0)  # let the deletes propagate before recreating
    finally:
        await admin.close()
    await create_topics(bootstrap)
    # Drop the ClickHouse tables so apply_schema rebinds the Kafka engine to the
    # freshly recreated chaos-output (dependents first).
    async with httpx.AsyncClient(base_url=CLICKHOUSE_URL, timeout=30.0) as client:
        for table in ("chaos_output_mv", "chaos_output_queue", "chaos_output"):
            (await client.post("/", content=f"DROP TABLE IF EXISTS flechtwerk.{table}")).raise_for_status()


async def produce_input(bootstrap: str = BOOTSTRAP_SERVERS, count: int = RECORD_COUNT) -> None:
    """Write ``count`` records — n = 0..count-1 — all under one key (one bucket)."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        key = STATE_KEY.encode()
        with quiet_fresh_topic_produce_race():
            for n in range(count):
                await producer.send(INPUT_TOPIC, key=key, value=json.dumps({"n": n}).encode())
            await producer.flush()
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
    await reset()  # clean slate: fresh topics, empty state, dropped ClickHouse tables
    await apply_schema()  # recreate the tables, rebinding the Kafka engine to fresh chaos-output
    await produce_input()
    print(f"Produced {RECORD_COUNT} input records — run: uv run poe run-chaos, then uv run poe verify-chaos")


if __name__ == "__main__":
    asyncio.run(main())

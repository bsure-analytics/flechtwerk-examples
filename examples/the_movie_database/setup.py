"""One-shot setup for the TMDB - The Movie Database pipeline — idempotent, safe to re-run.

    uv run poe setup-tmdb

Creates the compacted feed-config topic and the six data topics, and applies the ClickHouse
schema. **Nothing is seeded** — a feed record carries the access token, which is a secret that
only ``request-tmdb token`` may write (encrypted). Set one up after this step.

All seven topics share a partition count and every record is keyed ``<feed>:<id>`` (the config
record and the raw pages by the bare feed name), so an entity's change records, its versions and its credits
co-partition onto the one catalog task that owns its state.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .attributes import (
    CHANGES_RAW_TOPIC,
    CHANGES_TOPIC,
    CREDITS_TOPIC,
    FEEDS_TOPIC,
    MOVIES_TOPIC,
    PEOPLE_TOPIC,
    TV_TOPIC,
)

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

PARTITIONS = 8


async def create_topics() -> None:
    """Create the compacted feed-config topic and the six partitioned data topics."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (FEEDS_TOPIC, PARTITIONS, {"cleanup.policy": "compact"}),  # feeds + encrypted token
            (CHANGES_RAW_TOPIC, PARTITIONS, {}),                       # change-feed pages as delivered
            (CHANGES_TOPIC, PARTITIONS, {}),                           # "<feed>:<id> changed on <day>"
            (MOVIES_TOPIC, PARTITIONS, {}),                            # one row per movie version
            (TV_TOPIC, PARTITIONS, {}),                                # one row per TV-series version
            (PEOPLE_TOPIC, PARTITIONS, {}),                            # one row per person version
            (CREDITS_TOPIC, PARTITIONS, {}),                           # one row per credit of a title version
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
    print("TMDB - The Movie Database setup complete.")
    print()
    print("1. Make a keyring (once; it encrypts the TMDB token on the config topic):")
    print("     uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) "
          "~/.config/flechtwerk-examples/keyring.json")
    print("     chmod 600 ~/.config/flechtwerk-examples/keyring.json")
    print("2. Store your TMDB API Read Access Token (https://www.themoviedb.org/settings/api):")
    print("     uv run poe request-tmdb token < ~/.config/flechtwerk-examples/tmdb-token")
    print("3. Optionally pick an epoch (default: the last 90 days), e.g.:")
    print("     uv run poe request-tmdb since 2026-07-25 [movie|tv|person]")
    print('4. Then "uv run poe tmdb" (setup + all three stages).')


if __name__ == "__main__":
    asyncio.run(main())

"""One-shot setup for the Odds Arbitrage Radar pipeline — idempotent, safe to re-run.

    uv run poe setup-odds

Creates the compacted config topic and the three data topics, and applies the ClickHouse
schema. **Nothing is seeded** — a pair is a user's curated claim that two markets resolve the
same event, so there is no sensible default basket (the ADS-B pattern). Request pairs after
setup with ``uv run poe request-pair`` (or any producer to ``odds-pairs``, Kafbat included).

All four topics have the same partition count so a pair's config, its two venues' quotes, and
its margins co-partition by the pair key. The config topic is log-compacted so the latest
record per pair (or a tombstone) wins.

Read-only public data only: this pipeline observes Polymarket and Kalshi prices; it never
places an order. See the README's responsible-use note.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .attributes import MARGINS_TOPIC, PAIRS_TOPIC, QUOTES_TOPIC, SIGNALS_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

PARTITIONS = 8


async def create_topics() -> None:
    """Create the compacted config topic and the three partitioned data topics.

    ``odds-quotes``, ``odds-margins``, and ``odds-signals`` share the config topic's partition
    count and are all keyed by the pair, so a pair's quotes (from both venues), margins, and
    signals co-partition onto one radar task. Time is not the join key here — the pair is."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (PAIRS_TOPIC, PARTITIONS, {"cleanup.policy": "compact"}),  # curated pairs (requested, not seeded)
            (QUOTES_TOPIC, PARTITIONS, {}),                            # both venues' quotes
            (MARGINS_TOPIC, PARTITIONS, {}),                           # continuous derived margins
            (SIGNALS_TOPIC, PARTITIONS, {}),                           # sparse fresh net-positive arbs
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
    print('Odds Arbitrage Radar setup complete — request a pair, e.g.:')
    print('  uv run poe request-pair mlb-col-mil-2026-07-24 '
          'KXMLBGAME-26JUL261410COLMIL-MIL "Milwaukee Brewers"')
    print('then "uv run poe odds" (setup + all three stages).')


if __name__ == "__main__":
    asyncio.run(main())

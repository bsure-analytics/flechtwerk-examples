"""One-shot setup for the Glucose Monitor pipeline — idempotent, safe to re-run.

    uv run poe setup-glucose

Creates the compacted account-config topic and the two data topics, and applies the ClickHouse
schema. **Nothing is seeded** — an account record carries LibreLinkUp credentials, which only
``request-glucose account`` may write (encrypted). Set one up after this step.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .attributes import ACCOUNTS_TOPIC, RAW_TOPIC, READINGS_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

PARTITIONS = 8


async def create_topics() -> None:
    """Create the compacted account-config topic and the two partitioned data topics."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (ACCOUNTS_TOPIC, PARTITIONS, {"cleanup.policy": "compact"}),  # accounts + encrypted credentials
            (RAW_TOPIC, PARTITIONS, {}),                                   # graph responses as delivered
            (READINGS_TOPIC, PARTITIONS, {}),                              # one record per new reading
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
    print("Glucose Monitor setup complete.")
    print()
    print("1. In the FreeStyle Libre 3 app, invite a LibreLinkUp follower (a second email of your")
    print("   own works), install LibreLinkUp, sign in as that follower once and accept.")
    print("2. Make a keyring (once; it encrypts the credentials on the config topic):")
    print("     uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) "
          "~/.config/flechtwerk-examples/keyring.json")
    print("     chmod 600 ~/.config/flechtwerk-examples/keyring.json")
    print("3. Store the follower account (it asks for the email and password, signs in once to check):")
    print("     uv run poe request-glucose account me")
    print('4. Then "uv run poe glucose" (setup + both stages).')


if __name__ == "__main__":
    asyncio.run(main())

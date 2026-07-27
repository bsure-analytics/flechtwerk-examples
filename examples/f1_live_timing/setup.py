"""One-shot setup for the F1 Live Timing pipeline — idempotent, safe to re-run.

    uv run poe setup-f1

Creates the compacted config topic and the four data topics, and applies the ClickHouse
schema. **Nothing is seeded** — which sessions to ingest is a human's choice (the ADS-B /
wildfire pattern), and a default would spend somebody else's bandwidth on a race you don't
care about. Request sessions after setup with ``uv run poe request-f1`` (or any producer to
``f1-sessions``, Kafbat UI included).

All five topics share a partition count so that everything about one session co-partitions by
its **path**: its config record, its tape, its leaderboard snapshots, its events, and its
telemetry. One board task therefore owns a session's whole stream, and same-key serial
processing preserves the tape order the watermark merge worked to establish. The config topic
is log-compacted, so the latest record per session (or a tombstone) wins.

**The four data topics get ``retention.ms=-1``, and that is correctness, not hoarding.**
Records carry event-time ``Message.timestamp``s that are *genuinely months in the past* during a
backfill — a March race ingested in July is timestamped March. Kafka's time-based retention
judges a segment by its **maximum record timestamp**, so under the default seven days a freshly
backfilled March race would be eligible for deletion the instant it landed: the topic would
accept the data, report success, and quietly drop it at the next log-retention pass. Unlimited
retention is the only setting compatible with honest event time. It is also what makes the
season durable against the endpoint being withdrawn (the archive already 403s 2022) — an
ingested season stays ingested.

No credentials of any kind are needed, for setup or for running: the archive is public and
this example only ever reads it.
"""
import asyncio
from pathlib import Path

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from .attributes import EVENTS_TOPIC, SESSIONS_TOPIC, STATUS_TOPIC, TELEMETRY_TOPIC, TIMING_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

PARTITIONS = 8

FOREVER = "-1"
"""``retention.ms`` for every data topic — see the module docstring. Kafka's sentinel for
"never delete by time"."""


async def create_topics() -> None:
    """Create the compacted config topic and the four partitioned data topics.

    ``f1-timing``, ``f1-status``, ``f1-events``, and ``f1-telemetry`` share the config topic's
    partition count and are all keyed by the **session path**, so a session's tape and every
    projection of it land on one board task. The session is the join key here — not the driver
    and not time: a leaderboard is only meaningful within one session, and the numeric session
    key is not known until the tape's first line has been read.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (SESSIONS_TOPIC, {"cleanup.policy": "compact"}),   # sessions to ingest (requested, not seeded)
            (TIMING_TOPIC, {"retention.ms": FOREVER}),         # the tape
            (STATUS_TOPIC, {"retention.ms": FOREVER}),         # standings / weather / clock / heartbeat
            (EVENTS_TOPIC, {"retention.ms": FOREVER}),         # laps / pits / flags / race control / dims
            (TELEMETRY_TOPIC, {"retention.ms": FOREVER}),      # car + position samples
        ]
        new = [NewTopic(name, num_partitions=PARTITIONS, replication_factor=1,
                        topic_configs=configs)
               for name, configs in specs if name not in existing]
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
    print("F1 Live Timing setup complete.")
    print()
    print("1. Ask for some tape (no credentials needed — the archive is public):")
    print("     uv run poe request-f1 season 2026            # every competitive session so far")
    print("     uv run poe request-f1 follow 2026            # ... and pick up new ones unattended")
    print("     uv run poe request-f1 session 2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/")
    print('2. Then "uv run poe f1" (setup + both stages).')
    print("3. Open Grafana -> Flechtwerk — F1 Season and hit '▶ replay' on any session.")


if __name__ == "__main__":
    asyncio.run(main())

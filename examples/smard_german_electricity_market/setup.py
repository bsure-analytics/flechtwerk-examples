"""One-shot setup for the SMARD electricity-market pipeline — idempotent, safe to re-run.

    uv run poe setup-smard

Creates the config topic and the two data topics, seeds one config record per series in
the default basket (the German generation sources + grid load + residual load + the
day-ahead price), and applies the ClickHouse schema. Nothing about the *pipeline data* is
seeded — the values are the live SMARD feed; only the config records that point ingest at
each series are written here. Each config record is keyed ``"{filter}_{region}_{resolution}"``
on the compacted config topic, so any producer (Kafbat included) can add a series or edit
one live, and re-running setup idempotently overwrites each entry.

The basket is **generation + load + price for region DE** (the day-ahead price is region
``DE-LU``, the German-Luxembourg bidding zone). **Grid load (410) is the settle marker** —
it always has fresh data, so its intervals age out reliably and finalize the mix. Nuclear
(filter 1224) is deliberately absent: it ended in 2024, so its index is frozen and it
would only ever backfill one dead week — add it (and any other filter) as a config record
to see the dead-series no-op, the README's extension point.

Data © Bundesnetzagentur | SMARD.de, CC BY 4.0.
"""
import asyncio
import json
from pathlib import Path

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Event

from examples._setup import quiet_fresh_topic_produce_race

from .attributes import (
    FILTER,
    REGION,
    RESOLUTION,
    ROLE,
    SERIES_NAME,
    SETTLE_MARKER,
    SOURCE,
    UNIT,
)
from .ingest import OBSERVATIONS_TOPIC, SERIES_CONFIG_TOPIC
from .mix import MIX_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"

_MWH = "MWh"
_EUR = "EUR/MWh"

# The default basket: (filter, region, name, role, source, unit). resolution is
# quarterhour throughout; settle_marker is set on grid load alone (see below). Source ids
# are the canonical keys in mix.SOURCE_META. Filters verified live against SMARD.de.
SERIES: list[tuple[int, str, str, str, str | None, str]] = [
    (410,  "DE",    "Grid load",           "load",          None,                 _MWH),
    (4359, "DE",    "Residual load",       "residual_load", None,                 _MWH),
    (4169, "DE-LU", "Day-ahead price",     "price",         None,                 _EUR),
    (1223, "DE",    "Lignite",             "source",        "lignite",            _MWH),
    (4069, "DE",    "Hard coal",           "source",        "hard_coal",          _MWH),
    (4071, "DE",    "Natural gas",         "source",        "gas",                _MWH),
    (1227, "DE",    "Other conventional",  "source",        "other_conventional", _MWH),
    (4070, "DE",    "Pumped storage",      "source",        "pumped_storage",     _MWH),
    (4066, "DE",    "Biomass",             "source",        "biomass",            _MWH),
    (1226, "DE",    "Hydro",               "source",        "hydro",              _MWH),
    (4067, "DE",    "Wind onshore",        "source",        "wind_onshore",       _MWH),
    (1225, "DE",    "Wind offshore",       "source",        "wind_offshore",      _MWH),
    (4068, "DE",    "Photovoltaics",       "source",        "solar",              _MWH),
    (1228, "DE",    "Other renewables",    "source",        "other_renewable",    _MWH),
]

_MARKER_FILTER = 410
"""Grid load drives settlement — it always has fresh data, so its intervals age out of the
revision window on schedule and their settle markers finalize the whole interval's mix."""

RESOLUTION_VALUE = "quarterhour"


def _config_records() -> list[tuple[str, dict]]:
    """Build ``(wire_key, raw_config)`` for every series — typed, so encoding is validated."""
    records = []
    for filter_id, region, name, role, source, unit in SERIES:
        config = Event({
            FILTER: filter_id, REGION: region, RESOLUTION: RESOLUTION_VALUE,
            SERIES_NAME: name, ROLE: role, UNIT: unit,
        })
        if source is not None:
            config[SOURCE] = source
        if filter_id == _MARKER_FILTER:
            config[SETTLE_MARKER] = True
        records.append((f"{filter_id}_{region}_{RESOLUTION_VALUE}", config.raw))
    return records


async def create_topics() -> None:
    """Create the compacted config topic and the two partitioned data topics.

    ``smard-observations`` and ``smard-mix`` are both keyed by the interval instant with
    the same partition count, so every series' observation for one quarter-hour — and the
    mix record derived from it — co-partition onto one task. Time is the join key."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        specs = [
            (SERIES_CONFIG_TOPIC, 8, {"cleanup.policy": "compact"}),  # series to poll (seeded here)
            (OBSERVATIONS_TOPIC, 8, {}),                              # observations + settled markers
            (MIX_TOPIC, 8, {}),                                       # keyed by interval, like observations
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


async def seed_configs() -> None:
    """Seed one config record per series, keyed on the compacted config topic (idempotent)."""
    records = _config_records()
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for key, raw in records:
                await producer.send_and_wait(SERIES_CONFIG_TOPIC, key=key.encode(),
                                             value=json.dumps(raw).encode())
    finally:
        await producer.stop()
    print(f"Seeded {len(records)} series configs (settle marker: {_MARKER_FILTER})")


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
    await seed_configs()
    await apply_clickhouse_schema()
    print('SMARD setup complete — run "uv run poe smard" (setup + both stages)')


if __name__ == "__main__":
    asyncio.run(main())

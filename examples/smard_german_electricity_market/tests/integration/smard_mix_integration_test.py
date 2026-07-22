"""Tier 3 — integration. The cross-series join over a real broker, and the ClickHouse
schema applied against a real server.

- ``test_mix_join_over_the_broker`` produces two series' observations for one interval and
  then, a beat later, that interval's settled marker — all keyed by the interval instant —
  and runs the real ``mix`` transformer. It proves the join end to end: the observations
  land on the same partition/task (real key-hash co-partitioning), accumulate into
  preliminary mix records, and the marker emits the ``is_final`` record read back under
  ``read_committed``.
- ``test_clickhouse_schema_applies`` applies ``clickhouse.sql`` to a real ClickHouse and
  asserts the two queues, the three target tables, and the three materialized views are
  created — catching any DDL error the Docker-free tiers can't.
"""
import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Event
from flechtwerk.module import Flechtwerk

from examples.smard_german_electricity_market.attributes import (
    FETCHED_AT,
    INTERVAL_TS,
    KIND,
    REVISED,
    ROLE,
    SERIES_KEY,
    SERIES_NAME,
    SOURCE,
    UNIT,
    VALUE,
)
from examples.smard_german_electricity_market.ingest import OBSERVATIONS_TOPIC, interval_key
from examples.smard_german_electricity_market.mix import MIX_TOPIC, mix
from examples.smard_german_electricity_market.setup import apply_clickhouse_schema

pytestmark = pytest.mark.integration

UTC = timezone.utc
_INTERVAL = datetime(2026, 7, 23, 10, tzinfo=UTC)
_FETCHED = datetime(2026, 7, 23, 10, 5, tzinfo=UTC)


def _obs(series_key: str, role: str, value: float, *, source: str | None = None) -> bytes:
    record = Event({KIND: "observation", SERIES_KEY: series_key, SERIES_NAME: series_key,
                    ROLE: role, UNIT: "MWh", INTERVAL_TS: _INTERVAL, VALUE: value,
                    REVISED: False, FETCHED_AT: _FETCHED})
    if source is not None:
        record[SOURCE] = source
    return json.dumps(record.raw).encode()


def _settled(series_key: str) -> bytes:
    return json.dumps(Event({KIND: "settled", SERIES_KEY: series_key, INTERVAL_TS: _INTERVAL,
                             FETCHED_AT: _FETCHED + timedelta(hours=49)}).raw).encode()


async def test_mix_join_over_the_broker(kafka_bootstrap: str) -> None:
    key = interval_key(_INTERVAL).encode()

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        with suppress(Exception):  # idempotent — topics may already exist on the session broker
            await admin.create_topics([NewTopic(t, num_partitions=8, replication_factor=1)
                                       for t in (OBSERVATIONS_TOPIC, MIX_TOPIC)])
    finally:
        await admin.close()

    app = Flechtwerk.of(application_id="smard-mix-it", bootstrap_servers=kafka_bootstrap,
                        client_id="smard-mix-it-0", stage=mix)
    consumer = AIOKafkaConsumer(MIX_TOPIC, bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest",
                                group_id=None, isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        # Two same-interval observations accumulate the mix; a beat later, the settle
        # marker (having seen both) finalizes it.
        await producer.send_and_wait(OBSERVATIONS_TOPIC, key=key, value=_obs("solar", "source", 100.0, source="solar"))
        await producer.send_and_wait(OBSERVATIONS_TOPIC, key=key, value=_obs("gas", "source", 100.0, source="gas"))
        await asyncio.sleep(2.0)
        await producer.send_and_wait(OBSERVATIONS_TOPIC, key=key, value=_settled("load"))

        final: dict = {}
        deadline = asyncio.get_running_loop().time() + 60.0
        while not final.get("is_final"):
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never emitted a final mix record: {final}")
            for _tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for r in records:
                    final = json.loads(r.value)

        assert final["is_final"] is True
        assert final["total_generation_mwh"] == 200.0            # both sources folded in
        assert final["renewables_share"] == 0.5                  # solar renewable, gas not
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await producer.stop()
        await consumer.stop()


async def test_clickhouse_schema_applies(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    async with httpx.AsyncClient(base_url=clickhouse["base_url"], timeout=30.0, params={
        "user": clickhouse["user"], "password": clickhouse["password"], "database": clickhouse["database"],
    }) as client:
        response = await client.post("/", content="SELECT name FROM system.tables WHERE database = 'flechtwerk'")
        response.raise_for_status()
        tables = set(response.text.split())
    assert {"smard_observations_queue", "smard_observations", "smard_observations_mv",
            "smard_revisions", "smard_revisions_mv",
            "smard_mix_queue", "smard_mix", "smard_mix_mv"} <= tables

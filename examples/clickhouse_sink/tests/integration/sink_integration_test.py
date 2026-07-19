"""Tier 3 — integration. Ephemeral ClickHouse via testcontainers.

The idempotency claim, proven against a real ClickHouse: run the sink over a
batch, then over the SAME batch again (what at-least-once reprocessing does),
and assert the row count is unchanged — the `insert_deduplication_token` drops
every re-insert. No Kafka needed; the claim is about the ClickHouse write.
"""
import json

import httpx
import pytest

from flechtwerk.kafka import parse_message
from flechtwerk.testing import make_record
from flechtwerk.types import State

from examples.clickhouse_sink.setup import apply_schema
from examples.clickhouse_sink.sink import AdsbSink, HttpClickHouseWriter, INPUT_TOPIC

pytestmark = pytest.mark.integration


def _position_record(i: int):
    payload = {
        "hex": f"ac{i:04d}", "flight": f"TEST{i}", "alt_baro": 30000 + i,
        "gs": 400.0 + i, "lat": 51.5, "lon": -0.4, "requested_region": "london",
        "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 0,
    }
    return make_record(topic=INPUT_TOPIC, partition=0, offset=i, value=json.dumps(payload))


async def _count(clickhouse: dict[str, str]) -> int:
    async with httpx.AsyncClient(base_url=clickhouse["base_url"], params={
        "user": clickhouse["user"], "password": clickhouse["password"], "database": clickhouse["database"],
    }) as client:
        response = await client.post("/", content="SELECT count() FROM adsb_positions")
        response.raise_for_status()
        return int(response.text.strip())


async def test_reinserting_the_same_batch_is_idempotent(clickhouse: dict[str, str]) -> None:
    await apply_schema(clickhouse["base_url"], database=clickhouse["database"],
                       user=clickhouse["user"], password=clickhouse["password"])
    writer = HttpClickHouseWriter(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    sink = AdsbSink(writer=writer)
    records = [_position_record(i) for i in range(5)]

    try:
        for _ in range(2):  # process the same batch twice — at-least-once reprocessing
            for record in records:
                async for _ in sink.transform(parse_message(record), State()):
                    pass  # pragma: no cover — a pure sink yields nothing
    finally:
        await writer.aclose()

    # 5 distinct records inserted twice → deduped by token back to 5 rows.
    assert await _count(clickhouse) == 5

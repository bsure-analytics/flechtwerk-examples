"""Tier 3 — integration. The idempotent sink against a real ClickHouse.

Runs the sink over a batch of story + coverage records, then over the SAME batch again
(what at-least-once reprocessing does), and asserts the row counts are unchanged and the
latest state is queryable with FINAL — the insert_deduplication_token drops each re-insert
and the ReplacingMergeTree keeps one row per entity. No Kafka needed; the claim is about the
ClickHouse write.
"""
import json

import httpx
import pytest

from flechtwerk.kafka import parse_message
from flechtwerk.testing import make_record
from flechtwerk.types import State

from examples.gdelt_news_stories.coverage import COVERAGE_TOPIC
from examples.gdelt_news_stories.setup import apply_clickhouse_schema
from examples.gdelt_news_stories.sink import GdeltSink, HttpClickHouseWriter
from examples.gdelt_news_stories.stories import STORIES_TOPIC

pytestmark = pytest.mark.integration


def _story(i: int):
    value = {"story_id": f"s{i}", "article_count": 2 + i, "country_count": 2,
             "source_domains": ["bbc.co.uk", "lemonde.fr"], "countries": ["FR", "GB"],
             "top_entities": ["keir starmer"], "avg_tone": -1.5, "sample_url": f"http://x/{i}",
             "first_seen": "2026-07-21T08:30:00Z", "last_seen": "2026-07-21T08:30:00Z"}
    return make_record(topic=STORIES_TOPIC, partition=0, offset=i, value=json.dumps(value))


def _coverage(i: int):
    value = {"GlobalEventID": str(i), "event_seen": 1, "mention_count": 3 + i, "distinct_sources": 2,
             "EventRootCode": "14", "ActionGeo_Lat": "48.85", "ActionGeo_Long": "2.35", "AvgTone": "-2.0",
             "updated_at": "2026-07-21T08:30:00Z"}
    return make_record(topic=COVERAGE_TOPIC, partition=0, offset=i, value=json.dumps(value))


async def _count(clickhouse: dict[str, str], table: str, final: bool = False) -> int:
    async with httpx.AsyncClient(base_url=clickhouse["base_url"], params={
        "user": clickhouse["user"], "password": clickhouse["password"], "database": clickhouse["database"],
    }) as client:
        response = await client.post("/", content=f"SELECT count() FROM {table}{' FINAL' if final else ''}")
        response.raise_for_status()
        return int(response.text.strip())


async def test_reprocessing_the_same_batch_is_idempotent(clickhouse: dict[str, str]) -> None:
    await apply_clickhouse_schema(clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    writer = HttpClickHouseWriter(base_url=clickhouse["base_url"], database=clickhouse["database"],
                                  user=clickhouse["user"], password=clickhouse["password"])
    sink = GdeltSink(writer=writer)
    records = [_story(i) for i in range(5)] + [_coverage(i) for i in range(5)]

    try:
        for _ in range(2):  # process the same batch twice — at-least-once reprocessing
            for record in records:
                async for _ in sink.transform(parse_message(record), State()):
                    pass  # pragma: no cover — a pure sink yields nothing
    finally:
        await writer.aclose()

    # 5 distinct stories + 5 distinct coverage records inserted twice → deduped back to 5 each.
    assert await _count(clickhouse, "gdelt_stories", final=True) == 5
    assert await _count(clickhouse, "gdelt_event_coverage", final=True) == 5

"""ClickHouse sink — lands stories and coverage, idempotently (honest at-least-once).

Like the ``clickhouse_sink`` example, the teaching point is that the DB write is a side
effect **outside** the Kafka transaction: a transformer's transaction covers its output
messages, state, and offset commit, not an external HTTP insert. So a crash after the insert
but before the offset commit reprocesses the record and re-inserts it — **at-least-once**.
Two things make that converge to the same rows:

1. a stable ``insert_deduplication_token`` (``topic:partition:offset``) — ClickHouse drops a
   re-insert carrying a token it has recently seen (the tables set
   ``non_replicated_deduplication_window``); and
2. the target tables are ``ReplacingMergeTree`` keyed by the entity id with a version column
   (a story's ``last_seen``, a coverage record's ``updated_at``), so successive *updates* to
   the same entity replace rather than accumulate — query with ``FINAL`` for the live state.

One sink, two inputs: it consumes both ``gdelt-stories`` and ``gdelt-event-coverage`` and
routes each record to its table by topic. The record shape follows ADS-B's schemaless-ingest
idea: a curated set of typed columns is promoted, and the **whole message** rides in a
``payload JSON`` catch-all, so a field we don't promote today is still queryable as
``payload.<field>`` with no DDL change.

The projection (:func:`to_rows`) and token (:func:`dedup_token`) are pure, so the logic tier
drives them with no framework and no ClickHouse.
"""
import json
import logging
from collections.abc import AsyncIterator
from typing import Any, Protocol

import httpx
from flechtwerk import Event, IncomingMessage, Message, State, Transformer

from .coverage import COVERAGE_TOPIC
from .schema import (
    ACTION_GEO_COUNTRY,
    ACTION_GEO_FULLNAME,
    ACTION_GEO_LAT,
    ACTION_GEO_LONG,
    ARTICLE_COUNT,
    AVG_STORY_TONE,
    AVG_TONE,
    COUNTRIES,
    COUNTRY_COUNT,
    DISTINCT_SOURCES,
    EVENT_ROOT_CODE,
    EVENT_SEEN,
    FIRST_MENTION_AT,
    FIRST_SEEN,
    GLOBAL_EVENT_ID,
    LAST_MENTION_AT,
    LAST_SEEN,
    MENTION_COUNT,
    SAMPLE_TITLE,
    SOURCE_DOMAINS,
    SOURCE_URL,
    STORY_ID,
    TOP_ENTITIES,
    UPDATED_AT,
)
from .stories import STORIES_TOPIC

log = logging.getLogger(__name__)

CLICKHOUSE_URL = "http://localhost:8123"
DATABASE = "flechtwerk"
STORIES_TABLE = "gdelt_stories"
COVERAGE_TABLE = "gdelt_event_coverage"


def dedup_token(msg: IncomingMessage) -> str:
    """A token stable across reprocessing — the record's Kafka coordinates."""
    return f"{msg.topic}:{msg.partition}:{msg.offset}"


def _iso(value: Any) -> Any:
    """Render a datetime as ISO for JSONEachRow; pass other values through."""
    return value.isoformat() if hasattr(value, "isoformat") else value


def _float(value: Any) -> float | None:
    """Coerce a (possibly-empty, possibly-string) wire value to float, or ``None``."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _put(row: dict[str, Any], column: str, value: Any) -> None:
    """Set a column only when the value is present — absent ⇒ NULL/default, never a fake 0."""
    if value is not None:
        row[column] = value


def _story_row(value: Event) -> dict[str, Any]:
    """Project a ``gdelt-stories`` record into ``gdelt_stories`` columns + a payload catch-all."""
    row: dict[str, Any] = {
        "story_id": value[STORY_ID],
        "article_count": value.get(ARTICLE_COUNT) or 0,
        "country_count": value.get(COUNTRY_COUNT) or 0,
        "source_domains": value.get(SOURCE_DOMAINS) or [],
        "countries": value.get(COUNTRIES) or [],
        "top_entities": value.get(TOP_ENTITIES) or [],
        "first_seen": _iso(value[FIRST_SEEN]),
        "last_seen": _iso(value[LAST_SEEN]),
        "payload": value.raw,
    }
    _put(row, "avg_tone", value.get(AVG_STORY_TONE))
    _put(row, "sample_url", value.get(SAMPLE_TITLE))
    return row


def _coverage_row(value: Event) -> dict[str, Any]:
    """Project a ``gdelt-event-coverage`` record into ``gdelt_event_coverage`` columns.

    The event summary arrives under its GDELT wire names (the repo's carry-wire-names
    convention); this renames them into clean columns and coerces tone/lat/lon to numbers,
    the sink's job. Absent optional fields land as NULL, never a fabricated 0.
    """
    row: dict[str, Any] = {
        "global_event_id": value[GLOBAL_EVENT_ID],
        "event_seen": value.get(EVENT_SEEN) or 0,
        "mention_count": value.get(MENTION_COUNT) or 0,
        "distinct_sources": value.get(DISTINCT_SOURCES) or 0,
        "updated_at": _iso(value[UPDATED_AT]),
        "payload": value.raw,
    }
    _put(row, "event_root_code", value.get(EVENT_ROOT_CODE))
    _put(row, "action_geo_fullname", value.get(ACTION_GEO_FULLNAME))
    _put(row, "action_geo_country", value.get(ACTION_GEO_COUNTRY))
    _put(row, "action_lat", _float(value.get(ACTION_GEO_LAT)))
    _put(row, "action_lon", _float(value.get(ACTION_GEO_LONG)))
    _put(row, "avg_tone", _float(value.get(AVG_TONE)))
    _put(row, "source_url", value.get(SOURCE_URL))
    _put(row, "first_mention_at", _iso(value.get(FIRST_MENTION_AT)))
    _put(row, "last_mention_at", _iso(value.get(LAST_MENTION_AT)))
    return row


TABLE_BY_TOPIC = {STORIES_TOPIC: STORIES_TABLE, COVERAGE_TOPIC: COVERAGE_TABLE}


def to_rows(msg: IncomingMessage) -> tuple[str, list[dict[str, Any]]]:
    """Route one message to its ``(table, rows)`` by topic — pure and unit-testable."""
    if msg.topic == STORIES_TOPIC:
        return STORIES_TABLE, [_story_row(msg.value)]
    return COVERAGE_TABLE, [_coverage_row(msg.value)]


class ClickHouseWriter(Protocol):
    """The narrow sink surface ``GdeltSink`` needs — real over HTTP, fake in tests."""

    async def insert(self, table: str, rows: list[dict[str, Any]], *, dedup_token: str) -> None: ...


class HttpClickHouseWriter:
    """Inserts rows over ClickHouse's HTTP interface with an idempotency token."""

    def __init__(self, *, base_url: str = CLICKHOUSE_URL, database: str = DATABASE,
                 user: str = "default", password: str = "") -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=30.0,
            params={"user": user, "password": password, "database": database},
        )

    async def insert(self, table: str, rows: list[dict[str, Any]], *, dedup_token: str) -> None:
        body = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(row) for row in rows)
        response = await self._client.post(
            "/", content=body,
            params={"insert_deduplication_token": dedup_token, "date_time_input_format": "best_effort"},
        )
        response.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()


class GdeltSink(Transformer):
    """Sinks ``gdelt-stories`` and ``gdelt-event-coverage`` into ClickHouse, idempotently.

    A pure sink: it emits nothing to Kafka and keeps no state, so its task transaction only
    commits the input offset. The insert is the side effect outside that transaction — hence
    the dedup token + ReplacingMergeTree (see the module docstring).
    """

    input_topics = [STORIES_TOPIC, COVERAGE_TOPIC]

    def __init__(self, writer: ClickHouseWriter | None = None) -> None:
        super().__init__()
        self._writer = writer

    async def __aenter__(self) -> "GdeltSink":
        if self._writer is None:
            self._writer = HttpClickHouseWriter()  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if isinstance(self._writer, HttpClickHouseWriter):
            await self._writer.aclose()  # pragma: no cover — live path

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        assert self._writer is not None, "writer is created in __aenter__ or injected"
        table, rows = to_rows(msg)
        if rows:
            await self._writer.insert(table, rows, dedup_token=dedup_token(msg))
        return
        yield  # pragma: no cover — a pure sink emits nothing to Kafka


stage = GdeltSink()
"""The stage the dispatcher runs (``python -m examples.gdelt_news_stories sink``)."""

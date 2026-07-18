"""ClickHouse sink stage — the honest-semantics counterpart to example 1.

Example 1 sinks `adsb.aircraft` with ClickHouse's Kafka *engine* — the shortcut.
This is the pattern being taught instead: a Flechtwerk **transformer** whose work
is a ClickHouse insert. The teaching point is right here in `transform`:

    the DB write is a side effect OUTSIDE the Kafka transaction.

A transformer's transaction covers its output messages, state, and offset commit
— not an external HTTP insert. So if the process crashes after the insert but
before the offset commits, the record is reprocessed and the row is inserted
again: **at-least-once**. We make that harmless by giving each insert a stable
`insert_deduplication_token` (`topic:partition:offset`); ClickHouse drops a
re-insert carrying a token it has recently seen (the target table sets
`non_replicated_deduplication_window`, which turns the feature on for a plain
MergeTree). Reprocessing therefore converges to exactly the same rows.

The projection (`to_rows`) and the token (`dedup_token`) are pure functions, so
the logic tier drives them with no framework and no ClickHouse.
"""
import json
from collections.abc import AsyncIterator
from typing import Any, Protocol

import httpx

from flechtwerk import IncomingMessage, Message, State, Transformer

from examples.adsb_flight_tracker.attributes import (
    altitude_ft,
    HEX,
    IS_DELETED,
    POLLED_AT,
    REGION,
    SRC_ALTITUDE,
    SRC_CALLSIGN,
    SRC_GROUND_SPEED,
    SRC_LAT,
    SRC_LON,
)

INPUT_TOPIC = "adsb.aircraft"
TABLE = "adsb_positions"
CLICKHOUSE_URL = "http://localhost:8123"
DATABASE = "flechtwerk"


def dedup_token(msg: IncomingMessage) -> str:
    """A token stable across reprocessing — the record's Kafka coordinates.

    Reprocessing the same record (at-least-once) yields the same token, so the
    ClickHouse insert deduplicates against the first, committed one.
    """
    return f"{msg.topic}:{msg.partition}:{msg.offset}"


def to_rows(msg: IncomingMessage) -> list[dict[str, Any]]:
    """Project one aircraft event into ClickHouse rows (a positions history).

    Example 1 spreads the adsb.lol feed through under its wire names, so this reads
    the ``SRC_*`` handles (``flight``/``alt_baro``/``gs``/``lat``/``lon``) and
    projects them into this sink's own clean ``adsb_positions`` columns — renaming
    is the sink's job. Two kinds of event never become a position row —
    deterministically, so a reprocessed event is skipped again: a departure
    tombstone (``is_deleted=1``), and an identity-only event with no lat/lon (a
    Mode-S aircraft that broadcast no position). A positions history must not
    fabricate a ``(0, 0)`` fix. Optional telemetry the feed omitted (altitude,
    ground_speed) is likewise left out of the row so it lands as NULL, not a zero.
    ``alt_baro`` arrives faithful (a number or the string ``"ground"``); ``altitude_ft``
    yields feet for a number and ``None`` for ``"ground"`` (a surface aircraft has no
    numeric altitude — ``"ground"`` is not 0 ft), so it lands as NULL here too, never a
    fabricated 0 — the same "NULL not a zero" rule as the omitted telemetry above.
    """
    event = msg.value
    if event.get(IS_DELETED):
        return []
    lat, lon = event.get(SRC_LAT), event.get(SRC_LON)
    if lat is None or lon is None:
        return []
    row: dict[str, Any] = {
        "hex": event[HEX],
        "callsign": (event.get(SRC_CALLSIGN) or "").strip(),
        "lat": float(lat),
        "lon": float(lon),
        "region": event[REGION],
        "polled_at": event[POLLED_AT].isoformat(),
        "source_partition": msg.partition,
        "source_offset": msg.offset,
    }
    if (altitude := altitude_ft(event.get(SRC_ALTITUDE))) is not None:
        row["altitude"] = altitude
    if (ground_speed := event.get(SRC_GROUND_SPEED)) is not None:
        row["ground_speed"] = float(ground_speed)
    return [row]


class ClickHouseWriter(Protocol):
    """The narrow sink surface `AdsbSink` needs — real over HTTP, fake in tests."""

    async def insert(self, table: str, rows: list[dict[str, Any]], *, dedup_token: str) -> None: ...


class HttpClickHouseWriter:
    """Inserts rows over ClickHouse's HTTP interface with an idempotency token."""

    def __init__(self, *, base_url: str = CLICKHOUSE_URL, database: str = DATABASE,
                 user: str = "default", password: str = "") -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=30.0,
            params={"user": user, "password": password, "database": database},
        )

    async def insert(self, table: str, rows: list[dict[str, Any]], *, dedup_token: str) -> None:
        body = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(row) for row in rows)
        response = await self._client.post(
            "/",
            content=body,
            params={"insert_deduplication_token": dedup_token, "date_time_input_format": "best_effort"},
        )
        response.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()


class AdsbSink(Transformer):
    """Sinks `adsb.aircraft` position events into ClickHouse, idempotently.

    A pure sink: it emits nothing to Kafka and keeps no state, so its task
    transaction only commits the input offset. The insert is the side effect
    outside that transaction — hence the dedup token (see the module docstring).
    """

    input_topics = [INPUT_TOPIC]

    def __init__(self, writer: ClickHouseWriter | None = None) -> None:
        super().__init__()
        self._writer = writer

    async def __aenter__(self) -> "AdsbSink":
        if self._writer is None:
            self._writer = HttpClickHouseWriter()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if isinstance(self._writer, HttpClickHouseWriter):
            await self._writer.aclose()

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        assert self._writer is not None, "writer is created in __aenter__ or injected"
        rows = to_rows(msg)
        if rows:
            await self._writer.insert(TABLE, rows, dedup_token=dedup_token(msg))
        return
        yield  # pragma: no cover — a pure sink emits nothing to Kafka


stage = AdsbSink()
"""The stage the dispatcher runs (``python -m examples.clickhouse_sink``)."""

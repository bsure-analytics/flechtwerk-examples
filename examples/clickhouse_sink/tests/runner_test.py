"""Tier 2 — the transformer runner over the shipped fakes.

Drives the real `TransformerRunner.process_batch` (via `_FlechtwerkModule`) with
`flechtwerk.testing` doubles and an app-level fake ClickHouse writer, asserting
that positions are inserted with a stable dedup token, tombstones are skipped,
and the pure sink emits nothing to Kafka while still committing its offsets.
"""
import json
from typing import Any

from flechtwerk.module import _FlechtwerkModule
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.clickhouse_sink.sink import AdsbSink, INPUT_TOPIC


class RecordingWriter:
    """App-level fake ClickHouse writer — records inserts for assertions.

    (The framework ships no ClickHouse fake; this is not the "parallel
    scaffolding" the plan forbids — that means reinventing the Kafka/state
    doubles, which we reuse verbatim.)
    """

    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[dict[str, Any]], str]] = []

    async def insert(self, table: str, rows: list[dict[str, Any]], *, dedup_token: str) -> None:
        self.inserts.append((table, rows, dedup_token))


def _record(payload: dict, *, offset: int):
    return make_record(topic=INPUT_TOPIC, partition=0, offset=offset, value=json.dumps(payload))


def _make_module(stage: AdsbSink, records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "clickhouse-sink"
    mod.client_id = "clickhouse-sink"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = stage
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


POSITION = {"hex": "abc123", "flight": "BAW123", "alt_baro": 30000, "gs": 420.0,
            "lat": 51.5, "lon": -0.4, "requested_region": "london", "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 0}
TOMBSTONE = {"hex": "gone99", "requested_region": "london", "polled_at": "2026-07-17T12:00:05Z", "is_deleted": 1}


async def test_sink_inserts_positions_skips_tombstones_and_emits_nothing() -> None:
    writer = RecordingWriter()
    mod = _make_module(AdsbSink(writer=writer), [_record(POSITION, offset=0), _record(TOMBSTONE, offset=1)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    # One insert (the position); the tombstone was skipped.
    assert len(writer.inserts) == 1
    table, rows, token = writer.inserts[0]
    assert table == "adsb_positions"
    assert rows[0]["hex"] == "abc123"
    assert token == "adsb-aircraft:0:0"

    # Pure sink: nothing produced to Kafka, but the batch runs in one task
    # transaction that advances the input offset.
    assert producer.sent == []
    assert producer.transaction_count == 1
    assert producer.offsets_sent  # offset committed inside the task transaction

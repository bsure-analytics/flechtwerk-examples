"""Tier 2 — the transformer runner over the shipped fakes.

Drives `TransformerRunner.process_batch` over a batch of same-key records (one
serial bucket) and asserts the sequence is 1, 2, 3 in offset order, the input
values are forwarded, and the final counter is persisted to the task store.
"""
import json

from flechtwerk.module import _FlechtwerkModule
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.chaos_harness.attributes import COUNT
from examples.chaos_harness.transformer import INPUT_TOPIC, STATE_KEY, sequencer


def _make_module(records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "chaos-harness"
    mod.client_id = "chaos-harness"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = sequencer
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


async def test_batch_is_sequenced_exactly_once_and_the_counter_persists() -> None:
    records = [make_record(key=STATE_KEY, value=json.dumps({"n": n}), topic=INPUT_TOPIC, offset=n)
               for n in range(3)]
    mod = _make_module(records)
    runner = mod.runner
    producer = runner.tasks[0].producer
    store = runner.tasks[0].store

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    sent = [json.loads(payload["value"]) for _, payload in producer.sent]
    assert [s["n"] for s in sent] == [0, 1, 2]
    assert [s["seq"] for s in sent] == [1, 2, 3]  # serial bucket → contiguous counter
    assert (await store.get(STATE_KEY))[COUNT] == 3  # final counter persisted in the transaction

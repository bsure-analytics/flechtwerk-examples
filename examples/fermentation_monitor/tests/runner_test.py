"""Tier 2 — runner tier with the shipped fakes.

Two stages, two doubles: the bridge is driven with `FakeMqttConnection` (no
broker) to pin the ACK-after-Kafka contract; the monitor is driven through
`TransformerRunner.process_batch` (no broker, no ClickHouse) to pin the stall alert.
"""
import json

from flechtwerk import Config, Message, State
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.mqtt import MqttExtractor
from flechtwerk.testing import (
    FakeKafkaConsumer,
    FakeKafkaProducer,
    FakeMqttConnection,
    InMemoryStateStore,
    RecordingObserver,
    make_record,
)
from flechtwerk.transformer import Task

from examples.fermentation_monitor.attributes import GRAVITY
from examples.fermentation_monitor.bridge import CONFIG_TOPIC, READINGS_TOPIC, to_reading
from examples.fermentation_monitor.monitor import monitor

BATCH_CONFIG = Config.wrap({"topic": "ispindel/batch-42", "name": "batch-42"})


# --- the bridge: ACK only after Kafka ---

def _make_bridge() -> MqttExtractor:
    ext = MqttExtractor.of(config_topics=[CONFIG_TOPIC], relay=to_reading)
    ext.connection = FakeMqttConnection()
    ext.connection.subscribe(BATCH_CONFIG.raw["topic"])  # SUBACK precedes delivery in production
    ext.observer = RecordingObserver()
    return ext


async def _poll(ext: MqttExtractor) -> list[Message]:
    return [item async for item in ext.poll(BATCH_CONFIG, State())]


async def test_bridge_acks_only_after_the_batch_is_in_kafka() -> None:
    ext = _make_bridge()
    ext.connection.publish(
        topic="ispindel/batch-42",
        payload=json.dumps({"name": "batch-42", "gravity": 1.048, "temperature": 20.0}).encode(),
    )

    messages = await _poll(ext)
    assert [m.value[GRAVITY] for m in messages] == [1.048]

    sub = ext.connection.subscriptions["ispindel/batch-42"]
    assert len(sub.pending_acks) == 1 and sub.acked == []  # forwarded, not yet ACKed

    # The next poll ACKs the previous batch — provably durable in Kafka by now.
    assert await _poll(ext) == []
    assert len(sub.acked) == 1 and sub.pending_acks == []


# --- the monitor: stall alert through the runner ---

def _reading_record(gravity: float, *, offset: int):
    value = {"batch": "batch-42", "gravity": gravity, "temperature": 20.0, "at": "2026-07-17T12:00:00Z"}
    return make_record(key="batch-42", value=json.dumps(value), topic=READINGS_TOPIC, offset=offset)


def _make_monitor_module(records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "fermentation-monitor"
    mod.client_id = "fermentation-monitor"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = monitor
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


async def test_monitor_emits_a_stall_alert_through_the_runner() -> None:
    gravities = [1.050, 1.040, 1.040, 1.040, 1.040]  # falls, then flat → stall
    mod = _make_monitor_module([_reading_record(g, offset=i) for i, g in enumerate(gravities)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    alerts = [json.loads(payload["value"]) for _, payload in producer.sent]
    assert [a["kind"] for a in alerts] == ["stall"]
    assert alerts[0]["batch"] == "batch-42"

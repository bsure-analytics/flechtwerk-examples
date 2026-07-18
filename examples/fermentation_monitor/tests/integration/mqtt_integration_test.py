"""Tier 3 — integration. The MQTT→Kafka bridge, real broker to real broker.

Runs the actual `MqttExtractor` via `Flechtwerk.run()` against an ephemeral
Mosquitto and Kafka (testcontainers): publish an iSpindel reading over MQTT and
assert the relayed reading lands on `fermentation.readings` in Kafka. This is
the end-to-end path the unit tiers can only mock.
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import paho.mqtt.client as mqtt
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.module import Flechtwerk, MqttBrokerConfig
from flechtwerk.mqtt import MqttExtractor

from examples.fermentation_monitor.bridge import CONFIG_TOPIC, READINGS_TOPIC, to_reading

pytestmark = pytest.mark.integration


async def _prepare_kafka(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(CONFIG_TOPIC, num_partitions=8, replication_factor=1,
                     topic_configs={"cleanup.policy": "compact"}),
            NewTopic(READINGS_TOPIC, num_partitions=8, replication_factor=1),
        ])
        from aiokafka import AIOKafkaProducer
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                CONFIG_TOPIC, key=b"batch-42",
                value=json.dumps({"topic": "ispindel/batch-42", "name": "batch-42"}).encode())
        finally:
            await producer.stop()
    finally:
        await admin.close()


def _publish(broker: dict, gravity: float) -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"pub-{uuid4().hex[:6]}")
    client.connect(broker["broker"], broker["port"])
    client.loop_start()
    try:
        payload = json.dumps({"name": "batch-42", "gravity": gravity, "temperature": 20.0})
        client.publish("ispindel/batch-42", payload, qos=1).wait_for_publish(5)
    finally:
        client.loop_stop()
        client.disconnect()


async def test_bridge_relays_a_reading_from_mqtt_to_kafka(kafka_bootstrap: str, mosquitto: dict) -> None:
    await _prepare_kafka(kafka_bootstrap)
    application_id = f"ferm-{uuid4().hex[:8]}"
    flechtwerk = Flechtwerk.of(
        application_id=application_id,
        bootstrap_servers=kafka_bootstrap,
        client_id=f"{application_id}-0",
        mqtt=MqttBrokerConfig(broker=mosquitto["broker"], port=mosquitto["port"]),
        poll_interval=timedelta(milliseconds=200),
        stage=MqttExtractor.of(config_topics=[CONFIG_TOPIC], relay=to_reading),
    )
    consumer = AIOKafkaConsumer(
        READINGS_TOPIC, bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest", group_id=None, isolation_level="read_committed",
    )
    await consumer.start()
    run_task = asyncio.create_task(flechtwerk.run())
    try:
        readings: list[dict] = []
        deadline = asyncio.get_running_loop().time() + 45.0
        while not readings:
            if run_task.done():
                run_task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("no reading reached Kafka")
            # Republish each round: the bridge may not have subscribed yet, and a
            # QoS-1 publish to no subscriber is dropped by the broker.
            _publish(mosquitto, gravity=1.045)
            batch = await consumer.getmany(timeout_ms=1000)
            readings = [json.loads(m.value) for records in batch.values() for m in records]

        assert readings[0]["batch"] == "batch-42"
        assert readings[0]["gravity"] == 1.045
    finally:
        run_task.cancel()
        with suppress(asyncio.CancelledError):
            await run_task
        await consumer.stop()

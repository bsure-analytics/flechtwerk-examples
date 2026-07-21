"""Tier 3 — integration. The co-partitioned join against a real broker.

Produces a mention and then, a beat later, its event — to two topics keyed identically
by ``GlobalEventID`` — and runs the real ``GdeltEventCoverage`` transformer. Proves the
out-of-order path end to end: the mention lands on the same partition/task as its event
(real key-hash co-partitioning), is buffered as an orphan, and is reconciled the moment
the event arrives — the final coverage record reads ``event_seen=1`` with the mention
still counted. Topics are per-test unique; the transform re-targets its output topic too,
so nothing contaminates a sibling test.
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk import Message, Transformer
from flechtwerk.module import Flechtwerk

from examples.gdelt_news_stories.coverage import join_coverage

pytestmark = pytest.mark.integration

FILE_TS = "2026-07-21T08:30:00Z"


def _raw(table: str, event_id: str, **row) -> bytes:
    return json.dumps({"row": {"GlobalEventID": event_id, **row},
                       "metadata": {"table": table, "file_ts": FILE_TS}}).encode()


async def test_orphan_mention_reconciles_with_its_event_over_the_broker(kafka_bootstrap: str) -> None:
    suffix = uuid4().hex[:8]
    events_topic, mentions_topic, out_topic = (f"events-{suffix}", f"mentions-{suffix}", f"coverage-{suffix}")

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(t, num_partitions=8, replication_factor=1)
                                   for t in (events_topic, mentions_topic, out_topic)])
    finally:
        await admin.close()

    async def _transform(msg, state):
        async for item in join_coverage(state, msg):
            yield Message(key=item.key, topic=out_topic, value=item.value) if isinstance(item, Message) else item

    stage = Transformer.of(input_topics=[events_topic, mentions_topic], transform=_transform)
    app = Flechtwerk.of(application_id=f"gdelt-coverage-{suffix}", bootstrap_servers=kafka_bootstrap,
                        client_id=f"gdelt-coverage-{suffix}-0", stage=stage)
    consumer = AIOKafkaConsumer(out_topic, bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest",
                                group_id=None, isolation_level="read_committed")
    await consumer.start()
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        # Mention first (orphan), then — after the stage has surely processed it — the event.
        await producer.send_and_wait(mentions_topic, key=b"42", value=_raw("mentions", "42", MentionSourceName="bbc.co.uk"))
        await asyncio.sleep(2.0)
        await producer.send_and_wait(events_topic, key=b"42", value=_raw("events", "42", EventRootCode="14"))

        latest: dict = {}
        deadline = asyncio.get_running_loop().time() + 60.0
        while latest.get("event_seen") != 1:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never reconciled: {latest}")
            for _tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for record in records:
                    latest = json.loads(record.value)

        assert latest["event_seen"] == 1 and latest["EventRootCode"] == "14"
        assert latest["mention_count"] == 1 and latest["distinct_sources"] == 1  # orphan aggregate kept
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await producer.stop()
        await consumer.stop()

"""Tier 3 — integration. Ephemeral Kafka via testcontainers, the real runner.

Runs the actual ``Flechtwerk.of(...).run()`` for the ingest extractor against a real
broker (the shared session-scoped Kafka fixture), with only the GDELT feed stubbed
(the committed fixtures served over an ``httpx.MockTransport``). It proves the whole
path: config bootstrap, size/MD5-verified download, per-slice transaction, keys on the
three raw topics, and — the cursor's job — that letting it keep polling re-emits the
slice exactly once (GKG stays at 300, not 600). Topics are per-test unique (the stage
takes injectable raw-topic names + config topic) so nothing contaminates a sibling test.
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.module import Flechtwerk

from examples.gdelt_news_stories.ingest import GdeltIngest

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _fixture_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        path = FIXTURES / request.url.path.rsplit("/", 1)[-1]
        if path.suffix == ".txt":
            return httpx.Response(200, text=path.read_text())
        return httpx.Response(200, content=path.read_bytes())

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://gdelt.test")


async def test_ingest_lands_a_slice_once_across_repeated_polls(kafka_bootstrap: str) -> None:
    suffix = uuid4().hex[:8]
    feeds_topic = f"gdelt-feeds-{suffix}"
    raw = {"events": f"events-{suffix}", "mentions": f"mentions-{suffix}", "gkg": f"gkg-{suffix}"}

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(feeds_topic, num_partitions=8, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
            NewTopic(raw["events"], num_partitions=8, replication_factor=1),
            NewTopic(raw["mentions"], num_partitions=8, replication_factor=1),
            NewTopic(raw["gkg"], num_partitions=1, replication_factor=1),  # single-partition clustering input
        ])
    finally:
        await admin.close()

    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(feeds_topic, key=b"english", value=json.dumps({"feed": "english"}).encode())
    finally:
        await producer.stop()

    stage = GdeltIngest(client=_fixture_client(), base_url="http://gdelt.test", raw_topics=raw)
    stage.config_topics = [feeds_topic]
    app = Flechtwerk.of(
        application_id=f"gdelt-ingest-{suffix}",
        bootstrap_servers=kafka_bootstrap,
        client_id=f"gdelt-ingest-{suffix}-0",
        poll_interval=timedelta(milliseconds=200),
        stage=stage,
    )
    consumer = AIOKafkaConsumer(
        raw["events"], raw["mentions"], raw["gkg"],
        bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest",
        group_id=None, isolation_level="read_committed",
    )
    await consumer.start()
    task = asyncio.create_task(app.run())
    try:
        counts = {topic: 0 for topic in raw.values()}
        keyed_ok = True
        deadline = asyncio.get_running_loop().time() + 90.0
        while counts[raw["gkg"]] < 300:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"incomplete: {counts}")
            batch = await consumer.getmany(timeout_ms=500)
            for tp, records in batch.items():
                counts[tp.topic] += len(records)
                keyed_ok = keyed_ok and all(r.key for r in records)
        await asyncio.sleep(2.0)  # several more poll cycles; the cursor should gate them
        for tp, records in (await consumer.getmany(timeout_ms=500)).items():
            counts[tp.topic] += len(records)

        assert counts[raw["gkg"]] == 300              # emitted exactly once — cursor gated re-polls
        assert counts[raw["events"]] > 0 and counts[raw["mentions"]] > 0
        assert keyed_ok                               # every raw record carries its key
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await consumer.stop()

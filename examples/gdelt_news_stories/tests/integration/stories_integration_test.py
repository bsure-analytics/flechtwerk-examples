"""Tier 3 — integration. Online clustering + config-topic annotation over a real broker.

Seeds the ``gdelt-outlets`` config table, produces two same-story GKG articles (different
countries' outlets) to a single-partition clustering topic, and runs the real
``GdeltStories`` transformer. Proves that the constant-bucket keyed state clusters the two
articles into one story and annotates its coverage spread from the config table (GB + FR).
Input + config topics are per-test unique; the shared ``gdelt-stories`` output is
disambiguated by a per-test URL marker so nothing contaminates a sibling test.
"""
import asyncio
import json
from contextlib import suppress
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.module import Flechtwerk

from examples.gdelt_news_stories.stories import STORIES_TOPIC, GdeltStories

pytestmark = pytest.mark.integration


async def _ensure_topic(admin: AIOKafkaAdminClient, topic: NewTopic) -> None:
    if topic.name not in set(await admin.list_topics()):
        await admin.create_topics([topic])


async def test_stories_cluster_and_annotate_over_the_broker(kafka_bootstrap: str) -> None:
    suffix = uuid4().hex[:8]
    gkg_topic, outlets_topic = f"gkg-{suffix}", f"outlets-{suffix}"
    persons = "Keir Starmer,1;Andy Burnham,2;John Healey,3"

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(gkg_topic, num_partitions=1, replication_factor=1),  # single-partition clustering input
            NewTopic(outlets_topic, num_partitions=8, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
        ])
        await _ensure_topic(admin, NewTopic(STORIES_TOPIC, num_partitions=8, replication_factor=1))
    finally:
        await admin.close()

    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(outlets_topic, key=b"bbc.co.uk",
                                     value=json.dumps({"domain": "bbc.co.uk", "country": "GB"}).encode())
        await producer.send_and_wait(outlets_topic, key=b"lemonde.fr",
                                     value=json.dumps({"domain": "lemonde.fr", "country": "FR"}).encode())
    finally:
        await producer.stop()

    stage = GdeltStories()
    stage.input_topics = [gkg_topic]
    stage.config_topics = [outlets_topic]
    app = Flechtwerk.of(application_id=f"gdelt-stories-{suffix}", bootstrap_servers=kafka_bootstrap,
                        client_id=f"gdelt-stories-{suffix}-0", stage=stage)
    consumer = AIOKafkaConsumer(STORIES_TOPIC, bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest",
                                group_id=None, isolation_level="read_committed")
    await consumer.start()

    def _gkg(url: str, domain: str) -> bytes:
        return json.dumps({"row": {"DocumentIdentifier": url, "SourceCommonName": domain, "V2EnhancedPersons": persons},
                           "metadata": {"table": "gkg", "file_ts": "2026-07-21T08:30:00Z"}}).encode()

    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    task = asyncio.create_task(app.run())
    try:
        await producer.send_and_wait(gkg_topic, key=f"http://{suffix}/a".encode(), value=_gkg(f"http://{suffix}/a", "bbc.co.uk"))
        await producer.send_and_wait(gkg_topic, key=f"http://{suffix}/b".encode(), value=_gkg(f"http://{suffix}/b", "lemonde.fr"))

        latest: dict = {}
        deadline = asyncio.get_running_loop().time() + 60.0
        while latest.get("article_count") != 2:
            if task.done():
                task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"never clustered: {latest}")
            for _tp, records in (await consumer.getmany(timeout_ms=500)).items():
                for record in records:
                    value = json.loads(record.value)
                    if str(value.get("sample_url", "")).startswith(f"http://{suffix}/"):
                        latest = value

        assert latest["article_count"] == 2                       # both articles clustered into one story
        assert latest["country_count"] == 2                       # GB + FR from the config table
        assert sorted(latest["countries"]) == ["FR", "GB"]
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await producer.stop()
        await consumer.stop()

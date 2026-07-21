"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no
broker, no network — with the GDELT feed served from the committed fixtures via an
``httpx.MockTransport``:

- **ingest** drives the real ``ExtractorRunner``/``poll_one``, pinning that a new
  slice's rows land on the three raw topics, the per-feed cursor advances, and a
  second poll of the same slice re-emits nothing (the cursor gates it);
- **coverage** drives ``TransformerRunner.process_batch`` on co-partitioned
  Events + Mentions, pinning the orphan-mention buffer resolving when the event lands;
- **stories** drives ``process_batch`` on the single clustering bucket, pinning that
  same-story articles cluster, dedup skips a re-crawl, and outlets annotate coverage.
"""
import asyncio
import json
from datetime import timedelta
from pathlib import Path

import httpx

from flechtwerk import Config
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.gdelt_news_stories.coverage import COVERAGE_TOPIC, coverage
from examples.gdelt_news_stories.ingest import (
    EVENTS_RAW_TOPIC,
    FEEDS_CONFIG_TOPIC,
    GKG_RAW_TOPIC,
    MENTIONS_RAW_TOPIC,
    GdeltIngest,
)
from examples.gdelt_news_stories.stories import STORIES_TOPIC, GdeltStories
from examples.gdelt_news_stories.schema import FILE_TS, OUTLET_COUNTRY, OUTLET_DOMAIN

FIXTURES = Path(__file__).parent / "fixtures"


def _make_module(stage, records: list) -> _FlechtwerkModule:
    """Wire the real runner over the shipped fakes for a single task (partition 0)."""
    mod = _FlechtwerkModule()
    mod.application_id = "gdelt"
    mod.client_id = "gdelt"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = stage
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


def _by_topic(producer: FakeKafkaProducer) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for topic, payload in producer.sent:
        out.setdefault(topic, []).append(json.loads(payload["value"]))
    return out


# --- ingest: the real ExtractorRunner over the shipped fakes + a fixture-serving stub ---

def _fixture_client(pointer: str) -> httpx.AsyncClient:
    """An httpx client whose GETs serve the committed fixtures by basename: the pointer
    file, and each announced zip. Mirrors ADS-B's MockTransport stub idiom."""
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        path = FIXTURES / (pointer if name in ("lastupdate.txt", "lastupdate-translation.txt") else name)
        if path.suffix == ".txt":
            return httpx.Response(200, text=path.read_text())
        return httpx.Response(200, content=path.read_bytes())

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://gdelt.test")


def _extractor_runner(stage: GdeltIngest, feed: str) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one feed config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "gdelt-ingest-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=feed, value=json.dumps({"feed": feed}), topic=FEEDS_CONFIG_TOPIC)])
    runner.create_restore_consumer = lambda: FakeKafkaConsumer()
    runner.create_token_producer = lambda token: producer
    runner.extractor = stage
    runner.inner_store = inner
    runner.observer = Observer()
    runner.poll_interval = timedelta(0)
    runner.num_tokens = 1
    runner.tokens = frozenset({0})

    store = ChangelogStateStore()
    store.inner = inner
    store.producer = FakeKafkaProducer()
    store.topic = runner.changelog_topic
    runner.tasks[0] = TokenTask(asyncio.Lock(), producer, store)
    return runner, producer


async def test_ingest_emits_a_slice_then_the_cursor_gates_a_re_poll() -> None:
    stage = GdeltIngest(client=_fixture_client("lastupdate.txt"), base_url="http://gdelt.test")
    runner, producer = _extractor_runner(stage, "english")
    await runner.load_initial_configs()

    async with stage:
        await runner.poll_one(runner.entries["english"])

        topics = {topic for topic, _ in producer.sent}
        assert {EVENTS_RAW_TOPIC, MENTIONS_RAW_TOPIC, GKG_RAW_TOPIC} <= topics
        gkg = [payload for topic, payload in producer.sent if topic == GKG_RAW_TOPIC]
        assert len(gkg) == 300                                   # every fixture GKG row
        assert all(payload["key"] for payload in gkg)            # keyed by the article URL
        cursor = await runner.tasks[0].store.get("english")      # per-feed cursor committed
        assert cursor is not None and cursor[FILE_TS] is not None

        # Same pointer on the next poll (slice not newer than the cursor) → nothing re-emitted.
        before = len(producer.sent)
        await runner.poll_one(runner.entries["english"])
        assert len(producer.sent) == before


async def _poll_error(handler) -> str | None:
    """Run one english poll against a stub handler; return the ValueError message, or None."""
    stage = GdeltIngest(client=httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                                 base_url="http://gdelt.test"),
                        base_url="http://gdelt.test")
    runner, _ = _extractor_runner(stage, "english")
    await runner.load_initial_configs()
    async with stage:
        try:
            await runner.poll_one(runner.entries["english"])
            return None
        except ValueError as exc:
            return str(exc)


async def test_ingest_rejects_a_wrong_size() -> None:
    # A truncated/wrong download fails the size check first (no MD5 computed) — the slice
    # must crash ("let it crash"), never commit a corrupt download.
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name == "lastupdate.txt":
            return httpx.Response(200, text=(FIXTURES / "lastupdate.txt").read_text())
        return httpx.Response(200, content=b"corrupted-not-a-zip")

    message = await _poll_error(handler)
    assert message is not None and "bytes" in message  # size branch fired


async def test_ingest_rejects_a_wrong_md5_when_size_matches() -> None:
    # Right length, wrong content (one byte flipped): the size check passes, so the MD5
    # check must catch it — proving both checks run and the size-then-MD5 order.
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name == "lastupdate.txt":
            return httpx.Response(200, text=(FIXTURES / "lastupdate.txt").read_text())
        data = (FIXTURES / name).read_bytes()
        if ".gkg." in name:  # same length, different bytes → same size, different MD5
            data = data[:-1] + bytes([data[-1] ^ 0xFF])
        return httpx.Response(200, content=data)

    message = await _poll_error(handler)
    assert message is not None and "md5" in message  # size passed, MD5 branch fired


async def test_ingest_skips_an_incomplete_slice() -> None:
    # GDELT announces a slice in the pointer before all three files are published (the
    # translation feed lags): a 404 on any file means not-ready, so the poll skips without
    # crashing and without advancing the cursor — the next poll retries.
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name == "lastupdate.txt":
            return httpx.Response(200, text=(FIXTURES / "lastupdate.txt").read_text())
        if ".gkg." in name:
            return httpx.Response(404)  # the slice's GKG file isn't published yet
        return httpx.Response(200, content=(FIXTURES / name).read_bytes())

    stage = GdeltIngest(client=httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                                 base_url="http://gdelt.test"),
                        base_url="http://gdelt.test")
    runner, producer = _extractor_runner(stage, "english")
    await runner.load_initial_configs()
    async with stage:
        await runner.poll_one(runner.entries["english"])          # must not raise
    assert producer.sent == []                                    # nothing emitted…
    assert await runner.tasks[0].store.get("english") is None     # …and the cursor did not advance


# --- coverage: TransformerRunner.process_batch, co-partitioned across batches ---

def _raw(table: str, key: str, row: dict, *, file_ts: str = "2026-07-21T08:30:00Z", offset: int = 0):
    topic = EVENTS_RAW_TOPIC if table == "events" else MENTIONS_RAW_TOPIC
    value = {"row": {"GlobalEventID": key, **row}, "metadata": {"table": table, "file_ts": file_ts}}
    return make_record(key=key, value=json.dumps(value), topic=topic, partition=0, offset=offset)


async def _process(mod: _FlechtwerkModule) -> None:
    runner = mod.runner
    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))


async def test_coverage_buffers_an_orphan_mention_then_reconciles_across_batches() -> None:
    # A mention arrives (batch 1) before its event (batch 2) — co-partitioned on GlobalEventID,
    # so both land on the same task/state bucket. The orphan is buffered, then reconciled.
    mod = _make_module(coverage, [_raw("mentions", "42", {"MentionSourceName": "bbc.co.uk"})])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await _process(mod)
    orphan = _by_topic(producer)[COVERAGE_TOPIC][-1]
    assert orphan["event_seen"] == 0 and orphan["mention_count"] == 1  # buffered, event unseen

    # Batch 2: the event row for the same id lands on the same task store.
    runner.consumer = FakeKafkaConsumer([_raw("events", "42", {"EventRootCode": "14"}, offset=1)])
    await _process(mod)
    reconciled = _by_topic(producer)[COVERAGE_TOPIC][-1]
    # The event summary rides through under its GDELT wire name (the repo's carry-wire-names
    # convention, as ADS-B keeps `flight`/`alt_baro`); the sink aliases it at query time.
    assert reconciled["event_seen"] == 1 and reconciled["EventRootCode"] == "14"
    assert reconciled["mention_count"] == 1                            # aggregate preserved
    assert await runner.tasks[0].store.get("42") is not None          # coverage state persisted


async def test_coverage_joins_event_and_mention_in_one_batch() -> None:
    # Same batch, same bucket: the framework processes input_topics in order (events before
    # mentions), so one transaction folds both into a single reconciled coverage record.
    mod = _make_module(coverage, [
        _raw("events", "7", {"EventRootCode": "03", "ActionGeo_FullName": "Berlin"}, offset=0),
        _raw("mentions", "7", {"MentionSourceName": "dw.com"}, offset=1),
    ])
    runner = mod.runner
    await _process(mod)
    latest = _by_topic(runner.tasks[0].producer)[COVERAGE_TOPIC][-1]
    assert latest["event_seen"] == 1 and latest["mention_count"] == 1 and latest["distinct_sources"] == 1


# --- stories: TransformerRunner.process_batch on the single clustering bucket ---

def _gkg(url: str, persons: str, *, domain: str, offset: int, file_ts: str = "2026-07-21T08:30:00Z"):
    value = {"row": {"DocumentIdentifier": url, "SourceCommonName": domain, "V2EnhancedPersons": persons},
             "metadata": {"table": "gkg", "file_ts": file_ts}}
    return make_record(key=url, value=json.dumps(value), topic=GKG_RAW_TOPIC, partition=0, offset=offset)


async def test_stories_cluster_dedup_and_annotate_coverage_from_outlets() -> None:
    stage = GdeltStories()
    # Seed the gdelt-outlets config table directly (the framework's documented test seam).
    stage.configs = ConfigStore.of({
        "bbc.co.uk": Config({OUTLET_DOMAIN: "bbc.co.uk", OUTLET_COUNTRY: "GB"}),
        "lemonde.fr": Config({OUTLET_DOMAIN: "lemonde.fr", OUTLET_COUNTRY: "FR"}),
    })
    shared = "Keir Starmer,1;Andy Burnham,2;John Healey,3"
    mod = _make_module(stage, [
        _gkg("http://a", shared, domain="bbc.co.uk", offset=0),
        _gkg("http://b", shared + ";Rachel Reeves,4", domain="lemonde.fr", offset=1),  # same story, FR outlet
        _gkg("http://a", shared, domain="bbc.co.uk", offset=2),                        # re-crawl → deduped
    ])
    await _process(mod)

    stories = _by_topic(mod.runner.tasks[0].producer)[STORIES_TOPIC]
    assert len(stories) == 2                          # a spawned, b joined, the re-crawl emitted nothing
    latest = stories[-1]
    assert latest["article_count"] == 2               # two distinct articles clustered
    assert latest["country_count"] == 2               # GB + FR → annotated coverage spread
    assert sorted(latest["countries"]) == ["FR", "GB"]
    assert await mod.runner.tasks[0].store.get("clusters") is not None  # single bucket persisted

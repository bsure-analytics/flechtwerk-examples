"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no
broker, no network — with the feeds served from the committed fixtures via an
``httpx.MockTransport``:

- **loader** drives the real ``ExtractorRunner``/``poll_one``: the static zip's rail
  trips land on ``gtfs-trip-profiles``, the version cursor advances, and an unchanged
  version re-emits nothing;
- **ingest** drives ``poll_one``: a snapshot's TripUpdates land on ``gtfs-trip-updates``
  keyed by ``trip_id``, the ``feed_ts`` cursor advances, and the same snapshot re-emits
  nothing (the cursor gates it);
- **delays** drives ``TransformerRunner.process_batch`` on co-partitioned profile +
  update records, pinning that an update joins its stored profile into a delay record,
  and that an update with no profile is dropped.
"""
import asyncio
import json
from datetime import timedelta
from pathlib import Path

import httpx

from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.gtfs_german_rail_delays.attributes import FEED_TS, STATIC_VERSION, STATUS, TRIP_ID
from examples.gtfs_german_rail_delays.delays import DELAYS_TOPIC, build_delay_state, classify, delays
from examples.gtfs_german_rail_delays.ingest import UPDATES_TOPIC, GtfsRtIngest, decode_feed
from examples.gtfs_german_rail_delays.loader import PROFILES_TOPIC, StaticGtfsLoader, build_profiles

FIXTURES = Path(__file__).parent / "fixtures"
FV_ZIP = (FIXTURES / "fv_sample.zip").read_bytes()
RT_PB = (FIXTURES / "rt_sample.pb").read_bytes()


# --- extractor harness (mirrors the GDELT runner test) ---

def _extractor_runner(stage: Extractor, key: str, url: str) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "gtfs-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=key, value=json.dumps({"url": url}), topic=stage.config_topics[0])])
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


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://gtfs.test")


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(p["value"]) for t, p in producer.sent if t == topic]


# --- loader ---

async def test_loader_emits_profiles_then_version_gates_a_re_poll() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, content=FV_ZIP, headers={"ETag": "fv-v1"})

    stage = StaticGtfsLoader(client=_mock_client(handler))
    runner, producer = _extractor_runner(stage, "fernverkehr", "http://gtfs.test/fv.zip")
    await runner.load_initial_configs()
    async with stage:
        await runner.poll_one(runner.entries["fernverkehr"])
        profiles = _sent(producer, PROFILES_TOPIC)
        assert len(profiles) == 5                                      # every fixture rail trip
        assert all(p["trip_id"] for p in profiles)                     # keyed content present
        cursor = await runner.tasks[0].store.get("fernverkehr")
        assert cursor is not None and cursor[STATIC_VERSION] == "fv-v1"

        before = len(producer.sent)
        await runner.poll_one(runner.entries["fernverkehr"])           # same ETag → version matches cursor
        assert len(producer.sent) == before                           # nothing re-emitted


async def test_loader_skips_on_304() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # First call (no If-None-Match) serves the feed; later calls 304.
        if request.headers.get("If-None-Match") == "fv-v1":
            return httpx.Response(304)
        return httpx.Response(200, content=FV_ZIP, headers={"ETag": "fv-v1"})

    stage = StaticGtfsLoader(client=_mock_client(handler))
    runner, producer = _extractor_runner(stage, "fernverkehr", "http://gtfs.test/fv.zip")
    await runner.load_initial_configs()
    async with stage:
        await runner.poll_one(runner.entries["fernverkehr"])           # emits + cursor "fv-v1"
        before = len(producer.sent)
        await runner.poll_one(runner.entries["fernverkehr"])           # If-None-Match → 304
        assert len(producer.sent) == before


# --- ingest ---

async def test_ingest_emits_updates_then_the_cursor_gates_a_re_poll() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=RT_PB, headers={"ETag": "rt-1"})

    stage = GtfsRtIngest(client=_mock_client(handler))
    runner, producer = _extractor_runner(stage, "germany-free", "http://gtfs.test/rt.pb")
    await runner.load_initial_configs()
    async with stage:
        await runner.poll_one(runner.entries["germany-free"])
        updates = _sent(producer, UPDATES_TOPIC)
        assert len(updates) == 5                                       # one per TripUpdate (alert ignored)
        assert {p["key"] for t, p in producer.sent if t == UPDATES_TOPIC}  # keyed by trip_id
        cursor = await runner.tasks[0].store.get("germany-free")
        assert cursor is not None and cursor[FEED_TS] is not None

        before = len(producer.sent)
        await runner.poll_one(runner.entries["germany-free"])          # same snapshot ts → not newer
        assert len(producer.sent) == before                           # nothing re-emitted


# --- delays: TransformerRunner.process_batch over co-partitioned records ---

def _make_module(stage, records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "gtfs"
    mod.client_id = "gtfs"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = stage
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


async def _process(mod: _FlechtwerkModule) -> None:
    await mod.runner.process_batch(await mod.runner.consumer.getmany(timeout_ms=1000))


def _live_pair():
    """A (profile, update, trip_id) for a fixture trip that is mid-journey (emits a record)."""
    profiles = dict(build_profiles(FV_ZIP, "v1"))
    feed_ts, updates = decode_feed(RT_PB)
    for tid, update in updates:
        if tid in profiles and build_delay_state(profiles[tid], update, feed_ts) is not None:
            return profiles[tid], update, tid
    raise AssertionError("no mid-journey fixture trip")


async def test_delays_joins_profile_then_update() -> None:
    profile, update, tid = _live_pair()
    mod = _make_module(delays, [
        make_record(key=tid, value=json.dumps(profile.raw), topic=PROFILES_TOPIC, partition=0, offset=0),
        make_record(key=tid, value=json.dumps(update.raw), topic=UPDATES_TOPIC, partition=0, offset=1),
    ])
    await _process(mod)
    out = _sent(mod.runner.tasks[0].producer, DELAYS_TOPIC)
    assert len(out) == 1
    record = out[0]
    assert record["trip_id"] == tid
    assert record["status"] == classify(record["delay_s"])             # self-consistent bucket
    assert 45.0 < record["lat"] < 56.0 and 5.0 < record["lon"] < 16.0  # placed at a German station
    assert await mod.runner.tasks[0].store.get(tid) is not None        # profile persisted as state


async def test_delays_drops_update_without_profile() -> None:
    _, update, tid = _live_pair()
    mod = _make_module(delays, [
        make_record(key=tid, value=json.dumps(update.raw), topic=UPDATES_TOPIC, partition=0, offset=0),
    ])
    await _process(mod)
    assert _sent(mod.runner.tasks[0].producer, DELAYS_TOPIC) == []      # no profile yet → dropped

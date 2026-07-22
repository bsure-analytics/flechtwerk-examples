"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker,
no network — with SMARD served from the committed fixtures via an ``httpx.MockTransport``
whose ``Date`` header (the event-time clock) the test controls:

- **ingest** drives the real ``ExtractorRunner``/``poll_one``: a first poll lands the
  fixture week's points as observations keyed by interval and sets the window; an
  identical re-poll emits nothing (the window suppresses it); mutating one value emits
  exactly one revision (with its previous value); advancing the ``Date`` past the window
  emits settled markers — but only for the series flagged as the settle marker.
- **mix** drives ``TransformerRunner.process_batch`` on co-partitioned observations: a
  batch of same-interval observations accumulates and each emits a preliminary; a later
  settled marker emits the final record and tombstones the join state; an observation
  older than the revision window emits a correcting row but builds no state.
"""
import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx

from flechtwerk import Event
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.smard_german_electricity_market.attributes import (
    BOOTSTRAPPED,
    C_ROLE,
    C_SOURCE,
    C_VALUE,
    FETCHED_AT,
    INTERVAL_TS,
    IS_FINAL,
    KIND,
    PREVIOUS_VALUE,
    REVISED,
    ROLE,
    SERIES_KEY,
    SERIES_NAME,
    SOURCE,
    TOTAL_GENERATION_MWH,
    UNIT,
    VALUE,
    WINDOW,
)
from examples.smard_german_electricity_market.ingest import (
    OBSERVATIONS_TOPIC,
    SmardIngest,
    interval_key,
)
from examples.smard_german_electricity_market.mix import MIX_TOPIC, mix

FIXTURES = Path(__file__).parent / "fixtures"
INDEX = json.loads((FIXTURES / "index_quarterhour_sample.json").read_text())
WEEK = json.loads((FIXTURES / "week_sample.json").read_text())
UTC = timezone.utc

WEEK_START_MS = INDEX["timestamps"][-1]
FRONTIER = datetime(2026, 7, 23, 12, tzinfo=UTC)          # the fixture's last realized point
FRONTIER_MS = int(FRONTIER.timestamp() * 1000)
NON_NULL = sum(1 for _, v in WEEK["series"] if v is not None)

SOLAR_CONFIG = {"filter": 4068, "region": "DE", "resolution": "quarterhour",
                "name": "Photovoltaics", "role": "source", "source": "solar", "unit": "MWh"}
LOAD_CONFIG = {"filter": 410, "region": "DE", "resolution": "quarterhour",
               "name": "Grid load", "role": "load", "unit": "MWh", "settle_marker": True}


# --- extractor harness (mirrors the GTFS / GDELT runner tests) ---

def _extractor_runner(stage: Extractor, key: str, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "smard-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=key, value=json.dumps(config), topic=stage.config_topics[0])])
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


def _handler(clock: dict, week: dict):
    """A MockTransport handler serving the index and the current week file, stamping each
    response with the ``Date`` the test currently wants (the event-time clock)."""
    def handle(request: httpx.Request) -> httpx.Response:
        headers = {"Date": format_datetime(clock["date"], usegmt=True)}
        path = request.url.path
        if path.endswith("index_quarterhour.json"):
            return httpx.Response(200, json=INDEX, headers=headers)
        if path.endswith(f"_{WEEK_START_MS}.json"):
            return httpx.Response(200, json=week, headers=headers)
        return httpx.Response(404, headers=headers)          # a week the fixture doesn't cover
    return handle


def _client(clock: dict, week: dict) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(_handler(clock, week)),
                             base_url="http://smard.test")


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(p["value"]) for t, p in producer.sent if t == topic]


async def _poll(stage: SmardIngest, key: str, config: dict, clock: dict, week: dict):
    runner, producer = _extractor_runner(stage, key, config)
    await runner.load_initial_configs()
    return runner, producer


# --- ingest ---

async def test_ingest_bootstrap_lands_observations_and_sets_window() -> None:
    clock = {"date": FRONTIER}
    stage = SmardIngest(client=_client(clock, WEEK), base_url="http://smard.test/chart_data")
    runner, producer = await _poll(stage, "4068_DE_quarterhour", SOLAR_CONFIG, clock, WEEK)
    async with stage:
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])
        observations = _sent(producer, OBSERVATIONS_TOPIC)
        assert len(observations) == NON_NULL                       # every non-null fixture slot
        assert all(o["kind"] == "observation" for o in observations)
        keys = {p["key"].decode() for t, p in producer.sent if t == OBSERVATIONS_TOPIC}
        assert interval_key(FRONTIER) in keys                      # keyed by the interval instant
        state = await runner.tasks[0].store.get("4068_DE_quarterhour")
        assert state is not None and state[BOOTSTRAPPED] is True and state[WINDOW]

        before = len(producer.sent)
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])  # identical snapshot
        assert len(producer.sent) == before                        # nothing re-emitted


async def test_ingest_detects_a_single_revision() -> None:
    clock = {"date": FRONTIER}
    stage = SmardIngest(client=_client(clock, WEEK), base_url="http://smard.test/chart_data")
    runner, producer = await _poll(stage, "4068_DE_quarterhour", SOLAR_CONFIG, clock, WEEK)
    async with stage:
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])   # bootstrap
        before = len(producer.sent)

        # Restate one in-window point (the frontier slot) and re-poll from the mutated week.
        mutated = copy.deepcopy(WEEK)
        original = next(v for ms, v in mutated["series"] if ms == FRONTIER_MS)
        for point in mutated["series"]:
            if point[0] == FRONTIER_MS:
                point[1] = original + 1.0
        stage._client = _client(clock, mutated)
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])

        new = [json.loads(p["value"]) for t, p in producer.sent[before:] if t == OBSERVATIONS_TOPIC]
        assert len(new) == 1                                        # exactly one correction
        assert new[0]["revised"] is True
        assert new[0]["previous_value"] == original
        assert new[0]["value"] == original + 1.0


async def test_ingest_marker_series_emits_settled_after_window_advances() -> None:
    clock = {"date": FRONTIER}
    stage = SmardIngest(client=_client(clock, WEEK), base_url="http://smard.test/chart_data")
    runner, producer = await _poll(stage, "410_DE_quarterhour", LOAD_CONFIG, clock, WEEK)
    async with stage:
        await runner.poll_one(runner.entries["410_DE_quarterhour"])   # bootstrap
        before = len(producer.sent)

        clock["date"] = FRONTIER + timedelta(days=3)                # window advances past the data
        await runner.poll_one(runner.entries["410_DE_quarterhour"])
        new = [json.loads(p["value"]) for t, p in producer.sent[before:] if t == OBSERVATIONS_TOPIC]
        assert new and all(o["kind"] == "settled" for o in new)     # only settled markers now
        assert all(o["series_key"] == "410_DE_quarterhour" for o in new)


async def test_ingest_non_marker_series_emits_no_settled() -> None:
    clock = {"date": FRONTIER}
    stage = SmardIngest(client=_client(clock, WEEK), base_url="http://smard.test/chart_data")
    runner, producer = await _poll(stage, "4068_DE_quarterhour", SOLAR_CONFIG, clock, WEEK)
    async with stage:
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])
        before = len(producer.sent)
        clock["date"] = FRONTIER + timedelta(days=3)
        await runner.poll_one(runner.entries["4068_DE_quarterhour"])
        new = [json.loads(p["value"]) for t, p in producer.sent[before:] if t == OBSERVATIONS_TOPIC]
        assert not any(o["kind"] == "settled" for o in new)         # not the marker series


# --- mix: TransformerRunner.process_batch over co-partitioned observations ---

def _make_module(records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "smard"
    mod.client_id = "smard"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = mix
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


async def _process(mod: _FlechtwerkModule) -> None:
    await mod.runner.process_batch(await mod.runner.consumer.getmany(timeout_ms=1000))


_INTERVAL = datetime(2026, 7, 23, 10, tzinfo=UTC)
_FETCHED = datetime(2026, 7, 23, 10, 5, tzinfo=UTC)


def _obs(series_key: str, role: str, value: float, *, source: str | None = None,
         interval: datetime = _INTERVAL, fetched_at: datetime = _FETCHED) -> str:
    record = Event({KIND: "observation", SERIES_KEY: series_key, SERIES_NAME: series_key,
                    ROLE: role, UNIT: "MWh", INTERVAL_TS: interval, VALUE: value,
                    REVISED: False, FETCHED_AT: fetched_at})
    if source is not None:
        record[SOURCE] = source
    return json.dumps(record.raw)


def _settled(series_key: str, *, interval: datetime = _INTERVAL, fetched_at: datetime) -> str:
    return json.dumps(Event({KIND: "settled", SERIES_KEY: series_key,
                             INTERVAL_TS: interval, FETCHED_AT: fetched_at}).raw)


async def test_mix_accumulates_then_settles_and_tombstones() -> None:
    key = interval_key(_INTERVAL)
    # Batch 1: three same-interval observations accumulate the mix (co-partitioned by key).
    mod = _make_module([
        make_record(key=key, value=_obs("solar", "source", 100.0, source="solar"), topic=OBSERVATIONS_TOPIC, offset=0),
        make_record(key=key, value=_obs("gas", "source", 100.0, source="gas"), topic=OBSERVATIONS_TOPIC, offset=1),
        make_record(key=key, value=_obs("load", "load", 500.0), topic=OBSERVATIONS_TOPIC, offset=2),
    ])
    await _process(mod)
    out = _sent(mod.runner.tasks[0].producer, MIX_TOPIC)
    assert len(out) == 3 and all(r["is_final"] is False for r in out)   # a preliminary per observation
    assert out[-1]["total_generation_mwh"] == 200.0                     # both sources folded in
    stored = await mod.runner.tasks[0].store.get(key)
    assert stored is not None                                           # join state persisted

    # Batch 2: the settle marker finalizes and tombstones the interval.
    final_fetched = _FETCHED + timedelta(hours=49)
    mod.consumer.records = [make_record(key=key, value=_settled("load", fetched_at=final_fetched),
                                        topic=OBSERVATIONS_TOPIC, offset=3)]
    await _process(mod)
    out = _sent(mod.runner.tasks[0].producer, MIX_TOPIC)
    assert out[-1]["is_final"] is True and out[-1]["total_generation_mwh"] == 200.0
    assert await mod.runner.tasks[0].store.get(key) is None             # bucket tombstoned


async def test_mix_drops_stale_observation_from_join_state() -> None:
    # An observation fetched long after its interval (a bootstrap-backfill point) emits a
    # correcting row but must not build join state that would never be settled.
    key = interval_key(_INTERVAL)
    stale_fetch = _INTERVAL + timedelta(hours=72)
    mod = _make_module([
        make_record(key=key, value=_obs("solar", "source", 100.0, source="solar", fetched_at=stale_fetch),
                    topic=OBSERVATIONS_TOPIC, offset=0),
    ])
    await _process(mod)
    assert len(_sent(mod.runner.tasks[0].producer, MIX_TOPIC)) == 1     # correcting row still flows
    assert await mod.runner.tasks[0].store.get(key) is None             # but no join state

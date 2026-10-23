"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker, no
network — with TMDB served by an ``httpx.MockTransport`` whose ``Date`` header (the event-time
clock) the test controls:

- **changes** drives the real ``ExtractorRunner``/``poll_one`` under a pinned clock: every page is
  backed up exactly as served, with metadata beside it and nothing read but the page count;
  closed days are listed once, the open day on every poll, and its cursor is the last closed day;
  the page cap is honoured and logged, a 401 names the remedy and a 500 crashes the poll.
- **split** consumes those very pages (extractor and transformer wired back to back): closed days
  emit every id, the open day only ids it has not seen, a closing day gets its one full re-list,
  and ``adult`` ids never leave.
- **catalog** drives ``TransformerRunner.process_batch``: one change becomes one flat version row
  plus one row per credit, a replay is gated out by the content hash, a 404 tombstones, and a
  change for a feed nobody requested is skipped.

The credential is **ciphertext on the wire**: every test runs inside ``installed_keyring()`` and
the config record is built with a typed write, so the stages prove they decrypt it.
"""
import asyncio
import json
import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
from flechtwerk import Config, Event, State, Transformer
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import (
    FakeKafkaConsumer,
    FakeKafkaProducer,
    InMemoryStateStore,
    installed_keyring,
    make_record,
)
from flechtwerk.transformer import Task

from examples.the_movie_database.attributes import (
    ACCESS_TOKEN,
    CHANGED_ON,
    CHANGES_RAW_TOPIC,
    CHANGES_TOPIC,
    CONTENT_HASH,
    CREDITS_TOPIC,
    CURSOR,
    FEED,
    FEEDS_TOPIC,
    LISTING,
    MOVIES_TOPIC,
    PEOPLE_TOPIC,
    SEEN,
    SINCE,
    TMDB_ID,
    TV_TOPIC,
    VERSION,
)
from examples.the_movie_database.catalog import TmdbCatalog
from examples.the_movie_database.changes import TmdbChanges
from examples.the_movie_database.split import TmdbSplit

FIXTURES = Path(__file__).parent / "fixtures"
MOVIE = json.loads((FIXTURES / "movie_550.json").read_text())
TV = json.loads((FIXTURES / "tv_87108.json").read_text())
PERSON = json.loads((FIXTURES / "person_287.json").read_text())

UTC = timezone.utc
BASE = "http://tmdb.test/3"
TOKEN = "test-token"
TODAY = date(2026, 10, 23)
YESTERDAY = TODAY - timedelta(days=1)
_NOON = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def keyring() -> Iterator[None]:
    with installed_keyring():
        yield


def _feed_record(feed: str = "movie", *, since: date | None = None) -> dict:
    """A feed's config record as it travels on the wire: ``access_token`` is ciphertext."""
    record = {FEED: feed, ACCESS_TOKEN: TOKEN}
    if since is not None:
        record[SINCE] = since
    return Config(record).raw


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(payload["value"]) for sent_topic, payload in producer.sent if sent_topic == topic]


def _keys(producer: FakeKafkaProducer, topic: str) -> list[str]:
    return [payload["key"].decode() for sent_topic, payload in producer.sent if sent_topic == topic]


def test_the_config_record_carries_ciphertext_not_the_token() -> None:
    raw = _feed_record(since=YESTERDAY)
    assert raw["access_token"].startswith("flenc:jwe:") and TOKEN not in json.dumps(raw)
    assert raw["since"] == YESTERDAY.isoformat()
    assert Config.wrap(raw)[ACCESS_TOKEN] == TOKEN


# --- changes: the ExtractorRunner over the fakes ---

def _extractor_runner(stage: TmdbChanges, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "tmdb-changes-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=config["feed"], value=json.dumps(config), topic=FEEDS_TOPIC)])
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


def _page(ids: list[int], *, total_pages: int = 1, adult: set[int] = frozenset()) -> dict:
    return {"results": [{"id": i, "adult": i in adult, "softcore": False} for i in ids],
            "page": 1, "total_pages": total_pages, "total_results": len(ids)}


class Tmdb:
    """A MockTransport TMDB serving change pages by ``(day, page)``, recording every request."""

    def __init__(self, pages: dict[tuple[date, int], dict], *, status: int = 200,
                 body: dict | None = None) -> None:
        self.pages = pages
        self.status = status
        self.body = body
        self.requests: list[httpx.Request] = []
        self.clock = {"now": _NOON}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        headers = {"Date": format_datetime(self.clock["now"], usegmt=True)}
        if self.status != 200:
            return httpx.Response(self.status, json=self.body or {}, headers=headers)
        params = request.url.params
        assert params["start_date"] == params["end_date"], "one calendar day per window"
        day, page = date.fromisoformat(params["start_date"]), int(params["page"])
        return httpx.Response(200, json=self.pages.get((day, page), _page([], total_pages=0)),
                              headers=headers)

    def stage(self, **kwargs) -> TmdbChanges:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))
        return TmdbChanges(client, base_url=BASE, clock=lambda: self.clock["now"], **kwargs)

    def days_requested(self) -> list[tuple[str, str]]:
        return [(r.url.params["start_date"], r.url.params["page"]) for r in self.requests]


async def _runner(stage: TmdbChanges, since: date | None) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    runner, producer = _extractor_runner(stage, _feed_record(since=since))
    await runner.load_initial_configs()
    return runner, producer


async def _cursor(runner: ExtractorRunner) -> date | None:
    state = await runner.tasks[0].store.get("movie")
    return state.get(CURSOR) if state else None


TWO_DAYS = {
    (YESTERDAY, 1): _page([1, 2, 3], total_pages=2),
    (YESTERDAY, 2): _page([4, 5], total_pages=2),
    (TODAY, 1): _page([5, 6]),
}


async def test_changes_backs_up_every_page_as_delivered_with_metadata() -> None:
    tmdb = Tmdb(TWO_DAYS)
    stage = tmdb.stage()
    runner, producer = await _runner(stage, YESTERDAY)
    async with stage:
        await runner.poll_one(runner.entries["movie"])

    pages = _sent(producer, CHANGES_RAW_TOPIC)
    assert [p["response"] for p in pages] == [                  # bodies untouched, adult flags and all
        TWO_DAYS[(YESTERDAY, 1)], TWO_DAYS[(YESTERDAY, 2)], TWO_DAYS[(TODAY, 1)]]
    assert [p["metadata"] for p in pages] == [
        {"feed": "movie", "changed_on": "2026-10-22", "page": 1, "listing": "closed",
         "discovered_at": "2026-10-23T12:00:00Z"},
        {"feed": "movie", "changed_on": "2026-10-22", "page": 2, "listing": "closed",
         "discovered_at": "2026-10-23T12:00:00Z"},
        {"feed": "movie", "changed_on": "2026-10-23", "page": 1, "listing": "open",
         "discovered_at": "2026-10-23T12:00:00Z"},
    ]
    assert set(_keys(producer, CHANGES_RAW_TOPIC)) == {"movie"}  # a feed's pages stay in order
    assert all(r.headers["Authorization"] == f"Bearer {TOKEN}" for r in tmdb.requests)
    assert tmdb.days_requested() == [("2026-10-22", "1"), ("2026-10-22", "2"), ("2026-10-23", "1")]
    assert await _cursor(runner) == YESTERDAY                   # the last CLOSED day — its only state


async def test_changes_re_lists_the_open_day_in_full_every_poll() -> None:
    tmdb = Tmdb(TWO_DAYS)
    stage = tmdb.stage()
    runner, producer = await _runner(stage, YESTERDAY)
    async with stage:
        await runner.poll_one(runner.entries["movie"])
        producer.sent.clear()
        await runner.poll_one(runner.entries["movie"])

    pages = _sent(producer, CHANGES_RAW_TOPIC)
    assert [(p["metadata"]["changed_on"], p["metadata"]["listing"]) for p in pages] == [
        ("2026-10-23", "open")]                                 # the closed day is not listed again
    assert pages[0]["response"] == TWO_DAYS[(TODAY, 1)]
    assert await _cursor(runner) == YESTERDAY


async def test_changes_closes_the_day_after_midnight() -> None:
    tmdb = Tmdb(TWO_DAYS)
    stage = tmdb.stage()
    runner, producer = await _runner(stage, YESTERDAY)
    async with stage:
        await runner.poll_one(runner.entries["movie"])
        producer.sent.clear()
        tmdb.clock["now"] = _NOON + timedelta(days=1)
        await runner.poll_one(runner.entries["movie"])

    pages = _sent(producer, CHANGES_RAW_TOPIC)
    assert [(p["metadata"]["changed_on"], p["metadata"]["listing"]) for p in pages] == [
        ("2026-10-23", "closed"), ("2026-10-24", "open")]
    assert await _cursor(runner) == TODAY


async def test_changes_does_not_look_at_the_ids() -> None:
    # An adult id, a duplicate and an unknown shape all travel untouched: filtering is split's job.
    body = {"results": [{"id": 1, "adult": True}, {"id": 1}, {"surprise": "field"}], "page": 1,
            "total_pages": 1, "total_results": 3}
    tmdb = Tmdb({(TODAY, 1): body})
    stage = tmdb.stage()
    runner, producer = await _runner(stage, TODAY)
    async with stage:
        await runner.poll_one(runner.entries["movie"])
    assert [p["response"] for p in _sent(producer, CHANGES_RAW_TOPIC)] == [body]


async def test_changes_defaults_the_epoch_to_the_last_ninety_days() -> None:
    tmdb = Tmdb({})
    stage = tmdb.stage()
    runner, _ = await _runner(stage, None)
    async with stage:
        await runner.poll_one(runner.entries["movie"])
    days = [d for d, _ in tmdb.days_requested()]
    assert days[0] == (TODAY - timedelta(days=90)).isoformat() and days[-1] == TODAY.isoformat()
    assert len(days) == 91                               # every day inclusive, one empty page each


async def test_changes_honours_the_page_cap_and_says_so(caplog: pytest.LogCaptureFixture) -> None:
    tmdb = Tmdb({(TODAY, 1): _page([1, 2], total_pages=2), (TODAY, 2): _page([3, 4], total_pages=2)})
    stage = tmdb.stage(max_pages=1)
    runner, producer = await _runner(stage, TODAY)
    with caplog.at_level(logging.WARNING):
        async with stage:
            await runner.poll_one(runner.entries["movie"])
    assert len(_sent(producer, CHANGES_RAW_TOPIC)) == 1                     # page 1 only
    assert tmdb.days_requested() == [("2026-10-23", "1")]
    assert "capped at 1" in caplog.text


async def test_changes_a_401_names_the_remedy() -> None:
    tmdb = Tmdb({}, status=401, body={"status_code": 7, "status_message": "Invalid API key"})
    stage = tmdb.stage()
    runner, _ = await _runner(stage, TODAY)
    async with stage:
        with pytest.raises(RuntimeError, match=r"Invalid API key.*request-tmdb token"):
            await runner.poll_one(runner.entries["movie"])


async def test_changes_a_500_crashes_the_poll() -> None:
    tmdb = Tmdb({}, status=500)
    stage = tmdb.stage()
    runner, _ = await _runner(stage, TODAY)
    async with stage:
        with pytest.raises(httpx.HTTPStatusError):
            await runner.poll_one(runner.entries["movie"])


# --- split: the extractor's pages through the TransformerRunner ---

class Pipeline:
    """The extractor and ``split`` wired back to back over the fakes: each poll's raw pages are
    fed to the split runner as one batch, so the open-day dedupe runs on what the backup holds."""

    def __init__(self, pages: dict[tuple[date, int], dict]) -> None:
        self.tmdb = Tmdb(pages)
        self.extractor = self.tmdb.stage()
        self.split = _make_module(TmdbSplit(), [])
        self.offset = 0

    async def poll(self) -> list[dict]:
        runner, producer = self.runner, self.producer
        await runner.poll_one(runner.entries["movie"])  # client injected: no __aenter__ needed
        records = []
        for topic, payload in producer.sent:
            records.append(make_record(key=payload["key"], value=payload["value"], topic=topic,
                                       partition=0, offset=self.offset))
            self.offset += 1
        producer.sent.clear()
        out = self.split.runner.tasks[0].producer
        out.sent.clear()
        self.split.runner.consumer = FakeKafkaConsumer(records)
        await _process(self.split)
        return _sent(out, CHANGES_TOPIC)

    async def start(self, since: date) -> "Pipeline":
        self.runner, self.producer = await _runner(self.extractor, since)
        return self

    async def seen(self) -> dict:
        return (await self.split.runner.tasks[0].store.get("movie"))[SEEN]


async def test_split_emits_closed_days_in_full_and_the_open_day_as_news() -> None:
    pipeline = await Pipeline(dict(TWO_DAYS)).start(YESTERDAY)
    changes = await pipeline.poll()

    assert [(c["id"], c["changed_on"], c["listing"]) for c in changes] == [
        (1, "2026-10-22", "closed"), (2, "2026-10-22", "closed"), (3, "2026-10-22", "closed"),
        (4, "2026-10-22", "closed"), (5, "2026-10-22", "closed"),
        (5, "2026-10-23", "open"), (6, "2026-10-23", "open")]   # id 5 changed on both days
    keys = _keys(pipeline.split.runner.tasks[0].producer, CHANGES_TOPIC)
    assert keys == [f"movie:{c['id']}" for c in changes]
    assert {c["discovered_at"] for c in changes} == {"2026-10-23T12:00:00Z"}
    assert await pipeline.seen() == {TODAY.isoformat(): [5, 6]}  # the open day only


async def test_split_the_hourly_re_list_emits_only_the_news() -> None:
    pipeline = await Pipeline(dict(TWO_DAYS)).start(YESTERDAY)
    await pipeline.poll()
    assert await pipeline.poll() == []                           # same bodies: nothing new
    pipeline.tmdb.pages[(TODAY, 1)] = _page([5, 6, 8])
    assert [c["id"] for c in await pipeline.poll()] == [8]
    assert await pipeline.seen() == {TODAY.isoformat(): [5, 6, 8]}


async def test_split_a_closing_day_gets_one_full_re_list() -> None:
    pipeline = await Pipeline(dict(TWO_DAYS)).start(YESTERDAY)
    await pipeline.poll()
    pipeline.tmdb.clock["now"] = _NOON + timedelta(days=1)
    pipeline.tmdb.pages[(TODAY, 1)] = _page([5, 6, 7])          # 7 changed after the last hourly poll
    changes = await pipeline.poll()

    assert [(c["id"], c["changed_on"], c["listing"]) for c in changes] == [
        (5, "2026-10-23", "closed"), (6, "2026-10-23", "closed"), (7, "2026-10-23", "closed")]
    assert await pipeline.seen() == {(TODAY + timedelta(days=1)).isoformat(): []}


async def test_split_never_emits_an_adult_id() -> None:
    pipeline = await Pipeline({(TODAY, 1): _page([1, 2, 3], adult={2})}).start(TODAY)
    assert [c["id"] for c in await pipeline.poll()] == [1, 3]


# --- catalog: the TransformerRunner over the fakes ---

DETAILS = {"movie": MOVIE, "tv": TV, "person": PERSON}
DETAILS_DATE = datetime(2026, 10, 23, 10, 0, tzinfo=UTC)


def _catalog(status: int = 200, *, feeds: tuple[str, ...] = ("movie", "tv", "person")
             ) -> tuple[TmdbCatalog, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        headers = {"Date": format_datetime(DETAILS_DATE, usegmt=True)}
        if status != 200:
            return httpx.Response(status, json={"success": False, "status_code": 34}, headers=headers)
        feed = request.url.path.split("/")[2]
        return httpx.Response(200, json=DETAILS[feed], headers=headers)

    stage = TmdbCatalog(httpx.AsyncClient(transport=httpx.MockTransport(handle)), base_url=BASE)
    stage.configs = ConfigStore.of({feed: Config.wrap(_feed_record(feed)) for feed in feeds})
    return stage, requests


def _change(feed: str, tmdb_id: int, offset: int = 0):
    value = Event({FEED: feed, TMDB_ID: tmdb_id, CHANGED_ON: YESTERDAY, LISTING: "closed"}).raw
    return make_record(key=f"{feed}:{tmdb_id}", value=json.dumps(value), topic=CHANGES_TOPIC,
                       partition=0, offset=offset)


def _make_module(stage: Transformer, records: list) -> _FlechtwerkModule:
    module = _FlechtwerkModule()
    module.application_id = "tmdb-catalog"
    module.client_id = "tmdb-catalog"
    module.bootstrap_servers = "localhost:9092"
    module.metrics_labels = {}
    module.metrics_port = 0
    module.mqtt = None
    module.stage = stage
    module.consumer = FakeKafkaConsumer(records)
    module.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return module


async def _process(module: _FlechtwerkModule) -> None:
    await module.runner.process_batch(await module.runner.consumer.getmany(timeout_ms=1000))


async def test_catalog_emits_a_version_then_its_credits_then_the_state() -> None:
    stage, requests = _catalog()
    module = _make_module(stage, [_change("movie", 550)])
    await _process(module)
    producer = module.runner.tasks[0].producer

    movies = _sent(producer, MOVIES_TOPIC)
    assert len(movies) == 1
    movie = movies[0]
    assert movie["media_type"] == "movie" and movie["version"] == 1
    assert movie["title"] == "Fight Club" and movie["id"] == 550
    assert movie["version_at"] == "2026-10-23T10:00:00Z"        # the response's Date header
    assert movie["changed_on"] == YESTERDAY.isoformat()
    assert len(movie["content_hash"]) == 16 and "credits" not in movie
    assert "Drama" in movie["genres"] and movie["certifications"]["US"] == "R"

    credits = _sent(producer, CREDITS_TOPIC)
    cast, crew = MOVIE["credits"]["cast"], MOVIE["credits"]["crew"]
    assert len(credits) == len(cast) + len(crew)
    assert all(c["media_id"] == 550 and c["media_type"] == "movie" and c["version"] == 1
               for c in credits)
    assert set(_keys(producer, MOVIES_TOPIC) + _keys(producer, CREDITS_TOPIC)) == {"movie:550"}
    assert [t for t, _ in producer.sent][0] == MOVIES_TOPIC       # the version precedes its credits

    state = await module.runner.tasks[0].store.get("movie:550")
    assert state[CONTENT_HASH] == movie["content_hash"] and state[VERSION] == 1
    assert all(r.headers["Authorization"] == f"Bearer {TOKEN}" for r in requests)
    assert requests[0].url.path == "/3/movie/550"
    assert requests[0].url.params["append_to_response"].startswith("credits,")


async def test_catalog_replay_is_gated_by_the_content_hash() -> None:
    stage, requests = _catalog()
    module = _make_module(stage, [_change("movie", 550, 0), _change("movie", 550, 1)])
    await _process(module)
    producer = module.runner.tasks[0].producer

    assert len(requests) == 2                                    # fetched twice...
    assert len(_sent(producer, MOVIES_TOPIC)) == 1               # ...versioned once
    credits = len(MOVIE["credits"]["cast"]) + len(MOVIE["credits"]["crew"])
    assert len(_sent(producer, CREDITS_TOPIC)) == credits
    assert (await module.runner.tasks[0].store.get("movie:550"))[VERSION] == 1


async def test_catalog_new_content_is_the_next_version() -> None:
    stage, _ = _catalog()
    module = _make_module(stage, [_change("movie", 550)])
    await module.runner.tasks[0].store.put("movie:550", State({CONTENT_HASH: "0" * 16, VERSION: 4}))
    await _process(module)
    movie = _sent(module.runner.tasks[0].producer, MOVIES_TOPIC)[0]
    assert movie["version"] == 5


async def test_catalog_tv_yields_the_aggregate_credits() -> None:
    stage, _ = _catalog()
    module = _make_module(stage, [_change("tv", 87108)])
    await _process(module)
    producer = module.runner.tasks[0].producer

    series = _sent(producer, TV_TOPIC)
    assert len(series) == 1 and series[0]["name"] == "Chernobyl" and "seasons" not in series[0]
    aggregate = TV["aggregate_credits"]
    expected = (sum(len(c["roles"]) for c in aggregate["cast"])
                + sum(len(c["jobs"]) for c in aggregate["crew"]))
    credits = _sent(producer, CREDITS_TOPIC)
    assert len(credits) == expected and {c["media_type"] for c in credits} == {"tv"}


async def test_catalog_person_yields_no_credits() -> None:
    stage, _ = _catalog()
    module = _make_module(stage, [_change("person", 287)])
    await _process(module)
    producer = module.runner.tasks[0].producer
    assert [p["name"] for p in _sent(producer, PEOPLE_TOPIC)] == ["Brad Pitt"]
    assert _sent(producer, CREDITS_TOPIC) == []


async def test_catalog_a_404_tombstones_the_state_and_emits_nothing() -> None:
    stage, _ = _catalog(404)
    module = _make_module(stage, [_change("movie", 550)])
    store = module.runner.tasks[0].store
    await store.put("movie:550", State({CONTENT_HASH: "0" * 16, VERSION: 2}))
    await _process(module)
    assert module.runner.tasks[0].producer.sent == []
    assert await store.get("movie:550") is None


async def test_catalog_a_404_for_an_unknown_entity_writes_nothing() -> None:
    stage, _ = _catalog(404)
    module = _make_module(stage, [_change("movie", 550)])
    await _process(module)
    assert module.runner.tasks[0].producer.sent == []
    assert await module.runner.tasks[0].store.get("movie:550") is None


async def test_catalog_a_500_crashes_the_batch() -> None:
    stage, _ = _catalog(500)
    module = _make_module(stage, [_change("movie", 550)])
    with pytest.raises(httpx.HTTPStatusError):
        await _process(module)


async def test_catalog_a_401_names_the_remedy() -> None:
    stage, _ = _catalog(401)
    module = _make_module(stage, [_change("movie", 550)])
    with pytest.raises(RuntimeError, match="request-tmdb token"):
        await _process(module)


async def test_catalog_skips_a_feed_with_no_config_record(caplog: pytest.LogCaptureFixture) -> None:
    stage, requests = _catalog(feeds=("tv",))
    module = _make_module(stage, [_change("movie", 550)])
    with caplog.at_level(logging.WARNING):
        await _process(module)
    assert requests == [] and module.runner.tasks[0].producer.sent == []
    assert "no config record for feed 'movie'" in caplog.text and "request-tmdb token" in caplog.text

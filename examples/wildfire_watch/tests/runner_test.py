"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker, no
network — with FIRMS and Nominatim served from the committed fixtures via an
``httpx.MockTransport`` whose ``Date`` header (the event-time clock) the test controls:

- **ingest** drives the real ``ExtractorRunner``/``poll_one``: one poll fetches *both* sources,
  emits every new detection followed by exactly one ``sweep`` marker (last, always, even on a
  quiet poll), and persists the seen-set; a second poll against a superset body emits only the
  genuinely new rows. ``enrich_config`` geocodes a name-only region into a padded bounding box.
  A 503 and a 200-with-a-non-CSV-body both crash the poll (let it crash).
- **tracker** drives ``TransformerRunner.process_batch`` over a detections-then-sweep batch:
  clustering accumulates in the join state, a sweep heartbeats it, and a later sweep past the
  timeout extinguishes the fire and tombstones the region's bucket.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest

from flechtwerk import Config, Event
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.wildfire_watch.attributes import (
    ACQUIRED_AT,
    CONFIDENCE,
    DAYNIGHT,
    DETECTION_ID,
    DETECTIONS_TOPIC,
    EAST,
    EVENTS_TOPIC,
    FETCHED_AT,
    FIRES,
    FRP,
    INSTRUMENT,
    KIND,
    LAT,
    LON,
    NAME,
    NEW_DETECTIONS,
    NORTH,
    REGION,
    SATELLITE,
    SCAN,
    SEEN,
    SOUTH,
    STATUS_TOPIC,
    SWEEP_AT,
    TRACK,
    WEST,
)
from examples.wildfire_watch.geocoding import NominatimGeocoder
from examples.wildfire_watch.ingest import PAD_DEG, SOURCES, FirmsIngest
from examples.wildfire_watch.tracker import ACTIVE, EXTINGUISHED, IGNITION, tracker
from examples.wildfire_watch.tracking import EXTINGUISH_AFTER

FIXTURES = Path(__file__).parent / "fixtures"
N20_CSV = (FIXTURES / "firms_n20.csv").read_text()
N20_LATER_CSV = (FIXTURES / "firms_n20_later.csv").read_text()
N21_CSV = (FIXTURES / "firms_n21.csv").read_text()
ERROR_BODY = (FIXTURES / "firms_error.txt").read_text()
NOMINATIM_HIT = json.loads((FIXTURES / "nominatim_region.json").read_text())

HEADER_ONLY = N20_CSV.splitlines()[0] + "\n"

UTC = timezone.utc
REGION_SLUG = "alentejo-portugal"
REGION_NAME = "Alentejo, Portugal"
_T = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)

FIRMS_BASE = "http://firms.test"
NOMINATIM_SEARCH = "http://nominatim.test/search"
MAP_KEY = "test-map-key"

BBOX = {"west": -8.9, "south": 36.9, "east": -6.8, "north": 39.2}
CONFIG_WITH_BBOX = {"region": REGION_SLUG, "name": REGION_NAME, **BBOX}
CONFIG_NAME_ONLY = {"region": REGION_SLUG, "name": REGION_NAME}

# Real detection counts in the fixtures — see fixtures/PROVENANCE.md.
N20_ROWS, N20_LATER_ROWS, N21_ROWS = 21, 32, 24


# --- extractor harness (mirrors the odds / SMARD / GTFS runner tests) ---

def _extractor_runner(stage: Extractor, key: str, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "wildfire-changelog"
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


def _handler(clock: dict, bodies: dict, paths: list[str], *, status: int = 200):
    """A MockTransport handler serving FIRMS per source and Nominatim, stamping each response
    with the ``Date`` the test currently wants (the event-time clock) and recording every path."""
    def handle(request: httpx.Request) -> httpx.Response:
        headers = {"Date": format_datetime(clock["date"], usegmt=True)}
        path = str(request.url)
        paths.append(path)
        if "/search" in path:
            return httpx.Response(200, json=NOMINATIM_HIT, headers=headers)
        for source in SOURCES:
            if source in path:
                return httpx.Response(status, text=bodies[source], headers=headers)
        return httpx.Response(404, headers=headers)   # pragma: no cover — unreached
    return handle


def _stage(clock: dict, *, n20: str = N20_CSV, n21: str = N21_CSV,
           status: int = 200, geocode: bool = False) -> tuple[FirmsIngest, dict, list[str]]:
    """A ``FirmsIngest`` wired to the mock transport. Returns the stage, the mutable body map
    (so a test can change what the *next* poll sees), and the recorded request paths."""
    bodies = {SOURCES[0]: n20, SOURCES[1]: n21}
    paths: list[str] = []
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler(clock, bodies, paths, status=status)))
    geocoder = NominatimGeocoder(client=client, search_url=NOMINATIM_SEARCH) if geocode else None
    stage = FirmsIngest(client=client, map_key=MAP_KEY, base_url=FIRMS_BASE,
                        geocoder=geocoder)
    return stage, bodies, paths


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(payload["value"]) for sent_topic, payload in producer.sent if sent_topic == topic]


def _of_kind(producer: FakeKafkaProducer, topic: str, kind: str) -> list[dict]:
    return [record for record in _sent(producer, topic) if record["kind"] == kind]


async def _poll(stage: Extractor, config: dict) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    runner, producer = _extractor_runner(stage, REGION_SLUG, config)
    await runner.load_initial_configs()
    return runner, producer


# --- ingest ---

async def test_ingest_emits_every_detection_then_exactly_one_sweep() -> None:
    stage, _, _ = _stage({"date": _T})
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])

    records = _sent(producer, DETECTIONS_TOPIC)
    detections = [r for r in records if r["kind"] == "detection"]
    assert len(detections) == N20_ROWS + N21_ROWS            # both satellites' rows are distinct
    assert records[-1]["kind"] == "sweep"                    # the marker is LAST — the commit order
    assert [r["kind"] for r in records].count("sweep") == 1
    assert records[-1]["new_detections"] == N20_ROWS + N21_ROWS
    assert records[-1]["sweep_at"] == "2026-07-25T18:00:00Z"
    keys = {payload["key"].decode() for topic, payload in producer.sent if topic == DETECTIONS_TOPIC}
    assert keys == {REGION_SLUG}                             # everything keyed by the region


async def test_ingest_detections_are_sorted_by_event_time() -> None:
    stage, _, _ = _stage({"date": _T})
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])
    detections = _of_kind(producer, DETECTIONS_TOPIC, "detection")
    acquired = [r["acquired_at"] for r in detections]
    assert acquired == sorted(acquired)                      # deterministic replay order


async def test_ingest_persists_the_seen_set_bucketed_by_date() -> None:
    stage, _, _ = _stage({"date": _T})
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])

    state = await runner.tasks[0].store.get(REGION_SLUG)
    seen = state[SEEN]
    assert set(seen) == {"2026-07-24", "2026-07-25"}         # both acquisition dates present
    assert sum(len(ids) for ids in seen.values()) == N20_ROWS + N21_ROWS
    emitted = {r["detection_id"] for r in _of_kind(producer, DETECTIONS_TOPIC, "detection")}
    assert emitted == {identity for ids in seen.values() for identity in ids}


async def test_ingest_second_poll_emits_only_the_new_rows() -> None:
    clock = {"date": _T}
    stage, bodies, _ = _stage(clock)
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])
        first_count = len(_of_kind(producer, DETECTIONS_TOPIC, "detection"))

        # The later poll's view: NOAA-20 now returns a genuine superset, NOAA-21 is unchanged.
        producer.sent.clear()
        bodies[SOURCES[0]] = N20_LATER_CSV
        clock["date"] = _T + timedelta(minutes=5)
        await runner.poll_one(runner.entries[REGION_SLUG])

    assert first_count == N20_ROWS + N21_ROWS
    new = _of_kind(producer, DETECTIONS_TOPIC, "detection")
    assert len(new) == N20_LATER_ROWS - N20_ROWS             # 11 genuinely new rows, no duplicates
    sweep = _of_kind(producer, DETECTIONS_TOPIC, "sweep")
    assert len(sweep) == 1 and sweep[0]["new_detections"] == len(new)
    assert sweep[0]["sweep_at"] == "2026-07-25T18:05:00Z"    # the clock advanced

    state = await runner.tasks[0].store.get(REGION_SLUG)
    assert sum(len(ids) for ids in state[SEEN].values()) == N20_LATER_ROWS + N21_ROWS


async def test_ingest_repolling_identical_bodies_emits_only_a_sweep() -> None:
    clock = {"date": _T}
    stage, _, _ = _stage(clock)
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])
        producer.sent.clear()
        clock["date"] = _T + timedelta(minutes=5)
        await runner.poll_one(runner.entries[REGION_SLUG])

    records = _sent(producer, DETECTIONS_TOPIC)
    assert [r["kind"] for r in records] == ["sweep"]         # nothing new, but the clock still beats
    assert records[0]["new_detections"] == 0


async def test_ingest_quiet_region_still_emits_the_sweep() -> None:
    stage, _, _ = _stage({"date": _T}, n20=HEADER_ONLY, n21=HEADER_ONLY)
    runner, producer = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])

    records = _sent(producer, DETECTIONS_TOPIC)
    assert [r["kind"] for r in records] == ["sweep"]
    assert records[0]["new_detections"] == 0
    # Nothing to remember, so no seen-set is written — a quiet region never churns the changelog.
    state = await runner.tasks[0].store.get(REGION_SLUG)
    assert state is not None and state[SEEN] == {}


async def test_ingest_polls_both_sources_with_the_configured_bbox() -> None:
    stage, _, paths = _stage({"date": _T})
    runner, _ = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])

    assert len(paths) == 2
    assert any(SOURCES[0] in p for p in paths) and any(SOURCES[1] in p for p in paths)
    for path in paths:
        assert f"/api/area/csv/{MAP_KEY}/" in path
        assert "-8.9,36.9,-6.8,39.2/2" in path               # west,south,east,north / DAY_RANGE


async def test_ingest_explicit_bbox_skips_geocoding() -> None:
    stage, _, paths = _stage({"date": _T}, geocode=True)
    runner, _ = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])
    assert not any("/search" in p for p in paths)            # Nominatim never touched


async def test_ingest_enrich_config_fills_a_padded_bbox_from_nominatim() -> None:
    stage, _, paths = _stage({"date": _T}, geocode=True)
    # The framework calls enrich_config while loading configs, before any poll.
    runner, _ = await _poll(stage, CONFIG_NAME_ONLY)
    assert sum("/search" in p for p in paths) == 1           # once per config, not per poll

    async with stage:
        await runner.poll_one(runner.entries[REGION_SLUG])
        await runner.poll_one(runner.entries[REGION_SLUG])
    assert sum("/search" in p for p in paths) == 1           # still once after two polls

    # The captured hit's boundingbox is [south, north, west, east] = 37.0551, 39.0551,
    # -8.8606, -6.8606; each edge is pushed out by PAD_DEG.
    area_paths = [p for p in paths if "/api/area/" in p]
    assert f"{-8.8605799 - PAD_DEG},{37.0551003 - PAD_DEG}," in area_paths[0]
    assert f"{-6.8605799 + PAD_DEG},{39.0551003 + PAD_DEG}/" in area_paths[0]


async def test_enrich_config_is_a_plain_async_method() -> None:
    stage, _, _ = _stage({"date": _T}, geocode=True)
    enriched = await stage.enrich_config(Config({REGION: REGION_SLUG, NAME: REGION_NAME}))
    assert enriched[WEST] == pytest.approx(-8.8605799 - PAD_DEG)
    assert enriched[SOUTH] == pytest.approx(37.0551003 - PAD_DEG)
    assert enriched[EAST] == pytest.approx(-6.8605799 + PAD_DEG)
    assert enriched[NORTH] == pytest.approx(39.0551003 + PAD_DEG)
    assert enriched[NAME] == REGION_NAME                     # the rest is carried through


async def test_enrich_config_returns_a_complete_config_untouched() -> None:
    stage, _, paths = _stage({"date": _T}, geocode=True)
    config = Config.wrap(CONFIG_WITH_BBOX)
    assert (await stage.enrich_config(config))[WEST] == BBOX["west"]
    assert paths == []


async def test_geocoder_resolve_reports_what_matched() -> None:
    # The identity request.py surfaces before writing anything — the line that tells a typo's
    # street or an overseas-spanning country from the region the operator meant.
    client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=NOMINATIM_HIT)))
    match = await NominatimGeocoder(client=client, search_url=NOMINATIM_SEARCH).resolve(REGION_NAME)
    assert match.display_name == "Região do Alentejo, Beja, 7800-246, Portugal"
    assert match.addresstype == "region"
    assert (match.south, match.north) == (37.0551003, 39.0551003)
    assert (match.west, match.east) == (-8.8605799, -6.8605799)


async def test_enrich_config_raises_on_a_name_that_matches_nothing() -> None:
    clock, paths = {"date": _T}, []
    bodies = {source: HEADER_ONLY for source in SOURCES}

    def handle(request: httpx.Request) -> httpx.Response:
        if "/search" in str(request.url):
            return httpx.Response(200, json=[])             # Nominatim: no match
        return _handler(clock, bodies, paths)(request)       # pragma: no cover — unreached

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    stage = FirmsIngest(client=client, map_key=MAP_KEY, base_url=FIRMS_BASE,
                        geocoder=NominatimGeocoder(client=client, search_url=NOMINATIM_SEARCH))
    with pytest.raises(LookupError, match="no match for region"):
        await stage.enrich_config(Config.wrap(CONFIG_NAME_ONLY))


# --- let it crash ---

async def test_ingest_propagates_an_http_error() -> None:
    stage, _, _ = _stage({"date": _T}, status=503)
    runner, _ = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        with pytest.raises(httpx.HTTPStatusError):
            await runner.poll_one(runner.entries[REGION_SLUG])


async def test_ingest_raises_loudly_on_a_non_csv_body_served_as_200() -> None:
    # The failure mode the header guard exists for: a quota notice or maintenance page with a
    # 200 status would otherwise parse as "no fires".
    stage, _, _ = _stage({"date": _T}, n20=ERROR_BODY)
    runner, _ = await _poll(stage, CONFIG_WITH_BBOX)
    async with stage:
        with pytest.raises(RuntimeError, match="FIRMS did not return area CSV"):
            await runner.poll_one(runner.entries[REGION_SLUG])


# --- tracker: TransformerRunner.process_batch over a real batch ---

def _make_module(records: list) -> _FlechtwerkModule:
    module = _FlechtwerkModule()
    module.application_id = "wildfire-tracker"
    module.client_id = "wildfire-tracker"
    module.bootstrap_servers = "localhost:9092"
    module.metrics_labels = {}
    module.metrics_port = 0
    module.mqtt = None
    module.stage = tracker
    module.consumer = FakeKafkaConsumer(records)
    module.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return module


async def _process(module: _FlechtwerkModule) -> None:
    await module.runner.process_batch(await module.runner.consumer.getmany(timeout_ms=1000))


def _detection(identity: str, lat: float, lon: float, *, acquired: datetime = _T,
               frp: float = 10.0) -> str:
    return json.dumps(Event({
        KIND: "detection", REGION: REGION_SLUG, DETECTION_ID: identity,
        LAT: lat, LON: lon, ACQUIRED_AT: acquired, SATELLITE: "N20",
        INSTRUMENT: "VIIRS", CONFIDENCE: "n", SCAN: 0.4, TRACK: 0.4,
        DAYNIGHT: "D", FETCHED_AT: acquired, FRP: frp,
    }).raw)


def _sweep(sweep_at: datetime, new_detections: int = 0) -> str:
    return json.dumps(Event({KIND: "sweep", REGION: REGION_SLUG,
                             SWEEP_AT: sweep_at, NEW_DETECTIONS: new_detections}).raw)


async def test_tracker_clusters_a_batch_then_heartbeats_on_the_sweep() -> None:
    # One batch, in the order ingest produces it: detections, then the sweep. Same key → the
    # framework processes them serially, each seeing the previous one's state.
    module = _make_module([
        make_record(key=REGION_SLUG, value=_detection("aaa", 37.0, -7.0, frp=42.0),
                    topic=DETECTIONS_TOPIC, offset=0),
        make_record(key=REGION_SLUG, value=_detection("bbb", 37.0045, -7.0),
                    topic=DETECTIONS_TOPIC, offset=1),
        make_record(key=REGION_SLUG, value=_sweep(_T + timedelta(minutes=5), new_detections=2),
                    topic=DETECTIONS_TOPIC, offset=2),
    ])
    await _process(module)
    producer = module.runner.tasks[0].producer

    events = _sent(producer, EVENTS_TOPIC)
    assert len(events) == 1 and events[0]["kind"] == IGNITION      # the second pixel just joined
    assert events[0]["fire_id"] == "F-aaa"

    statuses = _sent(producer, STATUS_TOPIC)
    assert len(statuses) == 1                                     # one active fire, one heartbeat
    assert statuses[0]["status"] == ACTIVE and statuses[0]["detections"] == 2
    assert statuses[0]["as_of"] == "2026-07-25T18:05:00Z"
    assert statuses[0]["frp_sum"] == 52.0 and statuses[0]["frp_max"] == 42.0

    stored = await module.runner.tasks[0].store.get(REGION_SLUG)   # a State → Attribute keys
    assert stored is not None and set(stored[FIRES]) == {"F-aaa"}
    assert stored[FIRES]["F-aaa"]["count"] == 2


async def test_tracker_late_sweep_extinguishes_and_tombstones() -> None:
    module = _make_module([
        make_record(key=REGION_SLUG, value=_detection("aaa", 37.0, -7.0),
                    topic=DETECTIONS_TOPIC, offset=0),
    ])
    await _process(module)
    assert await module.runner.tasks[0].store.get(REGION_SLUG) is not None

    producer = module.runner.tasks[0].producer
    producer.sent.clear()
    module.consumer.records = [make_record(
        key=REGION_SLUG, value=_sweep(_T + EXTINGUISH_AFTER + timedelta(minutes=1)),
        topic=DETECTIONS_TOPIC, offset=1)]
    await _process(module)

    events = _sent(producer, EVENTS_TOPIC)
    assert len(events) == 1 and events[0]["kind"] == EXTINGUISHED
    assert events[0]["detections"] == 1 and events[0]["first_seen"] == "2026-07-25T18:00:00Z"
    statuses = _sent(producer, STATUS_TOPIC)
    assert len(statuses) == 1 and statuses[0]["status"] == EXTINGUISHED
    # The region's last fire is out → the whole bucket goes.
    assert await module.runner.tasks[0].store.get(REGION_SLUG) is None


async def test_tracker_sweep_on_an_unknown_region_is_a_noop() -> None:
    module = _make_module([make_record(key=REGION_SLUG, value=_sweep(_T),
                                       topic=DETECTIONS_TOPIC, offset=0)])
    await _process(module)
    producer = module.runner.tasks[0].producer
    assert producer.sent == []
    assert await module.runner.tasks[0].store.get(REGION_SLUG) is None

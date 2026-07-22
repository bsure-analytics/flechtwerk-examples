"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles
(``FakeKafkaConsumer``/``FakeKafkaProducer``, ``InMemoryStateStore``) — no broker,
no network, no live enrichment services:

- **ingest** drives the real ``ExtractorRunner``/``poll_one`` with a stubbed HTTP
  transport, pinning that one whole ``adsb-raw`` record (provenance + preserved
  ``ac[]``) is produced per poll;
- **enrich** drives ``TransformerRunner.process_batch`` with a *fake* ``Enricher``,
  pinning the enriched fan-out **and** the headline showcase — the enrichment
  cache, restored from state, serves a second batch with zero new lookups;
- **conflict** drives ``process_batch`` on cell-keyed records, pinning that a
  near-miss fires once and is not re-announced.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from flechtwerk import Config, State
from flechtwerk.attribute import Record
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.adsb_flight_tracker.attributes import (
    AIRCRAFT_TYPE_NAME,
    AIRLINE,
    AIRLINE_WIKI,
    LAT,
    LON,
    NAME,
    NEAREST_PLACE,
    OVER_COUNTRY,
    RADIUS,
    TYPE_WIKI,
)
from examples.adsb_flight_tracker.attributes import CHECKED_AT, ISO3
from examples.adsb_flight_tracker.geocoding import NominatimGeocoder
from examples.adsb_flight_tracker.conflict import conflict
from examples.adsb_flight_tracker.boundaries import (
    BOUNDARY_TABLE,
    COUNTRIES_TOPIC,
    WORLD_DICT,
    WORLD_TABLE,
    CountryLoader,
    region_dict,
)
from examples.adsb_flight_tracker.enrich import (
    _Backoff,
    AIRCRAFT_TOPIC,
    CELLS_TOPIC,
    EVENTS_TOPIC,
    LIVE_LOOKUPS_PER_POLL,
    AdsbEnrich,
    WikidataClickHouseEnricher,
    cell_key,
)
from examples.adsb_flight_tracker.ingest import CONFIG_TOPIC, DEFAULT_RADIUS, RAW_TOPIC, AdsbIngest

REGION = {"name": "london", "lat": 51.47, "lon": -0.45, "radius": 100}


# --- ingest: the real ExtractorRunner over the shipped fakes ---

def _stub_client(*payloads: dict) -> httpx.AsyncClient:
    """An httpx client whose successive GETs return the given adsb.lol payloads."""
    responses = iter(payloads)
    return httpx.AsyncClient(
        base_url="https://api.adsb.lol",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=next(responses))),
    )


def _extractor_runner(stage: AdsbIngest) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes (see example 1's
    original tracker test — the wiring mirrors the framework's own extractor tests)."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "adsb-ingest-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer([make_record(key=REGION["name"], value=json.dumps(REGION), topic=CONFIG_TOPIC)])
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


async def test_ingest_wraps_the_whole_response_onto_adsb_raw() -> None:
    stage = AdsbIngest()
    stage.client = _stub_client(
        {"now": 1_700_000_000_000, "total": 2, "ac": [
            {"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 30000},
            {"hex": "def456", "lat": 51.4, "lon": -0.5, "alt_baro": "ground"},
        ]},
    )
    runner, producer = _extractor_runner(stage)
    await runner.load_initial_configs()

    await runner.poll_one(runner.entries["london"])

    assert len(producer.sent) == 1  # one raw record per poll
    topic, payload = producer.sent[0]
    assert topic == RAW_TOPIC
    assert payload["key"] == b"london"
    value = json.loads(payload["value"])
    assert value["config"]["name"] == "london"  # config nested in its own namespace
    assert "fetched_at" in value["metadata"] and "fetch_duration" in value["metadata"]  # provenance
    response = value["response"]  # the feed response nested verbatim, un-collided
    assert [a["hex"] for a in response["ac"]] == ["abc123", "def456"]  # ac[] preserved
    assert response["total"] == 2  # and so is every other field the feed sent


# --- ingest: enrich_config forward-geocodes a name-only region (Nominatim /search) ---

def _stub_geocoder(*hits: list) -> NominatimGeocoder:
    """A real ``NominatimGeocoder`` whose successive ``/search`` calls return the given
    Nominatim result lists — the framework's stub-the-external-client idiom (MockTransport)."""
    responses = iter(hits)
    return NominatimGeocoder(
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=next(responses)))),
    )


async def test_enrich_config_forward_geocodes_a_name_only_region() -> None:
    # The magic: drop just a name and the stage resolves the coordinates the poll needs.
    stage = AdsbIngest()
    stage.geocoder = _stub_geocoder([{"lat": "51.5074", "lon": "-0.1278", "display_name": "London, England"}])

    config = await stage.enrich_config(Config({NAME: "London"}))

    assert round(config[LAT], 2) == 51.51 and round(config[LON], 2) == -0.13  # resolved centre (strings → float)
    assert config[RADIUS] == DEFAULT_RADIUS  # and the radius is still defaulted + clamped afterwards


async def test_enrich_config_does_not_geocode_when_coordinates_are_given() -> None:
    # An explicit position must never touch the network — the transport raises if called.
    def _boom(request: httpx.Request) -> httpx.Response:
        raise AssertionError("geocoder must not be called when lat/lon are present")

    stage = AdsbIngest()
    stage.geocoder = NominatimGeocoder(client=httpx.AsyncClient(transport=httpx.MockTransport(_boom)))

    config = await stage.enrich_config(Config({NAME: "london", LAT: 51.47, LON: -0.45}))
    assert (config[LAT], config[LON]) == (51.47, -0.45)


async def test_enrich_config_raises_on_a_region_that_matches_nothing() -> None:
    # A name Nominatim can't place is a config error, not a silent skip ("let it crash").
    stage = AdsbIngest()
    stage.geocoder = _stub_geocoder([])  # empty search result
    with pytest.raises(LookupError):
        await stage.enrich_config(Config({NAME: "Nowhere-at-all"}))


# --- boundaries: the loader loads the world map at startup + a country's fine map on request ---

WORLD_TEST_URL = "https://world.test/adm0.geojson"


def _loader_client(statements: list[str]) -> httpx.AsyncClient:
    """Stub the loader's HTTP surfaces over a MockTransport: the world ADM0 file (two
    countries), the geoBoundaries per-country metadata + GeoJSON (Germany publishes ADM1-3
    but not ADM4/ADM5), and ClickHouse (POST → recorded; every freshness ``SELECT`` answers
    'not fresh' so both maps load)."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            url = str(request.url)
            if url == WORLD_TEST_URL:
                return httpx.Response(200, json={"type": "FeatureCollection", "features": [
                    {"properties": {"NAME": "Germany", "ADM0_A3": "DEU"},
                     "geometry": {"type": "Polygon", "coordinates": [[[6, 50], [8, 50], [8, 52], [6, 52], [6, 50]]]}},
                    {"properties": {"NAME": "France", "ADM0_A3": "FRA"},
                     "geometry": {"type": "Polygon", "coordinates": [[[2, 47], [4, 47], [4, 49], [2, 49], [2, 47]]]}}]})
            if url.endswith("/DEU/ADM4/") or url.endswith("/DEU/ADM5/"):
                return httpx.Response(404)                    # Germany has no ADM4/ADM5
            if "/DEU/ADM" in url and url.endswith("/"):       # ADM1/ADM2/ADM3 metadata
                return httpx.Response(200, json={"gjDownloadURL": "https://gb.test/deu.geojson"})
            return httpx.Response(200, json={"type": "FeatureCollection", "features": [
                {"properties": {"shapeName": "Essen"},
                 "geometry": {"type": "Polygon", "coordinates": [[[6.9, 51.4], [7.1, 51.4], [7.1, 51.5], [6.9, 51.4]]]}}]})
        statements.append(request.content.decode())
        return httpx.Response(200, text="0\n")  # ClickHouse freshness SELECT → not fresh (load)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _loader(statements: list[str]) -> CountryLoader:
    return CountryLoader(client=_loader_client(statements), clickhouse_url="http://clickhouse.test/",
                         api_base="https://gb.test", world_url=WORLD_TEST_URL,
                         now=lambda: datetime(2026, 7, 19, tzinfo=timezone.utc))


async def test_country_loader_loads_world_at_startup_and_a_country_on_request() -> None:
    statements: list[str] = []
    async with _loader(statements) as loader:            # __aenter__ loads the world map
        items = [item async for item in loader.poll(Config({ISO3: "DEU"}), State())]

    # world map loaded at startup
    assert any(f"TRUNCATE TABLE {WORLD_TABLE}" in s for s in statements)
    assert any(f"INSERT INTO {WORLD_TABLE}" in s and "Germany" in s for s in statements)
    assert any(f"SYSTEM RELOAD DICTIONARY {WORLD_DICT}" in s for s in statements)
    # requested country's maps loaded on demand — all its levels (ADM1-3; it has no ADM4/5)
    assert any(f"INSERT INTO {BOUNDARY_TABLE}" in s and "Essen" in s for s in statements)
    assert any(f"SYSTEM RELOAD DICTIONARY {region_dict('ADM1')}" in s for s in statements)
    assert any(f"SYSTEM RELOAD DICTIONARY {region_dict('ADM3')}" in s for s in statements)
    assert not any(f"SYSTEM RELOAD DICTIONARY {region_dict('ADM4')}" in s for s in statements)  # not published
    states = [item for item in items if isinstance(item, State)]
    assert len(states) == 1 and states[0][CHECKED_AT] is not None  # a State page records the check


async def test_country_loader_is_a_noop_within_the_check_interval() -> None:
    # State says this country was just confirmed → the poll short-circuits with no ClickHouse
    # traffic (the timer keeps most polls free). The world load in __aenter__ is separate.
    statements: list[str] = []
    async with _loader(statements) as loader:
        baseline = len(statements)
        state = State({CHECKED_AT: datetime(2026, 7, 19, tzinfo=timezone.utc)})
        items = [item async for item in loader.poll(Config({ISO3: "DEU"}), state)]

    assert items == []
    assert len(statements) == baseline  # poll issued no ClickHouse work


async def test_country_loader_skips_a_country_with_no_geoboundaries_map() -> None:
    # A country the world map names but geoBoundaries has no map for (every level 404) is
    # skipped, not crashed — the poll commits its State (marks it checked) and loads nothing.
    statements: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            if str(request.url) == WORLD_TEST_URL:
                return httpx.Response(200, json={"type": "FeatureCollection", "features": []})
            return httpx.Response(404)  # no metadata at any admin level for this ISO-3
        statements.append(request.content.decode())
        return httpx.Response(200, text="0\n")

    loader = CountryLoader(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                           clickhouse_url="http://clickhouse.test/", api_base="https://gb.test",
                           world_url=WORLD_TEST_URL, now=lambda: datetime(2026, 7, 19, tzinfo=timezone.utc))
    async with loader:
        items = [item async for item in loader.poll(Config({ISO3: "XYZ"}), State())]

    assert not any(f"INSERT INTO {BOUNDARY_TABLE}" in s for s in statements)  # nothing loaded
    states = [item for item in items if isinstance(item, State)]
    assert len(states) == 1 and states[0][CHECKED_AT] is not None            # committed, no crash


def _loader_runner(loader: CountryLoader) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes for the loader — the same
    harness as the ingest test, seeding one adsb-countries request (an ISO-3 code)."""
    producer = FakeKafkaProducer()
    inner = InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "adsb-boundaries-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer([make_record(
        key="DEU", value=json.dumps({"iso3": "DEU"}), topic=COUNTRIES_TOPIC)])
    runner.create_restore_consumer = lambda: FakeKafkaConsumer()
    runner.create_token_producer = lambda token: producer
    runner.extractor = loader
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


async def test_country_loader_runs_through_the_extractor_runner() -> None:
    # Drive the loader through the REAL ExtractorRunner over the shipped fakes: prove a
    # State-only poll (the loader emits no data message — its product is the dictionaries)
    # commits cleanly and the load side effects fire (poll's _ensure_world loads the world too).
    statements: list[str] = []
    runner, producer = _loader_runner(_loader(statements))
    await runner.load_initial_configs()

    await runner.poll_one(runner.entries["DEU"])

    assert producer.sent == []                                              # no data message emitted
    assert any("SYSTEM RELOAD DICTIONARY " in s and "adsb_region_adm" in s for s in statements)  # a country map loaded
    assert await runner.tasks[0].store.get("DEU") is not None              # the State page committed


# --- enrich: TransformerRunner.process_batch with a fake Enricher ---

class FakeEnricher:
    """Records every call, so a test can prove the cache spared a second lookup."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def airline(self, icao: str) -> Record:
        self.calls.append(("airline", icao))
        return Record({AIRLINE: f"Airline {icao}", AIRLINE_WIKI: f"https://en.wikipedia.org/wiki/{icao}"})

    async def aircraft_type(self, designator: str) -> Record:
        self.calls.append(("aircraft_type", designator))
        return Record({AIRCRAFT_TYPE_NAME: f"Type {designator}", TYPE_WIKI: f"https://en.wikipedia.org/wiki/{designator}"})

    async def geocode(self, points: list[tuple[float, float]]) -> list[Record]:
        self.calls.extend(("geocode", round(lat, 2), round(lon, 2)) for lat, lon in points)
        return [Record({OVER_COUNTRY: "United Kingdom", ISO3: "GBR", NEAREST_PLACE: "London"}) for _ in points]


def _make_module(stage, records: list) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "adsb-enrich"
    mod.client_id = "adsb-enrich"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = stage
    mod.consumer = FakeKafkaConsumer(records)
    mod.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return mod


def _raw_record(aircraft: list[dict], *, now: int = 1_700_000_000_000, offset: int = 0):
    value = {"response": {"now": now, "ac": aircraft}, "config": {"name": "london"}}
    return make_record(key="london", value=json.dumps(value), topic=RAW_TOPIC, offset=offset)


def _by_topic(producer: FakeKafkaProducer) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for topic, payload in producer.sent:
        out.setdefault(topic, []).append(json.loads(payload["value"]))
    return out


async def test_enrich_produces_enriched_positions_cells_and_events() -> None:
    stage = AdsbEnrich(enricher=FakeEnricher())
    aircraft = [{"hex": "abc123", "flight": "BAW123  ", "lat": 51.5, "lon": -0.4,
                 "alt_baro": 10000, "t": "A320", "squawk": "7700"}]
    mod = _make_module(stage, [_raw_record(aircraft)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    sent = _by_topic(producer)
    position = sent[AIRCRAFT_TOPIC][0]
    assert position["airline"] == "Airline BAW"          # live-looked-up, applied
    assert position["aircraft_type_name"] == "Type A320"
    assert position["over_country"] == "United Kingdom"
    assert position["emergency"] == 1
    assert sent[CELLS_TOPIC][0]["cell"] == cell_key(51.5, -0.4)   # fan-out for the self-join
    assert sent[EVENTS_TOPIC][0]["event_type"] == "emergency"     # derived event


async def test_enrichment_cache_survives_a_restart_and_spares_the_second_lookup() -> None:
    fake = FakeEnricher()
    stage = AdsbEnrich(enricher=fake)
    aircraft = [{"hex": "abc123", "flight": "BAW123  ", "lat": 51.5, "lon": -0.4, "alt_baro": 30000, "t": "A320"}]
    mod = _make_module(stage, [_raw_record(aircraft, offset=0)])
    runner = mod.runner

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert len(fake.calls) == 3  # airline + aircraft_type + geocode, once each

    # A second batch on the SAME task store: the airline/type caches are restored from
    # state, so those are not looked up again (the headline showcase). Geocoding is not
    # cached — it reruns each poll from the aircraft's exact position — so it happens again.
    runner.consumer = FakeKafkaConsumer([_raw_record(aircraft, offset=1)])
    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert sum(1 for call in fake.calls if call[0] == "airline") == 1        # cached — not re-run
    assert sum(1 for call in fake.calls if call[0] == "aircraft_type") == 1  # cached — not re-run
    assert sum(1 for call in fake.calls if call[0] == "geocode") == 2        # not cached — re-run


async def test_empty_geocode_reruns_and_is_not_applied() -> None:
    # Geocoding isn't cached at all — an empty result (no boundary covers the point) is
    # simply not applied, and the next poll geocodes the position again from scratch.
    class EmptyGeocoder(FakeEnricher):
        async def geocode(self, points: list[tuple[float, float]]) -> list[Record]:
            self.calls.extend(("geocode", round(lat, 2), round(lon, 2)) for lat, lon in points)
            return [Record() for _ in points]  # no boundary covers these points yet

    stage = AdsbEnrich(enricher=(fake := EmptyGeocoder()))
    aircraft = [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 30000}]  # geocode-only
    mod = _make_module(stage, [_raw_record(aircraft, offset=0)])
    runner = mod.runner

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert sum(1 for call in fake.calls if call[0] == "geocode") == 1

    # A second batch on the SAME task store: the empty geocode was not cached, so it reruns.
    runner.consumer = FakeKafkaConsumer([_raw_record(aircraft, offset=1)])
    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert sum(1 for call in fake.calls if call[0] == "geocode") == 2  # retried, not spared


async def test_enrichment_lookup_failure_is_best_effort() -> None:
    class FlakyEnricher(FakeEnricher):
        async def airline(self, icao: str) -> dict:
            raise httpx.ConnectError("wikidata unreachable")

    stage = AdsbEnrich(enricher=FlakyEnricher())
    aircraft = [{"hex": "abc123", "flight": "BAW123  ", "lat": 51.5, "lon": -0.4, "alt_baro": 30000}]
    mod = _make_module(stage, [_raw_record(aircraft)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    position = _by_topic(producer)[AIRCRAFT_TOPIC][0]
    assert "airline" not in position          # the failed lookup is swallowed…
    assert position["over_country"] == "United Kingdom"  # …but telemetry + working enrichment still flow


class FailingGeocoder(FakeEnricher):
    """The geocode batch always fails (e.g. ClickHouse unreachable); airline/type resolve."""

    async def geocode(self, points: list[tuple[float, float]]) -> list[Record]:
        self.calls.extend(("geocode", round(lat, 2), round(lon, 2)) for lat, lon in points)
        raise httpx.ConnectError("clickhouse unreachable")


async def test_geocode_batch_failure_is_best_effort() -> None:
    stage = AdsbEnrich(enricher=FailingGeocoder())
    # Many aircraft, each in its own grid cell, none with a callsign/type → geocode-only.
    aircraft = [{"hex": f"ac{i:04d}", "lat": 51.0 + i * 0.3, "lon": -0.4} for i in range(20)]
    mod = _make_module(stage, [_raw_record(aircraft)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    sent = _by_topic(producer)
    # A failing geocode batch is swallowed: every aircraft is still emitted, un-enriched.
    assert len(sent[AIRCRAFT_TOPIC]) == 20
    assert all("over_country" not in position for position in sent[AIRCRAFT_TOPIC])
    # And the batch committed its state — the stage made progress, it did not wedge.
    assert await runner.tasks[0].store.get("london") is not None


async def test_enricher_circuit_breaker_pauses_a_rate_limited_wikidata_then_recovers() -> None:
    # The real enricher over a MockTransport + a fake clock: prove the breaker stops
    # calling Wikidata while it's rate-limiting, then resumes once the cooldown lapses.
    requests = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        requests["n"] += 1
        if requests["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"results": {"bindings": [
            {"itemLabel": {"value": "British Airways"},
             "article": {"value": "https://en.wikipedia.org/wiki/British_Airways"}}]}})

    clock = {"t": 0.0}
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    enricher = WikidataClickHouseEnricher(client=client, cooldown=timedelta(seconds=60), now=lambda: clock["t"])

    with pytest.raises(httpx.HTTPStatusError):     # 1st call → 429 → trips the breaker
        await enricher.airline("BAW")
    assert requests["n"] == 1

    with pytest.raises(_Backoff):                  # within cooldown → skipped, no request issued
        await enricher.airline("BAW")
    assert requests["n"] == 1

    clock["t"] = 61.0                              # cooldown elapsed → retried for real, breaker closes
    assert (await enricher.airline("BAW"))[AIRLINE] == "British Airways"
    assert requests["n"] == 2
    await enricher.aclose()


async def test_enricher_geocode_reads_world_and_all_level_dictionaries() -> None:
    # The real enricher's geocode is one ClickHouse POST hitting the world dict + every
    # per-level region dict; ClickHouse concatenates the level hits into nearest_place.
    def handler(request: httpx.Request) -> httpx.Response:
        sql = request.content.decode()
        assert request.method == "POST" and WORLD_DICT in sql
        assert region_dict("ADM5") in sql and region_dict("ADM1") in sql  # all levels stacked
        assert "arrayDistinct" in sql  # adjacent levels sharing a name collapse (no "Kent; Kent")
        return httpx.Response(200, text=json.dumps(
            {"over_country": "Germany", "iso3": "DEU", "nearest_place": "Essen; Nordrhein-Westfalen"}) + "\n")

    enricher = WikidataClickHouseEnricher(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    result = await enricher.geocode([(51.45, 7.01)])
    assert result[0][OVER_COUNTRY] == "Germany" and result[0][ISO3] == "DEU"
    assert result[0][NEAREST_PLACE] == "Essen; Nordrhein-Westfalen"  # hierarchical, finest→coarsest
    await enricher.aclose()


async def test_enricher_geocode_empty_match_is_a_graceful_miss() -> None:
    # dictGet returns empty strings when nothing is loaded (open ocean, or a country whose
    # fine map hasn't arrived yet) → an empty Record, so the aircraft is emitted un-geocoded.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=json.dumps({"over_country": "", "iso3": "", "nearest_place": ""}) + "\n")

    enricher = WikidataClickHouseEnricher(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    assert await enricher.geocode([(0.0, 0.0)]) == [Record()]
    await enricher.aclose()


# --- conflict: TransformerRunner.process_batch on cell-keyed records ---

def _cell_record(hex_: str, lat: float, lon: float, altitude: int, *, cell: str, offset: int,
                 at: str = "2026-07-17T12:00:00Z"):
    value = {"cell": cell, "hex": hex_, "requested_region": "london", "lat": lat, "lon": lon,
             "alt_baro": altitude, "polled_at": at}
    return make_record(key=cell, value=json.dumps(value), topic=CELLS_TOPIC, offset=offset)


def _conflict_count(producer: FakeKafkaProducer) -> int:
    return sum(1 for topic, _ in producer.sent if topic == EVENTS_TOPIC)


async def test_conflict_detects_a_near_miss_once_and_does_not_re_announce() -> None:
    cell = cell_key(51.50, -0.40)
    mod = _make_module(conflict, [
        _cell_record("aaa111", 51.50, -0.40, 35000, cell=cell, offset=0),
        _cell_record("bbb222", 51.51, -0.40, 34600, cell=cell, offset=1),
    ])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    conflicts = [json.loads(payload["value"]) for topic, payload in producer.sent if topic == EVENTS_TOPIC]
    assert [c["event_type"] for c in conflicts] == ["conflict"]
    assert conflicts[0]["hex"] == "bbb222"
    assert (await runner.tasks[0].store.get(cell))  # per-cell positions persisted

    # Next poll, same pair still close — the active pair is remembered, so silent.
    runner.consumer = FakeKafkaConsumer([
        _cell_record("aaa111", 51.50, -0.40, 35000, cell=cell, offset=2, at="2026-07-17T12:00:05Z"),
        _cell_record("bbb222", 51.51, -0.40, 34600, cell=cell, offset=3, at="2026-07-17T12:00:05Z"),
    ])
    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert _conflict_count(producer) == 1  # still one — not re-announced

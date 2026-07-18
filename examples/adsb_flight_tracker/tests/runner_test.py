"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles
(``FakeKafkaConsumer``/``FakeKafkaProducer``, ``InMemoryStateStore``) — no broker,
no network, no live enrichment services:

- **ingest** drives the real ``ExtractorRunner``/``poll_one`` with a stubbed HTTP
  transport, pinning that one whole ``adsb.raw`` record (provenance + preserved
  ``ac[]``) is produced per poll;
- **enrich** drives ``TransformerRunner.process_batch`` with a *fake* ``Enricher``,
  pinning the enriched fan-out **and** the headline showcase — the enrichment
  cache, restored from state, serves a second batch with zero new lookups;
- **conflict** drives ``process_batch`` on cell-keyed records, pinning that a
  near-miss fires once and is not re-announced.
"""
import asyncio
import json
from datetime import timedelta

import httpx
import pytest

from flechtwerk import Config
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
from examples.adsb_flight_tracker.__main__ import NOMINATIM_LOCAL, self_hosted_nominatim
from examples.adsb_flight_tracker.geocoding import NominatimGeocoder
from examples.adsb_flight_tracker.conflict import conflict
from examples.adsb_flight_tracker.enrich import (
    _Backoff,
    AIRCRAFT_TOPIC,
    CELLS_TOPIC,
    EVENTS_TOPIC,
    LIVE_LOOKUPS_PER_POLL,
    AdsbEnrich,
    WikidataNominatimEnricher,
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


# --- ops: the dispatcher auto-detects the self-hosted Nominatim (probe /status) ---

def _probe_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_self_hosted_nominatim_used_when_status_is_ok() -> None:
    # Profile up and done importing → /status is 200 → route geocoding to localhost.
    assert self_hosted_nominatim(_probe_client(lambda request: httpx.Response(200))) == NOMINATIM_LOCAL


def test_self_hosted_nominatim_skipped_while_still_importing() -> None:
    # Container up but the OSM import hasn't finished → /status not 200 → stay on public.
    assert self_hosted_nominatim(_probe_client(lambda request: httpx.Response(503))) is None


def test_self_hosted_nominatim_skipped_when_profile_is_down() -> None:
    # Profile not started → connection refused → stay on public (no crash).
    def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    assert self_hosted_nominatim(_probe_client(refused)) is None


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

    async def geocode(self, lat: float, lon: float) -> Record:
        self.calls.append(("geocode", round(lat, 2), round(lon, 2)))
        return Record({OVER_COUNTRY: "United Kingdom", NEAREST_PLACE: "London"})


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

    # A second batch on the SAME task store — the cache is restored from state, so
    # the resolved entities are not looked up again. This is the headline showcase.
    runner.consumer = FakeKafkaConsumer([_raw_record(aircraft, offset=1)])
    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))
    assert len(fake.calls) == 3  # unchanged — zero new lookups


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


class RateLimitedGeocoder(FakeEnricher):
    """Geocode always 429s (Nominatim rate-limiting); airline/type resolve normally."""

    async def geocode(self, lat: float, lon: float) -> Record:
        self.calls.append(("geocode", round(lat, 2), round(lon, 2)))
        request = httpx.Request("GET", WikidataNominatimEnricher.NOMINATIM_URL)
        raise httpx.HTTPStatusError("429 Too Many Requests", request=request,
                                    response=httpx.Response(429, request=request))


async def test_geocode_rate_limit_neither_stalls_emission_nor_runs_unbounded() -> None:
    stage = AdsbEnrich(enricher=(fake := RateLimitedGeocoder()))
    # Many aircraft, each in its own grid cell, none with a callsign/type → geocode-only.
    aircraft = [{"hex": f"ac{i:04d}", "lat": 51.0 + i * 0.3, "lon": -0.4} for i in range(20)]
    mod = _make_module(stage, [_raw_record(aircraft)])
    runner = mod.runner
    producer = runner.tasks[0].producer

    await runner.process_batch(await runner.consumer.getmany(timeout_ms=1000))

    sent = _by_topic(producer)
    # Every aircraft is still emitted (un-enriched) — a failing geocoder never blocks emission.
    assert len(sent[AIRCRAFT_TOPIC]) == 20
    assert all("over_country" not in position for position in sent[AIRCRAFT_TOPIC])
    # The per-poll budget caps attempts, so a 429 storm can't block the poll loop into an eviction.
    assert sum(1 for call in fake.calls if call[0] == "geocode") <= LIVE_LOOKUPS_PER_POLL
    # And the batch committed its state — the stage made progress, it did not wedge.
    assert await runner.tasks[0].store.get("london") is not None


async def test_enricher_circuit_breaker_pauses_a_rate_limited_upstream_then_recovers() -> None:
    # The real enricher over a MockTransport + a fake clock: prove the breaker stops
    # calling Nominatim while it's rate-limiting, then resumes once the cooldown lapses.
    requests = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        requests["n"] += 1
        if requests["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"address": {"country": "United Kingdom"}})

    clock = {"t": 0.0}
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    enricher = WikidataNominatimEnricher(client=client, cooldown=timedelta(seconds=60), now=lambda: clock["t"])

    with pytest.raises(httpx.HTTPStatusError):     # 1st call → 429 → trips the breaker
        await enricher.geocode(51.5, -0.4)
    assert requests["n"] == 1

    with pytest.raises(_Backoff):                  # within cooldown → skipped, no request issued
        await enricher.geocode(51.5, -0.4)
    assert requests["n"] == 1

    clock["t"] = 61.0                              # cooldown elapsed → retried for real, breaker closes
    assert (await enricher.geocode(51.5, -0.4))[OVER_COUNTRY] == "United Kingdom"
    assert requests["n"] == 2
    await enricher.aclose()


# --- conflict: TransformerRunner.process_batch on cell-keyed records ---

def _cell_record(hex_: str, lat: float, lon: float, altitude: int, *, cell: str, offset: int,
                 at: str = "2026-07-17T12:00:00Z"):
    value = {"cell": cell, "hex": hex_, "region": "london", "lat": lat, "lon": lon,
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

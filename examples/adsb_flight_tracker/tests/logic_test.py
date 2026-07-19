"""Tier 1 — pure logic. No framework machinery, no fakes, no I/O.

The pipeline's interesting behaviour lives in three plain async/sync functions:
``wrap_response`` (ingest), ``project_page`` (enrich), and ``detect_conflicts``
(conflict). Each is driven here by building a ``State``, feeding it input, and
reading back the yielded ``Message``/``State`` — the two-yield contract's biggest
payoff, with nothing mocked (the enrich stage's live lookups are pre-resolved into
the cache the pure generator reads).
"""
from datetime import datetime, timedelta, timezone

import pytest

from flechtwerk import Config, Message, State
from flechtwerk.attribute import MissingAttributeError, Record

from examples.adsb_flight_tracker.attributes import (
    AIRCRAFT_TYPE_NAME,
    AIRLINE,
    AIRLINE_WIKI,
    altitude_ft,
    AC,
    CELL,
    CONFIG,
    DETAIL,
    EMERGENCY,
    EVENT_TYPE,
    FETCH_DURATION,
    FETCHED_AT,
    HEX,
    IS_DELETED,
    LAT,
    LON,
    METADATA,
    NAME,
    NOW,
    POLLED_AT,
    POSITIONS,
    RADIUS,
    REQUESTED_REGION,
    RESPONSE,
    SRC_ALTITUDE,
    SRC_CALLSIGN,
    SRC_LAT,
    SRC_LON,
    TRACKED,
    TYPE_WIKI,
    VERTICAL_RATE,
)
from examples.adsb_flight_tracker.boundaries import boundary_rows, world_rows
from examples.adsb_flight_tracker.conflict import detect_conflicts
from examples.adsb_flight_tracker.enrich import (
    _airline_code,
    AIRCRAFT_TOPIC,
    CELLS_TOPIC,
    EVENTS_TOPIC,
    cell_key,
    project_page,
)
from examples.adsb_flight_tracker.ingest import AdsbIngest, DEFAULT_RADIUS, MAX_RADIUS, wrap_response

POLLED_AT_TS = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


# --- attributes: altitude_ft (the one place the polymorphic alt_baro is coerced) ---

def test_altitude_ft_reads_the_polymorphic_wire_value() -> None:
    assert altitude_ft(30000) == 30000   # a number is feet
    assert altitude_ft("ground") is None  # "ground" ≠ 0 ft MSL → no numeric altitude, NOT a fabricated 0
    assert altitude_ft(None) is None     # absent → unknown


# --- enrich: _airline_code (ICAO designator extraction) ---

def test_airline_code_extracts_the_icao_designator() -> None:
    assert _airline_code("BAW123") == "BAW"    # 3 letters + a flight number
    assert _airline_code("EZY51JC") == "EZY"   # a trailing letter in the flight number is fine
    assert _airline_code("baw123") == "BAW"    # normalised to upper


def test_airline_code_rejects_non_airline_callsigns() -> None:
    assert _airline_code("ABCD56") is None     # 4 leading letters → NOT "designator + flight number"
    assert _airline_code("N12345") is None     # general-aviation registration — no operator
    assert _airline_code("ABC") is None        # too short (no flight number)
    assert _airline_code("ABCDEF") is None     # all letters, no flight number
    assert _airline_code("") is None


# --- enrich: project_page (with a pre-filled enrichment cache) ---

async def _enrich(state: State, aircraft: list[dict], *, polled_at: datetime = POLLED_AT_TS,
                  airline: dict | None = None, aircraft_type: dict | None = None, geo: dict | None = None):
    # `project_page` reads aircraft through the inbound SRC_* handles, so it takes
    # Records — wrap the raw dicts exactly as `transform` does via `msg.value[AC]`.
    records = [Record.wrap(a) for a in aircraft]
    items = [
        item async for item in
        project_page("london", state, records, polled_at, airline or {}, aircraft_type or {}, geo or {}, set())
    ]
    messages = [i for i in items if isinstance(i, Message)]
    return messages, [i for i in items if isinstance(i, State)], items


def _by_topic(messages: list[Message], topic: str) -> list[Message]:
    return [m for m in messages if m.topic == topic]


async def test_emits_one_enriched_message_per_aircraft_then_a_state_page() -> None:
    airline = {"BAW": Record({AIRLINE: "British Airways",
                              AIRLINE_WIKI: "https://en.wikipedia.org/wiki/British_Airways"})}
    aircraft_type = {"A320": Record({AIRCRAFT_TYPE_NAME: "Airbus A320",
                                     TYPE_WIKI: "https://en.wikipedia.org/wiki/Airbus_A320"})}
    aircraft = [
        {"hex": "abc123", "flight": "BAW123  ", "lat": 51.5, "lon": -0.4, "alt_baro": 30000, "gs": 420.0, "t": "A320"},
        {"hex": "def456", "lat": 51.4, "lon": -0.5, "alt_baro": "ground"},
    ]
    messages, states, items = await _enrich(State(), aircraft, airline=airline, aircraft_type=aircraft_type)

    positions = _by_topic(messages, AIRCRAFT_TOPIC)
    assert [m.key for m in positions] == ["abc123", "def456"]
    assert positions[0].value[SRC_CALLSIGN] == "BAW123  "  # flight spreads through verbatim (padded); dashboards trim
    assert positions[0].value[AIRLINE] == "British Airways"  # our derived enrichment, from the cache
    assert positions[0].value[AIRLINE_WIKI].endswith("British_Airways")
    assert positions[0].value[AIRCRAFT_TYPE_NAME] == "Airbus A320"
    assert positions[0].value[EMERGENCY] == 0
    assert positions[1].value[SRC_ALTITUDE] == "ground"  # faithful: the polymorphic wire value passes through
    assert SRC_CALLSIGN not in positions[1].value  # absent feed fields stay absent
    assert AIRLINE not in positions[1].value  # no callsign → no airline lookup applied

    # The State is the final yield — the commit boundary that closes the page.
    assert len(states) == 1 and items[-1] is states[0]
    assert set(states[0][TRACKED].keys()) == {"abc123", "def456"}  # roster = the priors' keys


async def test_feed_fields_pass_through_untouched() -> None:
    # Only fields we compute with are read; everything else — known telemetry we
    # don't touch (gs/track/seen) and fields we've never heard of (rssi, nav_qnh) —
    # rides through verbatim under its wire name. That is the robustness win: a new
    # adsb.lol field needs no code change. Whole-number telemetry stays an int
    # (ClickHouse coerces to Float64); nothing is normalised — not even ``alt_baro``.
    aircraft = [{"hex": "abc123", "lat": 51, "lon": 0, "alt_baro": "ground",
                 "gs": 0, "track": 0, "seen": 2, "nav_qnh": 1013, "rssi": -8.4}]

    messages, _, _ = await _enrich(State(), aircraft)

    raw = _by_topic(messages, AIRCRAFT_TOPIC)[0].value.raw
    assert raw["gs"] == 0 and raw["track"] == 0 and raw["seen"] == 2  # untouched, still ints
    assert raw["lat"] == 51 and raw["lon"] == 0  # spread verbatim (ClickHouse coerces to Float64)
    assert raw["nav_qnh"] == 1013 and raw["rssi"] == -8.4  # fields we don't model, carried anyway
    assert raw["alt_baro"] == "ground"  # the polymorphic field, faithful — collapsed to feet only at compute sites


async def test_positioned_aircraft_fan_out_to_cells_for_the_self_join() -> None:
    aircraft = [
        {"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 30000},
        {"hex": "nopos", "alt_baro": 10000},  # no position → not a conflict candidate
    ]
    messages, _, _ = await _enrich(State(), aircraft)

    cells = _by_topic(messages, CELLS_TOPIC)
    assert [m.key for m in cells] == [cell_key(51.5, -0.4)]  # re-keyed by grid cell
    assert cells[0].value[HEX] == "abc123"
    assert cells[0].value[CELL] == cell_key(51.5, -0.4)


async def test_vertical_rate_is_derived_from_the_prior_altitude() -> None:
    prior = State({TRACKED: {"abc123": Record({SRC_ALTITUDE: 35000, POLLED_AT: POLLED_AT_TS})}})
    later = POLLED_AT_TS + timedelta(minutes=1)

    messages, _, _ = await _enrich(prior, [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 33000}], polled_at=later)

    value = _by_topic(messages, AIRCRAFT_TOPIC)[0].value
    assert value[VERTICAL_RATE] == -2000.0  # 2000 ft lost in one minute
    assert _by_topic(messages, EVENTS_TOPIC) == []  # -2000 is not yet a rapid descent


async def test_rapid_descent_fires_once_at_onset() -> None:
    prior = State({TRACKED: {"abc123": Record({SRC_ALTITUDE: 40000, POLLED_AT: POLLED_AT_TS})}})
    t1 = POLLED_AT_TS + timedelta(minutes=1)

    messages, states, _ = await _enrich(prior, [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 36000}], polled_at=t1)
    events = _by_topic(messages, EVENTS_TOPIC)
    assert [e.value[EVENT_TYPE] for e in events] == ["rapid_descent"]  # -4000 ft/min
    assert "ft/min" in events[0].value[DETAIL]

    # Still descending as fast on the next poll — already flagged, so no repeat.
    t2 = t1 + timedelta(minutes=1)
    messages2, _, _ = await _enrich(states[0], [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 32000}], polled_at=t2)
    assert _by_topic(messages2, EVENTS_TOPIC) == []


async def test_emergency_squawk_fires_once_at_onset() -> None:
    messages, states, _ = await _enrich(State(), [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 10000, "squawk": "7700"}])

    assert _by_topic(messages, AIRCRAFT_TOPIC)[0].value[EMERGENCY] == 1
    events = _by_topic(messages, EVENTS_TOPIC)
    assert [e.value[EVENT_TYPE] for e in events] == ["emergency"]
    assert "7700" in events[0].value[DETAIL]

    # Still squawking 7700 next poll — the onset already fired, so no repeat.
    messages2, _, _ = await _enrich(states[0], [{"hex": "abc123", "lat": 51.5, "lon": -0.4, "alt_baro": 10000, "squawk": "7700"}])
    assert _by_topic(messages2, EVENTS_TOPIC) == []


async def test_departure_tombstones_and_airborne_loss_goes_dark() -> None:
    prior = State({TRACKED: {
        "high99": Record({SRC_ALTITUDE: 35000, SRC_LAT: 51.5, SRC_LON: -0.4, POLLED_AT: POLLED_AT_TS}),
        "low11": Record({SRC_ALTITUDE: 400, SRC_LAT: 51.4, SRC_LON: -0.5, POLLED_AT: POLLED_AT_TS}),  # near the ground
    }})

    messages, states, _ = await _enrich(prior, [])  # empty feed → both depart

    tombstones = [m for m in _by_topic(messages, AIRCRAFT_TOPIC) if m.value[IS_DELETED] == 1]
    assert {t.key for t in tombstones} == {"high99", "low11"}  # both retired downstream
    events = _by_topic(messages, EVENTS_TOPIC)
    assert [e.value[EVENT_TYPE] for e in events] == ["going_dark"]  # only the airborne one
    assert events[0].value[HEX] == "high99"
    assert states[0][TRACKED] == {}  # the region persists; its roster is now empty


async def test_aircraft_without_a_hex_is_a_data_error() -> None:
    # hex is the aircraft identity — adsb.lol always sends it. A missing one is
    # malformed, so the page crashes ("let it crash"), never silently dropped.
    with pytest.raises(MissingAttributeError):
        await _enrich(State(), [{"lat": 51.5, "lon": -0.4}])


# --- ingest: wrap_response ---

def test_wrap_response_nests_response_config_and_metadata() -> None:
    config = Config({NAME: "london", LAT: 51.47, LON: -0.45, RADIUS: 100})
    raw = {"now": 1_700_000_000_000, "ac": [{"hex": "abc123", "lat": 51.5}], "total": 1, "ptime": 5}
    fetched_at = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)
    duration = timedelta(milliseconds=42.5)

    # The raw feed dict is Record.wrap-ed at the boundary (as `fetch` does), so
    # wrap_response only ever handles a typed Record — never a naive dict.
    value = wrap_response(config, Record.wrap(raw), fetched_at, duration)

    # Each part keeps its own namespace, so the uncontrolled feed can never collide
    # with our fields (a flat merge could).
    assert value[CONFIG][NAME] == "london"
    assert value[METADATA][FETCHED_AT] == fetched_at
    assert value[METADATA][FETCH_DURATION] == duration  # a timedelta, round-tripped
    response = value[RESPONSE]
    assert float(response[NOW]) == 1_700_000_000_000
    assert [a[HEX] for a in response[AC]] == ["abc123"]  # the ac[] array is preserved verbatim
    assert response.raw["total"] == 1  # and so is every other field the feed sent


# --- conflict: detect_conflicts (cell records keep the feed's wire keys) ---

def _cell_aircraft(hex_: str, lat: float, lon: float, altitude: object, at: datetime = POLLED_AT_TS) -> Record:
    # altitude is the faithful wire value — a number OR the string "ground" (ANY).
    return Record({HEX: hex_, REQUESTED_REGION: "london", SRC_LAT: lat, SRC_LON: lon, SRC_ALTITUDE: altitude, POLLED_AT: at})


async def _detect(state: State, aircraft: Record, at: datetime = POLLED_AT_TS):
    items = [item async for item in detect_conflicts(state, aircraft, at)]
    return [i for i in items if isinstance(i, Message)], [i for i in items if isinstance(i, State)][0]


async def test_two_close_aircraft_raise_one_conflict_then_dedup() -> None:
    # First aircraft into the cell: nothing to compare against yet.
    messages, state = await _detect(State(), _cell_aircraft("aaa111", 51.50, -0.40, 35000))
    assert messages == []

    # Second aircraft ~1 nm and 400 ft away → a conflict, keyed by the pair.
    messages, state = await _detect(state, _cell_aircraft("bbb222", 51.51, -0.40, 34600))
    assert [m.value[EVENT_TYPE] for m in messages] == ["conflict"]
    assert messages[0].key == "aaa111|bbb222"
    assert "nm" in messages[0].value[DETAIL]

    # Still close on the next poll — the pair is active, so it is not re-announced.
    messages, _ = await _detect(state, _cell_aircraft("bbb222", 51.51, -0.40, 34600, at=POLLED_AT_TS + timedelta(seconds=5)))
    assert messages == []


async def test_vertically_separated_aircraft_do_not_conflict() -> None:
    _, state = await _detect(State(), _cell_aircraft("aaa111", 51.50, -0.40, 35000))
    messages, _ = await _detect(state, _cell_aircraft("bbb222", 51.51, -0.40, 30000))  # 5000 ft apart
    assert messages == []


async def test_ground_aircraft_are_not_conflict_candidates() -> None:
    # Two aircraft parked wingtip-to-wingtip on the same apron: co-located and both
    # on the surface (alt_baro "ground", faithful). Airborne-conflict detection must
    # ignore them — and never keep them in the cell's self-join.
    _, state = await _detect(State(), _cell_aircraft("aaa111", 51.500, -0.400, "ground"))
    messages, state = await _detect(state, _cell_aircraft("bbb222", 51.5001, -0.400, "ground"))
    assert messages == []
    assert state[POSITIONS] == {}  # neither enters the self-join


async def test_airborne_over_grounded_is_not_a_conflict() -> None:
    # An aircraft climbing out directly over one still on the runway → no conflict;
    # only the airborne one is tracked.
    _, state = await _detect(State(), _cell_aircraft("air111", 51.50, -0.40, 5000))
    messages, state = await _detect(state, _cell_aircraft("gnd222", 51.50, -0.40, "ground"))
    assert messages == []
    assert set(state[POSITIONS].keys()) == {"air111"}


async def test_aircraft_below_the_airborne_floor_are_filtered() -> None:
    # A numeric-but-low reading (on short final) is below the floor too — not just
    # the "ground" sentinel — so approach-corridor clutter stays quiet.
    _, state = await _detect(State(), _cell_aircraft("aaa111", 51.50, -0.40, 300))
    messages, state = await _detect(state, _cell_aircraft("bbb222", 51.50, -0.40, 300))
    assert messages == []
    assert state[POSITIONS] == {}


async def test_stale_cell_positions_are_dropped_before_checking() -> None:
    _, state = await _detect(State(), _cell_aircraft("aaa111", 51.50, -0.40, 35000))

    # 20 s later aaa111 has gone stale, so a new arrival sees an empty cell.
    late = POLLED_AT_TS + timedelta(seconds=20)
    messages, state = await _detect(state, _cell_aircraft("bbb222", 51.51, -0.40, 35000, at=late), at=late)

    assert messages == []
    assert set(state[POSITIONS].keys()) == {"bbb222"}  # aaa111 expired out of the cell


# --- ingest: enrich_config (config normalisation) ---

async def test_enrich_config_defaults_and_clamps_the_radius() -> None:
    stage = AdsbIngest()

    assert (await stage.enrich_config(Config({NAME: "x", LAT: 0.0, LON: 0.0})))[RADIUS] == DEFAULT_RADIUS
    assert (await stage.enrich_config(Config({NAME: "x", LAT: 0.0, LON: 0.0, RADIUS: 9999})))[RADIUS] == MAX_RADIUS
    assert (await stage.enrich_config(Config({NAME: "x", LAT: 0.0, LON: 0.0, RADIUS: 50})))[RADIUS] == 50


# --- boundaries: world_rows / boundary_rows (geoBoundaries GeoJSON → ClickHouse rows) ---

def test_boundary_rows_normalises_a_polygon_to_a_multipolygon() -> None:
    ring = [[6.9, 51.4], [7.1, 51.4], [7.1, 51.5], [6.9, 51.4]]
    geojson = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"shapeName": "Essen"},
         "geometry": {"type": "Polygon", "coordinates": [ring]}}]}

    rows = boundary_rows(geojson, "DEU", "ADM3", 1_700_000_000)

    assert len(rows) == 1
    row = rows[0]
    assert (row["name"], row["iso3"], row["admin_level"]) == ("Essen", "DEU", "ADM3")  # tagged by country + level
    assert row["loaded_at"] == 1_700_000_000  # Unix seconds → the table's DateTime
    # A Polygon is wrapped one level deeper so every row is a MultiPolygon; coordinates
    # ride through verbatim — GeoJSON's [lon, lat] is already ClickHouse's (x, y).
    assert row["geometry"] == [[ring]]


def test_boundary_rows_keeps_multipolygons_and_skips_non_polygons() -> None:
    polygons = [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 2.0]]]]
    geojson = {"features": [
        {"properties": {"shapeName": "Multi"}, "geometry": {"type": "MultiPolygon", "coordinates": polygons}},
        {"properties": {"shapeName": "A Point"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}},
    ]}

    rows = boundary_rows(geojson, "DEU", "ADM4", 0)

    assert [row["name"] for row in rows] == ["Multi"]        # the non-polygon feature is dropped
    assert (rows[0]["geometry"], rows[0]["admin_level"]) == (polygons, "ADM4")  # used as-is, level recorded


def test_world_rows_carries_country_name_and_iso3() -> None:
    # The global ADM0 map (Natural Earth): one row per country, keyed for enrich's detector.
    geojson = {"features": [
        {"properties": {"NAME": "Germany", "ADM0_A3": "DEU"},
         "geometry": {"type": "Polygon", "coordinates": [[[6, 50], [8, 50], [8, 52], [6, 50]]]}},
        {"properties": {"NAME": "Ocean point"}, "geometry": {"type": "Point", "coordinates": [0, 0]}},
    ]}

    rows = world_rows(geojson, 1_700_000_000)

    assert len(rows) == 1  # the non-polygon feature is dropped
    assert (rows[0]["country"], rows[0]["iso3"]) == ("Germany", "DEU")

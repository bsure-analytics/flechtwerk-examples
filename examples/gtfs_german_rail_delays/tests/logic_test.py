"""Tier 1 — pure logic. No framework, no mocks, no network.

Drives the stages' pure cores straight off the committed fixtures
(``fv_sample.zip`` / ``rt_sample.pb``, both trimmed from the real feeds by
``fixtures/make_fixtures.py``): the loader's profile projection, and (added with
the later stages) the ingest decode + the delay computation.
"""
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from flechtwerk import Event

from examples.gtfs_german_rail_delays.attributes import (
    DELAY_S,
    DESTINATION,
    FEED_TS,
    LAT,
    LINE,
    LON,
    NEXT_STOP,
    ROUTE_TYPE,
    STATIC_VERSION,
    STATUS,
    STOP_ARR_S,
    STOP_DEP_S,
    STOP_ID,
    STOP_LAT,
    STOP_LON,
    STOP_NAME,
    STOP_SEQ,
    STOPS,
    STOPS_TOTAL,
    TIMEZONE,
    TRIP,
    TRIP_ID,
)
from examples.gtfs_german_rail_delays.delays import (
    build_delay_state,
    classify,
    effective_delays,
    locate,
    service_time_to_utc,
)
from examples.gtfs_german_rail_delays.ingest import decode_feed
from examples.gtfs_german_rail_delays.loader import build_profiles, parse_gtfs_time

FIXTURES = Path(__file__).parent / "fixtures"
FV_ZIP = (FIXTURES / "fv_sample.zip").read_bytes()
RT_PB = (FIXTURES / "rt_sample.pb").read_bytes()
BERLIN = ZoneInfo("Europe/Berlin")


def _with_agency(agency_txt: str) -> bytes:
    """The fixture zip with its ``agency.txt`` replaced — every other table verbatim."""
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(FV_ZIP)) as src, zipfile.ZipFile(out, "w") as dst:
        for name in src.namelist():
            dst.writestr(name, agency_txt if name == "agency.txt" else src.read(name))
    return out.getvalue()


# --- parse_gtfs_time ---

def test_parse_gtfs_time_basic() -> None:
    assert parse_gtfs_time("00:00:00") == 0
    assert parse_gtfs_time("08:30:00") == 8 * 3600 + 30 * 60


def test_parse_gtfs_time_past_midnight() -> None:
    # GTFS expresses an after-midnight stop in the service day's clock (> 24 h).
    assert parse_gtfs_time("25:14:00") == 25 * 3600 + 14 * 60


# --- build_profiles ---

def test_build_profiles_projects_every_fixture_trip() -> None:
    profiles = dict(build_profiles(FV_ZIP, "etag-123"))
    assert len(profiles) == 5                                    # the five fixture trips
    for trip_id, profile in profiles.items():
        assert profile[TRIP_ID] == trip_id                       # keyed by its own id
        assert profile[LINE].startswith("IC")                    # ICE/IC long-distance line
        assert profile[ROUTE_TYPE] == 2                          # rail
        assert profile[STATIC_VERSION] == "etag-123"             # version stamped through


def test_build_profiles_stops_are_ordered_and_geolocated() -> None:
    _, profile = next(iter(build_profiles(FV_ZIP, "v1")))
    stops = profile[STOPS]
    assert len(stops) >= 2
    seqs = [s[STOP_SEQ] for s in stops]
    assert seqs == sorted(seqs)                                  # ordered by stop_sequence
    for s in stops:
        assert isinstance(s[STOP_LAT], float) and isinstance(s[STOP_LON], float)
        assert 45.0 < s[STOP_LAT] < 56.0 and 5.0 < s[STOP_LON] < 16.0  # inside Germany
        assert isinstance(s[STOP_ARR_S], int) and isinstance(s[STOP_DEP_S], int)
        assert s[STOP_NAME]


def test_build_profiles_destination_is_last_stop() -> None:
    _, profile = next(iter(build_profiles(FV_ZIP, "v1")))
    assert profile[DESTINATION] == profile[STOPS][-1][STOP_NAME]


def test_build_profiles_carries_the_feed_timezone() -> None:
    # Every agency in the fixture names Europe/Berlin; the profile carries it typed and
    # puts the IANA key on the wire.
    for _, profile in build_profiles(FV_ZIP, "v1"):
        assert profile[TIMEZONE] == BERLIN
        assert profile.raw["timezone"] == "Europe/Berlin"


def test_build_profiles_rejects_a_feed_with_mixed_agency_timezones() -> None:
    feed = _with_agency("agency_id,agency_name,agency_url,agency_timezone\n"
                        "1,A,https://a.example,Europe/Berlin\n2,B,https://b.example,Europe/Vienna\n")
    with pytest.raises(ValueError, match="exactly one agency_timezone"):
        list(build_profiles(feed, "v1"))


def test_build_profiles_rejects_an_unknown_agency_timezone() -> None:
    feed = _with_agency("agency_id,agency_name,agency_url,agency_timezone\n1,A,https://a.example,Mars/Olympus\n")
    with pytest.raises(ValueError, match="unknown time zone"):
        list(build_profiles(feed, "v1"))


def test_build_profiles_route_type_filter_excludes_non_rail() -> None:
    # The fixture is all rail (route_type 2); filtering to an empty set yields nothing,
    # proving the filter is applied before projection (a national feed's buses stay out).
    assert list(build_profiles(FV_ZIP, "v1", route_types=frozenset())) == []


# --- ingest: decode_feed (protobuf → dicts at the edge) ---

def test_decode_feed_yields_one_message_per_tripupdate() -> None:
    feed_ts, updates = decode_feed(RT_PB)
    # The header timestamp becomes the event time (the fixture froze the real snapshot).
    assert feed_ts == datetime(2026, 7, 22, 9, 37, 18, tzinfo=timezone.utc)
    assert len(updates) == 5                                     # 5 TripUpdates; the alert is ignored
    for trip_id, update in updates:
        assert update[TRIP][TRIP_ID] == trip_id                  # keyed by its own trip_id
        assert update[FEED_TS] == feed_ts                        # event time stamped on every message


def test_decode_feed_preserves_wire_types_faithfully() -> None:
    # int32 delay decodes to a Python int (a number on the wire), not a string; the whole
    # TripUpdate rides through verbatim so unread fields still reach ClickHouse.
    _, updates = decode_feed(RT_PB)
    delays = [
        stu.get("departure", {}).get("delay", stu.get("arrival", {}).get("delay"))
        for _, update in updates for stu in (update.raw.get("stop_time_update") or [])
    ]
    assert any(isinstance(d, int) for d in delays if d is not None)


# --- delays: pure computation ---

def test_service_time_to_utc_summer_and_winter_dst() -> None:
    # Local noon on a summer day is 10:00Z (CEST, +2); on a winter day 11:00Z (CET, +1).
    assert service_time_to_utc("20260722", 12 * 3600, BERLIN) == datetime(2026, 7, 22, 10, 0, tzinfo=timezone.utc)
    assert service_time_to_utc("20260115", 12 * 3600, BERLIN) == datetime(2026, 1, 15, 11, 0, tzinfo=timezone.utc)


def test_service_time_to_utc_follows_the_given_zone() -> None:
    # Nothing is pinned to Berlin: the same clock-time in New York (EDT, -4) is 16:00Z.
    assert service_time_to_utc("20260722", 12 * 3600, ZoneInfo("America/New_York")) == \
        datetime(2026, 7, 22, 16, 0, tzinfo=timezone.utc)


def test_service_time_to_utc_past_midnight() -> None:
    # "25:30" on the 2026-07-22 service day is 01:30 local next day = 23:30Z same day.
    assert service_time_to_utc("20260722", parse_gtfs_time("25:30:00"), BERLIN) == \
        datetime(2026, 7, 22, 23, 30, tzinfo=timezone.utc)


def test_classify_boundaries() -> None:
    assert classify(-61) == "early"
    assert classify(-60) == "on_time" and classify(0) == "on_time" and classify(360) == "on_time"
    assert classify(361) == "late" and classify(1800) == "late"
    assert classify(1801) == "severe"


def _stop(stop_id: str, arr_s: int, dep_s: int, *, name: str = "", lat: float = 50.0, lon: float = 8.0):
    return {STOP_ID: stop_id, STOP_ARR_S: arr_s, STOP_DEP_S: dep_s,
            STOP_NAME: name or stop_id, STOP_LAT: lat, STOP_LON: lon, STOP_SEQ: 0}


def test_effective_delays_carry_forward_and_skip() -> None:
    stops = [_stop("A", 0, 0), _stop("B", 100, 100), _stop("C", 200, 200), _stop("D", 300, 300)]
    stus = [
        {STOP_ID: "A", "departure": {"delay": 120}},                       # +120 from A
        {STOP_ID: "C", "schedule_relationship": "SKIPPED"},                # skipped, delay carries
        {STOP_ID: "D", "arrival": {"delay": 300}},                        # override at D
    ]
    delays, skipped = effective_delays(stops, stus)
    assert delays == [120, 120, 120, 300]                                 # B inherits A; C carries; D overrides
    assert skipped == [False, False, True, False]


def test_effective_delays_no_data_resets() -> None:
    stops = [_stop("A", 0, 0), _stop("B", 100, 100)]
    stus = [{STOP_ID: "A", "departure": {"delay": 600}}, {STOP_ID: "B", "schedule_relationship": "NO_DATA"}]
    assert effective_delays(stops, stus)[0] == [600, 0]                   # NO_DATA reverts to schedule


def test_locate_before_mid_and_terminated() -> None:
    stops = [_stop("A", 0, 0), _stop("B", 100, 100), _stop("C", 200, 200)]
    base = service_time_to_utc("20260722", 0, BERLIN)                     # A's scheduled departure
    delays = [0, 0, 0]
    assert locate(stops, delays, "20260722", BERLIN, base - timedelta(seconds=10)).next_idx == 0  # before origin
    mid = base + timedelta(seconds=150)                                   # between B and C
    prog = locate(stops, delays, "20260722", BERLIN, mid)
    assert prog.next_idx == 2 and prog.stops_done == 2
    assert locate(stops, delays, "20260722", BERLIN, base + timedelta(seconds=1000)) is None  # arrived → nothing


def test_build_delay_state_falls_back_to_the_local_service_day() -> None:
    # An update without trip.start_date takes the service day from feed_ts — in the feed's
    # zone, not UTC. 23:30Z on 21 July is 01:30 on 22 July in Berlin: on the 22nd's
    # schedule the train is between A (01:00) and B (02:00); on the 21st's it would have
    # arrived a day ago and the record would vanish.
    profile = Event({
        TRIP_ID: "t", LINE: "ICE 1", ROUTE_TYPE: 2, TIMEZONE: BERLIN,
        STOPS: [_stop("A", 3600, 3600), _stop("B", 7200, 7200)],
    })
    record = build_delay_state(profile, Event({}), datetime(2026, 7, 21, 23, 30, tzinfo=timezone.utc))
    assert record is not None and record[NEXT_STOP] == "B"


def test_build_delay_state_golden_on_a_real_fixture_trip() -> None:
    # End-to-end over the real fixtures: join the severe-delay ICE (chosen by make_fixtures)
    # and assert the derived record is placed at a real German station with a matching bucket.
    profiles = dict(build_profiles(FV_ZIP, "v1"))
    feed_ts, updates = decode_feed(RT_PB)
    matched = [(profiles[tid], up) for tid, up in updates if tid in profiles]
    assert matched, "fixtures must share trip_ids"
    records = [build_delay_state(p, up, feed_ts) for p, up in matched]
    live = [r for r in records if r is not None]
    assert live, "at least one fixture trip is mid-journey"
    for r in live:
        # status is the honest bucket of the current delay, self-consistent end to end
        assert r[STATUS] == classify(r[DELAY_S])
        assert 45.0 < r[LAT] < 56.0 and 5.0 < r[LON] < 16.0              # snapped to a German station
        assert r[NEXT_STOP] and r[STOPS_TOTAL] >= 2                      # placed at a real stop
        assert r[FEED_TS] == feed_ts                                    # event time carried through

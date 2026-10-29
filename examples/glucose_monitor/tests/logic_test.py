"""Tier 1 — the pure logic, no framework runtime, no mocks.

Everything ``readings`` decides is a plain function in ``projection.py``: parse a LibreLinkUp
timestamp, derive the phone's UTC offset, take the readings out of a ``graph`` response, and keep
only the ones not emitted before. The fixture is synthetic (``fixtures/PROVENANCE.md``), and the
counts that depend on it are computed from it here.
"""
import copy
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from examples.glucose_monitor.projection import (
    HORIZON,
    Reading,
    fresh,
    parse_timestamp,
    prune_seen,
    readings,
    utc_offset_minutes,
)

FIXTURES = Path(__file__).parent / "fixtures"
GRAPH = json.loads((FIXTURES / "graph.json").read_text())["data"]
HISTORY = GRAPH["graphData"]
LIVE = GRAPH["connection"]["glucoseMeasurement"]
UTC = timezone.utc


# --- timestamps ---

@pytest.mark.parametrize(("text", "expected"), [
    ("10/29/2026 4:45:07 AM", datetime(2026, 10, 29, 4, 45, 7)),
    ("1/2/2026 12:00:00 AM", datetime(2026, 1, 2, 0, 0, 0)),     # midnight is 12 AM
    ("1/2/2026 12:30:00 PM", datetime(2026, 1, 2, 12, 30, 0)),   # noon is 12 PM
    ("12/31/2026 11:59:59 PM", datetime(2026, 12, 31, 23, 59, 59)),
    (" 6/21/2026 6:42:32 PM ", datetime(2026, 6, 21, 18, 42, 32)),
])
def test_parse_timestamp(text: str, expected: datetime) -> None:
    assert parse_timestamp(text) == expected


@pytest.mark.parametrize("text", ["2026-10-29T04:45:07Z", "10/29/2026 4:45 AM", "10/29/2026 16:45:07"])
def test_parse_timestamp_refuses_other_formats(text: str) -> None:
    with pytest.raises(ValueError, match="not a LibreLinkUp timestamp"):
        parse_timestamp(text)


@pytest.mark.parametrize(("hours", "seconds", "expected"), [
    (1, 0, 60),            # CET
    (2, 3, 120),           # CEST, with the two clocks a few seconds apart
    (-5, -2, -300),        # US Eastern
    (5.5, 0, 330),         # India
    (0, 0, 0),
])
def test_utc_offset_rounds_to_a_quarter_hour(hours: float, seconds: int, expected: int) -> None:
    utc = datetime(2026, 10, 29, 12, 0, 0)
    assert utc_offset_minutes(utc, utc + timedelta(hours=hours, seconds=seconds)) == expected


# --- readings ---

def test_readings_takes_the_live_measurement_and_every_history_point() -> None:
    out = readings(GRAPH)
    assert [r.kind for r in out].count("live") == 1
    assert [r.kind for r in out].count("history") == len(HISTORY)
    assert [r.measured_at for r in out] == sorted(r.measured_at for r in out)  # oldest first
    live = next(r for r in out if r.kind == "live")
    assert live.item is LIVE                                  # spread through, untouched
    assert live.measured_at == datetime(2026, 10, 29, 7, 13, 41, tzinfo=UTC)  # FactoryTimestamp is UTC
    assert live.utc_offset_min == 60                          # Timestamp is the phone's CET clock
    assert "TrendArrow" not in out[0].item                    # history points carry no arrow


def test_readings_skips_an_item_it_cannot_place_in_time(caplog: pytest.LogCaptureFixture) -> None:
    data = copy.deepcopy(GRAPH)
    del data["graphData"][0]["FactoryTimestamp"]
    with caplog.at_level(logging.WARNING):
        out = readings(data)
    assert len(out) == len(HISTORY)                           # one history point fewer, live kept
    assert "without FactoryTimestamp" in caplog.text


def test_readings_without_a_local_clock_has_no_offset() -> None:
    data = copy.deepcopy(GRAPH)
    del data["connection"]["glucoseMeasurement"]["Timestamp"]
    live = next(r for r in readings(data) if r.kind == "live")
    assert live.utc_offset_min is None                        # unknown, never a fabricated 0


def test_readings_of_an_empty_response() -> None:
    assert readings({}) == []
    assert readings({"connection": {}, "graphData": None}) == []


# --- the seen-set ---

def _reading(kind: str, at: datetime) -> Reading:
    return Reading(kind, at, 60, {"ValueInMgPerDl": 100})


T0 = datetime(2026, 10, 29, 7, 0, 0, tzinfo=UTC)


def test_fresh_emits_once() -> None:
    first, seen = fresh(readings(GRAPH), {})
    assert len(first) == len(HISTORY) + 1
    assert set(seen) == {"live", "history"}
    again, seen_again = fresh(readings(GRAPH), seen)
    assert again == [] and seen_again == seen                 # a repeated window is no news


def test_fresh_catches_a_history_point_filled_in_behind_the_newest() -> None:
    _, seen = fresh([_reading("history", T0), _reading("history", T0 + timedelta(minutes=30))], {})
    late = _reading("history", T0 + timedelta(minutes=15))   # LibreLinkUp reported it late
    new, seen = fresh([late, _reading("history", T0 + timedelta(minutes=30))], seen)
    assert new == [late]                                      # a high-water mark would have lost it


def test_fresh_dedupes_within_a_kind_only() -> None:
    new, _ = fresh([_reading("live", T0), _reading("history", T0), _reading("live", T0)], {})
    assert [r.kind for r in new] == ["live", "history"]       # same instant, two series; one duplicate


def test_fresh_drops_what_is_older_than_the_horizon() -> None:
    _, seen = fresh([_reading("live", T0)], {})
    ancient = _reading("history", T0 - HORIZON - timedelta(seconds=1))
    new, seen = fresh([ancient], seen)
    assert new == [] and seen == {"live": [int(T0.timestamp())]}


def test_the_seen_set_stays_bounded_by_event_time() -> None:
    seen: dict[str, list[int]] = {}
    for minute in range(3 * 24 * 60):                         # three days of a reading a minute
        _, seen = fresh([_reading("live", T0 + timedelta(minutes=minute))], seen)
    assert len(seen["live"]) == int(HORIZON.total_seconds() // 60) + 1
    assert len(json.dumps(seen)) < 50_000                     # far under the ~1 MB changelog record


def test_prune_seen_drops_empty_kinds() -> None:
    assert prune_seen({"live": {10, 20}, "history": {5}}, 10) == {"live": [10, 20]}
    assert prune_seen({}, 0) == {}

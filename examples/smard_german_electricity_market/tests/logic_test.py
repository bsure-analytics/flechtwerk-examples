"""Tier 1 — pure logic. No framework, no mocks, no network.

Drives the stages' pure cores directly: the ingest window differ
(``select_week_files`` / ``diff_series`` / ``aged_out``) and the mix assembler
(``assemble_mix``), plus a drift guard tying the seeded basket to ``SOURCE_META``.
The committed fixtures (synthesized by ``fixtures/make_fixtures.py``) stand in for one
week file; most cases build tiny point lists inline so the arithmetic is obvious.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from examples.smard_german_electricity_market.attributes import (
    C_ROLE,
    C_SOURCE,
    C_VALUE,
    CO2_G_PER_KWH,
    GENERATION,
    INTERVAL_TS,
    IS_FINAL,
    LOAD_MWH,
    N_SOURCES,
    PRICE_EUR_MWH,
    RENEWABLES_SHARE,
    RESIDUAL_LOAD_MWH,
    ROLE,
    TOTAL_GENERATION_MWH,
    UPDATED_AT,
)
from examples.smard_german_electricity_market.ingest import (
    Observation,
    aged_out,
    diff_series,
    interval_key,
    select_week_files,
)
from examples.smard_german_electricity_market.mix import SOURCE_META, assemble_mix
from examples.smard_german_electricity_market.setup import SERIES

FIXTURES = Path(__file__).parent / "fixtures"
INDEX = json.loads((FIXTURES / "index_quarterhour_sample.json").read_text())
WEEK = json.loads((FIXTURES / "week_sample.json").read_text())

WEEK_START_MS = INDEX["timestamps"][-1]                      # 2026-07-19T22:00:00Z (Mon 00:00 Berlin)
WEEK_START = datetime.fromtimestamp(WEEK_START_MS / 1000, tz=timezone.utc)
UTC = timezone.utc


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# --- select_week_files ---

def test_select_week_files_mid_week_takes_one() -> None:
    # Well inside the current week: only the current week file intersects the 48 h window.
    window_start = WEEK_START + timedelta(days=3)
    assert select_week_files(INDEX["timestamps"], window_start, bootstrapped=True) == [WEEK_START_MS]


def test_select_week_files_boundary_takes_two() -> None:
    # A window that reaches back before the current week also pulls the previous week file.
    window_start = WEEK_START - timedelta(hours=6)
    selected = select_week_files(INDEX["timestamps"], window_start, bootstrapped=True)
    assert selected == INDEX["timestamps"][-2:]             # previous + current week


def test_select_week_files_dead_series_takes_none() -> None:
    # A series whose newest file is long past (nuclear, ended 2024) intersects nothing —
    # the poll is one cheap index GET, no file fetched.
    dead = [_ms(datetime(2024, 1, 22, tzinfo=UTC)), _ms(datetime(2024, 1, 29, tzinfo=UTC))]
    assert select_week_files(dead, datetime(2026, 7, 21, 12, tzinfo=UTC), bootstrapped=True) == []


def test_select_week_files_unbootstrapped_takes_latest_only() -> None:
    # Before the first poll, only the newest file (the backfill) — regardless of the window.
    assert select_week_files(INDEX["timestamps"], WEEK_START, bootstrapped=False) == [WEEK_START_MS]


# --- diff_series ---

def test_diff_series_bootstrap_emits_all_but_windows_only_recent() -> None:
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    old, recent1, recent2, nul = ws - timedelta(hours=1), ws + timedelta(minutes=15), ws + timedelta(minutes=30), ws + timedelta(minutes=45)
    points = [[_ms(old), 10.0], [_ms(recent1), 20.0], [_ms(nul), None], [_ms(recent2), 30.0]]
    obs, new_window = diff_series(points, {}, ws, bootstrapped=False)
    assert [o.value for o in obs] == [10.0, 20.0, 30.0]         # every non-null point, null skipped
    assert all(not o.revised for o in obs)                      # nothing is a revision at bootstrap
    assert set(new_window) == {interval_key(recent1), interval_key(recent2)}  # only in-window remembered


def test_diff_series_new_point_is_a_first_publication() -> None:
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    seen, fresh = ws + timedelta(minutes=15), ws + timedelta(minutes=30)
    window = {interval_key(seen): 20.0}
    obs, new_window = diff_series([[_ms(seen), 20.0], [_ms(fresh), 40.0]], window, ws, bootstrapped=True)
    assert obs == [Observation(fresh, 40.0, revised=False, previous=None)]  # only the new one


def test_diff_series_revision_carries_previous_value() -> None:
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    point = ws + timedelta(minutes=15)
    obs, new_window = diff_series([[_ms(point), 25.0]], {interval_key(point): 20.0}, ws, bootstrapped=True)
    assert obs == [Observation(point, 25.0, revised=True, previous=20.0)]
    assert new_window[interval_key(point)] == 25.0             # window advances to the corrected value


def test_diff_series_unchanged_is_suppressed() -> None:
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    point = ws + timedelta(minutes=15)
    obs, _ = diff_series([[_ms(point), 20.0]], {interval_key(point): 20.0}, ws, bootstrapped=True)
    assert obs == []                                           # no change → nothing emitted


def test_diff_series_immaterial_jitter_is_suppressed() -> None:
    # A sub-REVISION_MIN_DELTA restatement (SMARD's metering jitter) is not a revision:
    # nothing is emitted and the window keeps the last emitted value.
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    point = ws + timedelta(minutes=15)
    window = {interval_key(point): 5091.38}
    obs, new_window = diff_series([[_ms(point), 5091.37]], window, ws, bootstrapped=True)
    assert obs == []                                           # 0.01 MWh move → jitter, not a correction
    assert new_window[interval_key(point)] == 5091.38          # last emitted value kept


def test_diff_series_future_point_enters_the_window() -> None:
    # Day-ahead prices publish points beyond "now"; the window is [window_start, ∞), so a
    # future interval is in-window and emitted.
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    future = ws + timedelta(days=2)
    obs, new_window = diff_series([[_ms(future), 100.0]], {}, ws, bootstrapped=True)
    assert obs == [Observation(future, 100.0, revised=False, previous=None)]
    assert interval_key(future) in new_window


def test_diff_series_disappeared_point_is_retained() -> None:
    # SMARD emits no deletions; a windowed point missing from the file keeps its last value.
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    kept, gone = ws + timedelta(minutes=15), ws + timedelta(minutes=30)
    window = {interval_key(kept): 20.0, interval_key(gone): 30.0}
    obs, new_window = diff_series([[_ms(kept), 20.0]], window, ws, bootstrapped=True)
    assert obs == []
    assert new_window[interval_key(gone)] == 30.0             # carried forward, not dropped


def test_diff_series_ages_out_below_window_start() -> None:
    # A resumed poll whose window advanced: the old point drops from the window (it will
    # show up in aged_out), only in-window news is emitted.
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    stale, fresh = ws - timedelta(hours=1), ws + timedelta(minutes=15)
    window = {interval_key(stale): 5.0}
    obs, new_window = diff_series([[_ms(fresh), 7.0]], window, ws, bootstrapped=True)
    assert obs == [Observation(fresh, 7.0, revised=False, previous=None)]
    assert new_window == {interval_key(fresh): 7.0}           # stale aged out of the window


def test_diff_series_over_the_fixture_week() -> None:
    # Bootstrap over the whole committed week: every non-null slot is emitted once.
    ws = datetime.fromtimestamp(WEEK["series"][0][0] / 1000, tz=UTC) - timedelta(days=1)
    obs, _ = diff_series(WEEK["series"], {}, ws, bootstrapped=False)
    non_null = sum(1 for _, v in WEEK["series"] if v is not None)
    assert len(obs) == non_null and non_null > 0


# --- aged_out ---

def test_aged_out_returns_only_intervals_below_window_start() -> None:
    ws = datetime(2026, 7, 21, 12, tzinfo=UTC)
    stale, live = ws - timedelta(minutes=15), ws + timedelta(minutes=15)
    window = {interval_key(stale): 5.0, interval_key(live): 7.0}
    assert aged_out(window, ws) == [stale]


# --- assemble_mix ---

_T = datetime(2026, 7, 23, 10, tzinfo=UTC)


def test_assemble_mix_full_interval() -> None:
    contributions = {
        "solar_key": {C_ROLE: "source", C_SOURCE: "solar", C_VALUE: 100.0},
        "gas_key":   {C_ROLE: "source", C_SOURCE: "gas",   C_VALUE: 100.0},
        "load_key":  {C_ROLE: "load",   C_VALUE: 500.0},
        "price_key": {C_ROLE: "price",  C_VALUE: 42.0},
    }
    record = assemble_mix(_T, contributions, _T, is_final=True)
    assert record[TOTAL_GENERATION_MWH] == 200.0
    assert record[RENEWABLES_SHARE] == 0.5                     # solar renewable, gas not
    assert record[CO2_G_PER_KWH] == (100 * 45.0 + 100 * 490.0) / 200  # generation-weighted g/kWh
    assert record[LOAD_MWH] == 500.0 and record[PRICE_EUR_MWH] == 42.0
    assert record[GENERATION] == {"solar": 100.0, "gas": 100.0}
    assert record[N_SOURCES] == 2                              # completeness signal
    assert record[IS_FINAL] is True and record[INTERVAL_TS] == _T and record[UPDATED_AT] == _T


def test_assemble_mix_partial_interval_has_no_fabricated_aggregates() -> None:
    # A preliminary interval that has only seen the price: no generation total, no fake 0.
    record = assemble_mix(_T, {"price_key": {C_ROLE: "price", C_VALUE: 30.0}}, _T, is_final=False)
    assert record[PRICE_EUR_MWH] == 30.0
    assert record.get(TOTAL_GENERATION_MWH) is None
    assert record.get(RENEWABLES_SHARE) is None
    assert record.get(CO2_G_PER_KWH) is None
    assert record.get(RESIDUAL_LOAD_MWH) is None
    assert record[N_SOURCES] == 0                              # no generation reported yet
    assert record[IS_FINAL] is False


def test_assemble_mix_all_renewable_share_is_one() -> None:
    contributions = {
        "w": {C_ROLE: "source", C_SOURCE: "wind_onshore", C_VALUE: 300.0},
        "s": {C_ROLE: "source", C_SOURCE: "solar",        C_VALUE: 100.0},
    }
    record = assemble_mix(_T, contributions, _T, is_final=False)
    assert record[RENEWABLES_SHARE] == 1.0


def test_every_seeded_source_has_co2_metadata() -> None:
    # Drift guard: each role=source series in the default basket must have a SOURCE_META
    # entry, or its generation would silently carry a 0 CO₂ factor.
    seeded_sources = {source for _, _, _, role, source, _ in SERIES if role == "source"}
    assert seeded_sources <= set(SOURCE_META)
    assert seeded_sources                                       # the basket actually has sources

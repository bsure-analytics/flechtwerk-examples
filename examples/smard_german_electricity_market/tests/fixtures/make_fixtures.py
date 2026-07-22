"""Regenerate the committed SMARD test fixtures.

NOT a test (pytest ignores it — no ``_test`` suffix). Run it to refresh the tiny,
committed fixtures the logic/runner tiers read:

    uv run python examples/smard_german_electricity_market/tests/fixtures/make_fixtures.py

SMARD's chart-data payload is trivially shaped — an index is ``{"timestamps": [epoch_ms,
…]}`` (one entry per weekly data file) and a week file is ``{"series": [[epoch_ms,
value|null], …]}`` (672 quarter-hour slots). So rather than freeze a slice of the live
feed (which drifts daily and would couple the tests to a capture date), these fixtures are
**synthesized deterministically** from fixed anchors, shaped like a real quarter-hour
generation series: a diurnal curve, realized up to a frontier and ``null`` afterwards (the
not-yet-published future), inside a Monday-00:00-Berlin week. That keeps the tests hermetic
and their arithmetic obvious, while staying byte-faithful to the real wire shape.

Written:
    index_quarterhour_sample.json  — three weekly timestamps (two prior weeks + current)
    week_sample.json               — the current week's 672 quarter-hour points

The anchors below are the contract the tests rely on; change them and the fixtures (and
any test that cites WEEK_START_MS / FRONTIER) move together. Shape only — SMARD.de,
CC BY 4.0, is the real source at runtime.
"""
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent

WEEK = timedelta(days=7)
QUARTER = timedelta(minutes=15)
SLOTS = 672  # quarter-hours in a week

# Monday 2026-07-20 00:00 Europe/Berlin (CEST, +02:00) == 2026-07-19T22:00:00Z.
WEEK_START = datetime(2026, 7, 19, 22, 0, tzinfo=timezone.utc)
# The realized frontier: Thursday ~14:00 Berlin. Points at or before this are published;
# later slots in the week are null (not yet published) — including the whole current day's
# tail, the shape that makes revisions and settlement meaningful.
FRONTIER = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)

BERLIN_OFFSET = timedelta(hours=2)  # CEST for this summer week (fixtures need no tz database)


def _solar_mwh(instant: datetime) -> float:
    """A plausible photovoltaic-like quarter-hour value (MWh): a daytime half-sine, zero at
    night, peaking ~9 GW-equivalent around local noon. Purely for realistic shape."""
    local_hour = (instant + BERLIN_OFFSET).hour + (instant.minute / 60)
    if 5 <= local_hour <= 21:
        return round(9200.0 * math.sin(math.pi * (local_hour - 5) / 16) ** 2, 2)
    return 0.0


def main() -> None:
    index = {"timestamps": [
        int((WEEK_START - 2 * WEEK).timestamp() * 1000),
        int((WEEK_START - WEEK).timestamp() * 1000),
        int(WEEK_START.timestamp() * 1000),
    ]}
    series = []
    for i in range(SLOTS):
        instant = WEEK_START + i * QUARTER
        ms = int(instant.timestamp() * 1000)
        value = _solar_mwh(instant) if instant <= FRONTIER else None
        series.append([ms, value])
    (HERE / "index_quarterhour_sample.json").write_text(json.dumps(index))
    (HERE / "week_sample.json").write_text(json.dumps({"series": series}))

    non_null = sum(1 for _, v in series if v is not None)
    print(f"WEEK_START = {WEEK_START.isoformat()} ({index['timestamps'][-1]} ms)")
    print(f"FRONTIER   = {FRONTIER.isoformat()}  ({non_null} non-null of {SLOTS} slots)")
    print(f"index_quarterhour_sample.json = {(HERE / 'index_quarterhour_sample.json').stat().st_size} bytes")
    print(f"week_sample.json              = {(HERE / 'week_sample.json').stat().st_size} bytes")


if __name__ == "__main__":
    main()

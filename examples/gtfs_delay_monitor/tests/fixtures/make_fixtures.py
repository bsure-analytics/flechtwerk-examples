"""Regenerate the committed GTFS delay-monitor test fixtures from real feeds.

NOT a test (pytest ignores it — no ``_test`` suffix). Run it to refresh the
tiny, committed fixtures used by the logic/runner tiers:

    uv run python examples/gtfs_delay_monitor/tests/fixtures/make_fixtures.py \\
        <rt.pb> <fv.zip>

``rt.pb``  = a snapshot of https://realtime.gtfs.de/realtime-free.pb
``fv.zip`` = https://download.gtfs.de/germany/fv_free/latest.zip

It trims the ~52 MB national RT snapshot and the 374 KB long-distance static
feed down to a handful of long-distance trips that appear in BOTH, chosen for
delay variety (early / on-time / late / severe) and including one with a SKIPPED
stop, and writes:

    rt_sample.pb   — a FeedMessage with just those trips' TripUpdates (+1 alert)
    fv_sample.zip  — agency/routes/trips/stops/stop_times rows for just those trips

The fixtures are frozen bytes, so tests over them are deterministic (the real
feed's header.timestamp is preserved, so trips sit mid-journey as captured).
Data © DELFI e.V. via gtfs.de, CC-BY 4.0.
"""
import csv
import io
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2

HERE = Path(__file__).parent
BERLIN = ZoneInfo("Europe/Berlin")
N_TRIPS = 5


def parse_gtfs_time(hhmmss: str) -> int:
    h, m, s = (int(p) for p in hhmmss.split(":"))
    return h * 3600 + m * 60 + s


def service_time_to_utc(start_date: str, seconds: int) -> datetime:
    day = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=BERLIN)
    noon_minus_12 = day.replace(hour=12) - timedelta(hours=12)
    return (noon_minus_12 + timedelta(seconds=seconds)).astimezone(timezone.utc)


def main(rt_path: str, fv_path: str) -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(Path(rt_path).read_bytes())
    header_ts = feed.header.timestamp
    now = datetime.fromtimestamp(header_ts, tz=timezone.utc)
    tu_by_trip = {e.trip_update.trip.trip_id: e for e in feed.entity if e.HasField("trip_update")}
    alert = next((e for e in feed.entity if e.HasField("alert")), None)

    zf = zipfile.ZipFile(fv_path)

    def rows(name):
        with zf.open(name) as f:
            yield from csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))

    trips = {t["trip_id"]: t for t in rows("trips.txt")}
    routes = {r["route_id"]: r for r in rows("routes.txt")}
    stops = {s["stop_id"]: s for s in rows("stops.txt")}
    stop_times: dict[str, list[dict]] = {}
    for st in rows("stop_times.txt"):
        stop_times.setdefault(st["trip_id"], []).append(st)
    for stl in stop_times.values():
        stl.sort(key=lambda r: int(r["stop_sequence"]))
    agency_rows = list(rows("agency.txt"))

    # Candidates: trips in BOTH feeds, currently mid-journey (first dep < now < last arr).
    cands = []
    for tid, tu_entity in tu_by_trip.items():
        if tid not in trips or tid not in stop_times:
            continue
        stl = stop_times[tid]
        start_date = tu_entity.trip_update.trip.start_date or now.strftime("%Y%m%d")
        first_dep = service_time_to_utc(start_date, parse_gtfs_time(stl[0]["departure_time"]))
        last_arr = service_time_to_utc(start_date, parse_gtfs_time(stl[-1]["arrival_time"]))
        if not (first_dep <= now <= last_arr):
            continue
        stus = tu_entity.trip_update.stop_time_update
        delay = max((s.departure.delay or s.arrival.delay for s in stus if s.HasField("departure") or s.HasField("arrival")), default=0, key=abs)
        skipped = any(s.schedule_relationship == 1 for s in stus)
        cands.append((tid, delay, skipped, len(stl)))

    if not cands:
        sys.exit("no mid-journey overlapping trips found — capture a fresh rt.pb during service hours")

    # Pick for variety: an early, an on-time, a late, a severe, and one with a SKIPPED stop.
    chosen: dict[str, tuple] = {}
    def take(pred):
        for c in sorted(cands, key=lambda c: abs(c[1])):
            if c[0] not in chosen and pred(c):
                chosen[c[0]] = c
                return
    take(lambda c: c[2])                       # a trip with a SKIPPED stop
    take(lambda c: c[1] < -60)                 # early
    take(lambda c: -60 <= c[1] <= 60)          # on-time
    take(lambda c: 360 < c[1] <= 1800)         # late
    take(lambda c: c[1] > 1800)                # severe
    for c in sorted(cands, key=lambda c: -abs(c[1])):  # fill up to N with the most delayed
        if len(chosen) >= N_TRIPS:
            break
        chosen.setdefault(c[0], c)
    chosen_ids = list(chosen)[:N_TRIPS]

    # --- write rt_sample.pb ---
    out = gtfs_realtime_pb2.FeedMessage()
    out.header.CopyFrom(feed.header)
    for tid in chosen_ids:
        out.entity.add().CopyFrom(tu_by_trip[tid])
    if alert is not None:
        out.entity.add().CopyFrom(alert)   # keep one alert so the ignore-path is exercised
    (HERE / "rt_sample.pb").write_bytes(out.SerializeToString())

    # --- write fv_sample.zip (only the referenced rows) ---
    used_routes, used_stops = set(), set()
    for tid in chosen_ids:
        used_routes.add(trips[tid]["route_id"])
        for st in stop_times[tid]:
            used_stops.add(st["stop_id"])

    def csv_bytes(fieldnames, records):
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(records)
        return buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(HERE / "fv_sample.zip", "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("agency.txt", csv_bytes(list(agency_rows[0]), agency_rows))
        z.writestr("routes.txt", csv_bytes(list(next(iter(routes.values()))),
                                           [routes[r] for r in used_routes]))
        z.writestr("trips.txt", csv_bytes(list(trips[chosen_ids[0]]),
                                          [trips[t] for t in chosen_ids]))
        z.writestr("stops.txt", csv_bytes(list(next(iter(stops.values()))),
                                          [stops[s] for s in used_stops]))
        z.writestr("stop_times.txt", csv_bytes(list(stop_times[chosen_ids[0]][0]),
                                               [st for t in chosen_ids for st in stop_times[t]]))

    # --- verify + report ---
    assert (HERE / "rt_sample.pb").stat().st_size < 60_000, "rt_sample.pb too big"
    assert (HERE / "fv_sample.zip").stat().st_size < 60_000, "fv_sample.zip too big"
    print(f"header.timestamp = {header_ts} ({now.isoformat()})")
    print(f"chose {len(chosen_ids)} trips:")
    for tid in chosen_ids:
        _, delay, skipped, n = chosen[tid]
        line = routes[trips[tid]["route_id"]]["route_short_name"]
        print(f"  {tid} {line!r:>10}  delay~{delay:+5d}s  skipped={skipped}  stops={n}")
    print(f"rt_sample.pb  = {(HERE / 'rt_sample.pb').stat().st_size} bytes")
    print(f"fv_sample.zip = {(HERE / 'fv_sample.zip').stat().st_size} bytes")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])

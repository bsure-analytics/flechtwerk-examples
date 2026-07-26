"""Tier 1 — pure logic. No framework machinery, no mocks, no network.

Drives the stages' pure cores directly: the ingest helpers (``parse_area_csv`` /
``detection_id`` / ``acquired_at`` / ``normalize_detection`` / ``prune_seen``), the clustering
core in ``tracking.py``, and the ``run_tracker`` fold driven as a bare async generator over
hand-built ``State``/``IncomingMessage`` — the SMARD ``run_mix`` and odds ``run_radar`` style.

The CSV fixtures are trimmed **real** captures (see ``fixtures/PROVENANCE.md``), so the parsing
and clustering assertions are pinned to rows NASA actually served; the geometry and lifecycle
cases build tiny fires inline so the arithmetic is visible.
"""
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from flechtwerk import Config, Event, IncomingMessage, Message, State

from examples.wildfire_watch.attributes import (
    ACQUIRED_AT,
    AS_OF,
    BRIGHT_TI4,
    CONFIDENCE,
    DAYNIGHT,
    DETECTION_ID,
    DETECTIONS,
    EVENTS_TOPIC,
    FETCHED_AT,
    F_COUNT,
    F_FIRST_SEEN,
    F_FRP_MAX,
    F_FRP_SUM,
    F_LAST_SEEN,
    F_LAT,
    F_LON,
    F_MAX_LAT,
    F_MAX_LON,
    F_MIN_LAT,
    F_MIN_LON,
    F_SATELLITES,
    FIRE_ID,
    FIRES,
    FIRST_SEEN,
    FRP,
    FRP_MAX,
    FRP_SUM,
    INSTRUMENT,
    KIND,
    LAST_SEEN,
    LAT,
    LON,
    MERGED_INTO,
    NEW_DETECTIONS,
    OCCURRED_AT,
    REGION,
    SATELLITE,
    SCAN,
    STATUS,
    STATUS_TOPIC,
    SWEEP_AT,
    TRACK,
)
from examples.wildfire_watch.ingest import (
    SEEN_HARD_CAP,
    acquired_at,
    detection_id,
    normalize_detection,
    parse_area_csv,
    prune_seen,
)
from examples.wildfire_watch.request import box_of, check_box_size, overlap, slugify
from examples.wildfire_watch.tiles import (
    LAND_CELLS,
    Tile,
    cell_slug,
    parse_points,
    quadtree,
)
from examples.wildfire_watch.tracker import (
    ACTIVE,
    EXTINGUISHED,
    IGNITION,
    MERGED,
    run_tracker,
)
from examples.wildfire_watch.tracking import (
    EXTINGUISH_AFTER,
    LINK_KM,
    MAX_FIRES,
    Detection,
    absorb,
    detection_links,
    evict_stalest,
    expired,
    found_fire,
    link_spans,
    merge_fires,
)

FIXTURES = Path(__file__).parent / "fixtures"
N20_CSV = (FIXTURES / "firms_n20.csv").read_text()
N20_LATER_CSV = (FIXTURES / "firms_n20_later.csv").read_text()
N21_CSV = (FIXTURES / "firms_n21.csv").read_text()
ERROR_BODY = (FIXTURES / "firms_error.txt").read_text()

UTC = timezone.utc
REGION_SLUG = "alentejo-portugal"
_T = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)

HEADER = ("latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,"
          "confidence,version,bright_ti5,frp,daynight")


def _row(**overrides: str) -> dict[str, str]:
    """One synthetic raw CSV row, shaped exactly like a real one (all values strings)."""
    return {
        "latitude": "37.54301", "longitude": "-7.05702", "bright_ti4": "339.96",
        "scan": "0.53", "track": "0.42", "acq_date": "2026-07-24", "acq_time": "1353",
        "satellite": "N20", "instrument": "VIIRS", "confidence": "n", "version": "2.0NRT",
        "bright_ti5": "311.66", "frp": "9.5", "daynight": "D",
    } | overrides


# --- request.py: slugify / overlap / box_of (pure ops helpers) ---

@pytest.mark.parametrize("name, slug", [
    ("Alentejo, Portugal", "alentejo-portugal"),
    ("Attica, Greece", "attica-greece"),
    ("  Extremadura  ", "extremadura"),
    ("Provence-Alpes-Côte d'Azur", "provence-alpes-c-te-d-azur"),   # non-ASCII collapses
    ("British Columbia (BC)", "british-columbia-bc"),
])
def test_slugify_is_deterministic_and_url_safe(name: str, slug: str) -> None:
    assert slugify(name) == slug
    assert slugify(name) == slugify(name)                            # same name → same key


@pytest.mark.parametrize("name", ["", "   ", "!!!", "…"])
def test_slugify_yields_an_empty_slug_for_nameless_input(name: str) -> None:
    # request.py turns this into a SystemExit rather than writing a keyless record.
    assert slugify(name) == ""


def test_overlap_returns_the_intersection_rectangle() -> None:
    # The two live Iberian regions that exposed the double-tracking.
    alentejo = (-8.9606, 36.9551, -6.7606, 39.1551)
    extremadura = (-7.6417, 37.8410, -4.5476, 40.5867)
    shared = overlap(alentejo, extremadura)
    assert shared == (-7.6417, 37.8410, -6.7606, 39.1551)
    assert overlap(extremadura, alentejo) == shared                  # symmetric


def test_overlap_is_none_for_disjoint_boxes() -> None:
    assert overlap((0.0, 0.0, 1.0, 1.0), (2.0, 2.0, 3.0, 3.0)) is None
    assert overlap((0.0, 0.0, 1.0, 1.0), (2.0, 0.0, 3.0, 1.0)) is None   # apart in longitude
    assert overlap((0.0, 0.0, 1.0, 1.0), (0.0, 2.0, 1.0, 3.0)) is None   # apart in latitude


def test_overlap_treats_touching_edges_as_disjoint() -> None:
    # A shared boundary line holds no area, so no detection can fall inside both.
    assert overlap((0.0, 0.0, 1.0, 1.0), (1.0, 0.0, 2.0, 1.0)) is None
    assert overlap((0.0, 0.0, 1.0, 1.0), (0.0, 1.0, 1.0, 2.0)) is None


def test_overlap_of_a_contained_box_is_that_box() -> None:
    outer, inner = (0.0, 0.0, 10.0, 10.0), (2.0, 3.0, 4.0, 5.0)
    assert overlap(outer, inner) == inner and overlap(inner, outer) == inner


def test_overlap_of_a_box_with_itself_is_itself() -> None:
    box = (-8.9606, 36.9551, -6.7606, 39.1551)
    assert overlap(box, box) == box


def test_box_of_reads_a_cached_config_record() -> None:
    config = Config.wrap({"region": "r", "name": "R",
                          "west": -8.0, "south": 37.0, "east": -6.0, "north": 39.0})
    assert box_of(config) == (-8.0, 37.0, -6.0, 39.0)


@pytest.mark.parametrize("partial", [
    {}, {"west": -8.0}, {"west": -8.0, "south": 37.0, "east": -6.0},   # any missing edge
])
def test_box_of_is_none_for_a_name_only_record(partial: dict) -> None:
    # enrich_config resolves these at runtime, so the request tool can't intersect them.
    assert box_of(Config.wrap({"region": "r", "name": "R", **partial})) is None


def test_check_box_size_passes_a_sane_box(capsys: pytest.CaptureFixture) -> None:
    check_box_size(-1.365, 44.0938903, 0.4146498, 45.7018694)     # Gironde + pad
    assert capsys.readouterr().out == ""


def test_check_box_size_warns_between_the_thresholds(capsys: pytest.CaptureFixture) -> None:
    check_box_size(-5.55, 41.16, 9.93, 51.41)                     # France métropolitaine + pad
    assert "⚠️" in capsys.readouterr().out


def test_check_box_size_refuses_an_absurd_box() -> None:
    # The live incident: Nominatim's box for "France" spans the whole Republic — Kerguelen to
    # French Polynesia, 350.9° × 101.7° — and one region that size crashlooped both stages.
    with pytest.raises(SystemExit, match="request-wildfire world"):
        check_box_size(-178.4873749, -50.3187169, 172.4057152, 51.4055721)


# --- tiles: the world quadtree ---

def test_cell_slug_covers_all_hemisphere_corners() -> None:
    assert cell_slug(20, -20) == "e020s20"
    assert cell_slug(-10, 40) == "w010n40"
    assert cell_slug(-180, -60) == "w180s60"
    assert cell_slug(170, 80) == "e170n80"


def test_quadtree_quiet_land_stays_at_base_cells() -> None:
    tiles = quadtree([], land_cells=((0, 0), (10, 0)), threshold=10, min_deg=5.0)
    assert [t.slug for t in tiles] == ["e000n00", "e010n00"]
    assert tiles[0] == Tile("e000n00", 0.0, 0.0, 10.0, 10.0)


def test_quadtree_hot_cell_splits_into_all_four_quadrants() -> None:
    # 11 points in one quadrant: the cell splits, the THREE EMPTY quadrants are kept (fires
    # ignite where there were none), and the hot 5° child stops at min_deg.
    points = [(7.0 + i * 0.1, 7.0) for i in range(11)]
    tiles = quadtree(points, land_cells=((0, 0),), threshold=10, min_deg=5.0)
    assert [t.slug for t in tiles] == ["e000n00-0", "e000n00-1", "e000n00-2", "e000n00-3"]
    assert Tile("e000n00-3", 5.0, 5.0, 10.0, 10.0) in tiles


def test_quadtree_splits_recursively_until_min_deg() -> None:
    points = [(1.0, 1.0)] * 11                               # one hot spot in the SW of the SW
    tiles = quadtree(points, land_cells=((0, 0),), threshold=10, min_deg=2.5)
    slugs = {t.slug for t in tiles}
    assert "e000n00-0-0" in slugs                            # 2.5° leaf: still hot, at the floor
    assert "e000n00-3" in slugs                              # the quiet 5° sibling stays whole
    assert len(tiles) == 7                                   # 4 quadrants, the SW one split again


def test_quadtree_adds_offshore_cells_with_detections() -> None:
    # A gas-flare cluster in an ocean cell joins the base set by itself — the land grid is a
    # floor, not a filter.
    tiles = quadtree([(-155.0, 15.0)], land_cells=((0, 0),), threshold=10, min_deg=5.0)
    assert {t.slug for t in tiles} == {"e000n00", "w160n10"}


def test_quadtree_ignores_the_far_south() -> None:
    assert quadtree([(0.0, -75.0)], land_cells=(), threshold=10, min_deg=5.0) == []


def test_quadtree_is_deterministic_under_input_order() -> None:
    points = [(7.0 + i * 0.1, 3.0) for i in range(11)]
    assert (quadtree(points, land_cells=((0, 0),), threshold=10, min_deg=5.0)
            == quadtree(points[::-1], land_cells=((0, 0),), threshold=10, min_deg=5.0))


def test_world_land_grid_is_plausible() -> None:
    # The embedded grid: on the 10° lattice, Antarctica excluded, both hemispheres populated.
    assert len(LAND_CELLS) == 267
    assert all(w % 10 == 0 and s % 10 == 0 and -180 <= w < 180 and -60 <= s < 90
               for w, s in LAND_CELLS)
    assert (20, -20) in LAND_CELLS and (-120, 40) in LAND_CELLS   # savanna belt, US west


def test_parse_points_reads_lon_lat_pairs() -> None:
    assert parse_points("latitude,longitude,bright_ti4\n38.9,-9.0,300.0\n") == [(-9.0, 38.9)]


def test_parse_points_rejects_a_non_csv_body() -> None:
    with pytest.raises(RuntimeError, match="public 24h CSV"):
        parse_points("<html>maintenance</html>")


# --- parse_area_csv ---

def test_parse_area_csv_reads_the_real_capture() -> None:
    rows = parse_area_csv(N20_CSV)
    assert len(rows) == 21                                     # the 2026-07-24 rows
    assert rows[0]["latitude"] == "38.92123" and rows[0]["satellite"] == "N20"
    assert set(rows[0]) == set(HEADER.split(","))               # all 14 columns, values as strings


def test_parse_area_csv_later_poll_is_a_superset() -> None:
    first, later = parse_area_csv(N20_CSV), parse_area_csv(N20_LATER_CSV)
    assert len(later) == 32
    assert {detection_id(r) for r in first} < {detection_id(r) for r in later}


def test_parse_area_csv_header_only_is_a_quiet_region() -> None:
    assert parse_area_csv(HEADER + "\n") == []                  # normal, not an error


def test_parse_area_csv_tolerates_a_bom() -> None:
    assert len(parse_area_csv("﻿" + N20_CSV)) == 21


@pytest.mark.parametrize("body", [ERROR_BODY, "", "   ", "<html>maintenance</html>"])
def test_parse_area_csv_rejects_a_non_csv_body(body: str) -> None:
    # A bad key is really a 400 (raise_for_status catches it); this guard is what stops a
    # maintenance page or quota notice served as 200 from reading as "no fires".
    with pytest.raises(RuntimeError, match="FIRMS did not return area CSV"):
        parse_area_csv(body)


def test_parse_area_csv_error_message_carries_a_body_snippet() -> None:
    with pytest.raises(RuntimeError, match="Invalid MAP_KEY"):
        parse_area_csv(ERROR_BODY)


# --- acquired_at ---

@pytest.mark.parametrize("acq_time, hour, minute", [
    ("48", 0, 48),        # the documented unpadded edge case: 00:48
    ("230", 2, 30),       # real fixture value
    ("1353", 13, 53),     # real fixture value
    ("0", 0, 0),
    ("2359", 23, 59),
])
def test_acquired_at_parses_unpadded_hhmm(acq_time: str, hour: int, minute: int) -> None:
    when = acquired_at(_row(acq_time=acq_time))
    assert (when.hour, when.minute) == (hour, minute)
    assert when.tzinfo is not None and when.utcoffset() == timedelta(0)   # aware UTC


# --- detection_id ---

def test_detection_id_is_deterministic_12_hex() -> None:
    identity = detection_id(_row())
    assert len(identity) == 12 and int(identity, 16) >= 0
    assert identity == detection_id(_row())                     # same row → same id


def test_detection_id_ignores_columns_outside_the_identity() -> None:
    # Identity is position + time + satellite. A revised brightness or FRP for the same pixel
    # must NOT create a second detection.
    assert detection_id(_row(frp="99.9", bright_ti4="367", confidence="h")) == detection_id(_row())


@pytest.mark.parametrize("field, value", [
    ("latitude", "37.54302"), ("longitude", "-7.05703"),
    ("acq_date", "2026-07-25"), ("acq_time", "1354"), ("satellite", "N21"),
])
def test_detection_id_changes_with_each_identity_field(field: str, value: str) -> None:
    assert detection_id(_row(**{field: value})) != detection_id(_row())


def test_detection_id_hashes_raw_strings_not_parsed_floats() -> None:
    # "37.54301" and "37.5430100" are the same float but different raw strings — and FIRMS is
    # consistent about its formatting, so treating them as one would require reparsing. Hashing
    # the raw text is what keeps the seen-set stable across restarts.
    assert detection_id(_row(latitude="37.5430100")) != detection_id(_row())


def test_detection_id_is_collision_free_over_the_whole_capture() -> None:
    rows = parse_area_csv(N20_LATER_CSV) + parse_area_csv(N21_CSV)
    assert len({detection_id(r) for r in rows}) == len(rows)


# --- normalize_detection ---

def test_normalize_detection_projects_every_field() -> None:
    detection = normalize_detection(_row(), REGION_SLUG, _T)
    assert detection[KIND] == "detection" and detection[REGION] == REGION_SLUG
    assert detection[DETECTION_ID] == detection_id(_row())
    assert (detection[LAT], detection[LON]) == (37.54301, -7.05702)
    assert detection[ACQUIRED_AT] == datetime(2026, 7, 24, 13, 53, tzinfo=UTC)
    assert detection[SATELLITE] == "N20" and detection[INSTRUMENT] == "VIIRS"
    assert detection[CONFIDENCE] == "n" and detection[DAYNIGHT] == "D"
    assert (detection[SCAN], detection[TRACK]) == (0.53, 0.42)
    assert detection[FRP] == 9.5


def test_normalize_detection_accepts_integer_valued_columns() -> None:
    # bright_ti4 saturates at a bare "367"; the FLOAT codec rejects int, so float() is required.
    assert normalize_detection(_row(bright_ti4="367"), REGION_SLUG, _T)[BRIGHT_TI4] == 367.0


@pytest.mark.parametrize("empty", ["", "  "])
def test_normalize_detection_empty_measurement_is_absent(empty: str) -> None:
    detection = normalize_detection(_row(frp=empty), REGION_SLUG, _T)
    assert detection.get(FRP) is None                           # absent, never a fabricated 0.0
    assert detection[BRIGHT_TI4] == 339.96                      # the other optionals survive


def test_normalize_detection_keeps_a_genuine_zero_frp() -> None:
    # 0.0 FRP occurs in real FIRMS data — absence is decided by the empty string, not falsiness.
    assert normalize_detection(_row(frp="0.0"), REGION_SLUG, _T)[FRP] == 0.0


def test_normalize_detection_is_json_serializable() -> None:
    raw = json.loads(json.dumps(normalize_detection(_row(), REGION_SLUG, _T).raw))
    assert raw["acquired_at"] == "2026-07-24T13:53:00Z" and raw["fetched_at"] == "2026-07-25T18:00:00Z"


# --- prune_seen ---

def test_prune_seen_drops_dates_below_the_window() -> None:
    seen = {"2026-07-20": ["a"], "2026-07-22": ["b"], "2026-07-24": ["c"], "2026-07-25": ["d"]}
    # DAY_RANGE = 2 keeps max, max-1, max-2 (one day of grace past the requested window).
    assert prune_seen(seen, date(2026, 7, 25)) == {"2026-07-24": ["c"], "2026-07-25": ["d"]}


def test_prune_seen_passes_an_in_window_set_through_unchanged() -> None:
    seen = {"2026-07-24": ["a", "b"], "2026-07-25": ["c"]}
    assert prune_seen(seen, date(2026, 7, 25)) == seen


def test_prune_seen_hard_cap_drops_whole_oldest_buckets_first() -> None:
    seen = {
        "2026-07-23": [f"old{i:05d}" for i in range(8)],
        "2026-07-24": [f"mid{i:05d}" for i in range(8)],
        "2026-07-25": [f"new{i:05d}" for i in range(8)],
    }
    pruned = prune_seen(seen, date(2026, 7, 25), day_range=5, hard_cap=20)
    assert set(pruned) == {"2026-07-24", "2026-07-25"}          # oldest bucket sacrificed
    assert sum(len(v) for v in pruned.values()) == 16 <= 20


def test_prune_seen_hard_cap_trims_within_a_single_bucket() -> None:
    # One monster day bucket (the metropolitan-France / world-watch case): whole-bucket drops
    # can't shrink it, so its OLDEST ids are trimmed and the newest kept — the record stays
    # bounded no matter what, which is the whole point of a hard cap.
    seen = {"2026-07-25": [f"id{i:05d}" for i in range(50)]}
    pruned = prune_seen(seen, date(2026, 7, 25), hard_cap=10)
    assert pruned == {"2026-07-25": [f"id{i:05d}" for i in range(40, 50)]}


def test_prune_seen_hard_cap_drops_buckets_before_trimming_the_survivor() -> None:
    # Whole-bucket drops stay the first resort; the intra-bucket trim only finishes the job.
    seen = {"2026-07-24": [f"old{i:02d}" for i in range(30)],
            "2026-07-25": [f"new{i:02d}" for i in range(15)]}
    pruned = prune_seen(seen, date(2026, 7, 25), hard_cap=10)
    assert pruned == {"2026-07-25": [f"new{i:02d}" for i in range(5, 15)]}


@pytest.mark.parametrize("seen", [
    {"2026-07-25": [f"id{i:05d}" for i in range(50)]},                       # one monster bucket
    {"2026-07-24": ["a"] * 30, "2026-07-25": ["b"] * 30},                    # two over-cap buckets
    {"2026-07-23": ["x"] * 5, "2026-07-24": ["y"] * 5, "2026-07-25": ["z"] * 5},  # under the cap
    {},                                                                       # nothing at all
])
def test_prune_seen_result_never_exceeds_the_cap(seen: dict[str, list[str]]) -> None:
    # The invariant a crashlooping stage taught us to test directly: whatever the shape of the
    # input, the surviving id count is ≤ hard_cap, so the State record size is bounded.
    pruned = prune_seen(seen, date(2026, 7, 25), hard_cap=10)
    assert sum(len(ids) for ids in pruned.values()) <= 10


def test_prune_seen_warns_when_the_cap_bites(caplog: pytest.LogCaptureFixture) -> None:
    seen = {"2026-07-24": ["a"] * 30, "2026-07-25": ["b"] * 5}
    with caplog.at_level("WARNING"):
        prune_seen(seen, date(2026, 7, 25), hard_cap=10)
    assert "2026-07-24" in caplog.text and "cap" in caplog.text


def test_prune_seen_warns_when_the_trim_bites(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        prune_seen({"2026-07-25": ["a"] * 30}, date(2026, 7, 25), hard_cap=10)
    assert "trimmed" in caplog.text and "20" in caplog.text


def test_seen_hard_cap_leaves_changelog_headroom() -> None:
    # 12 hex chars + JSON quoting/comma ≈ 15 bytes; the cap must stay well under the broker's
    # ~1 MB per-record ceiling, because one State is one changelog record.
    assert SEEN_HARD_CAP * 15 < 500_000


# --- link_spans / detection_links ---

def test_link_spans_latitude_is_uniform_longitude_is_not() -> None:
    d_lat_eq, d_lon_eq = link_spans(0.0)
    d_lat_60, d_lon_60 = link_spans(60.0)
    assert d_lat_eq == pytest.approx(d_lat_60)                  # latitude degrees don't vary
    assert d_lon_eq == pytest.approx(LINK_KM / 111.32, rel=1e-6)
    # cos(60°) = 0.5, so a degree of longitude is half as wide → the span doubles.
    assert d_lon_60 == pytest.approx(2 * d_lon_eq, rel=1e-6)


def test_link_spans_stays_finite_at_the_pole() -> None:
    d_lat, d_lon = link_spans(90.0)
    assert d_lat > 0 and 0 < d_lon < 200                        # clamped, not a division blow-up


def _point_fire(lat: float, lon: float, *, when: datetime = _T, frp: float | None = None) -> dict:
    _, entry = found_fire(Detection("d0", lat, lon, when, "N20", frp))
    return entry


def test_detection_links_inside_and_outside_at_the_equator() -> None:
    fire = _point_fire(0.0, 0.0)
    reach = LINK_KM / 111.32
    assert detection_links(fire, reach * 0.9, 0.0)              # just inside
    assert not detection_links(fire, reach * 1.1, 0.0)          # just outside


def test_detection_links_accounts_for_cos_lat_at_60_north() -> None:
    fire = _point_fire(60.0, 20.0)
    reach_lon = LINK_KM / (111.32 * 0.5)                        # cos(60°) = 0.5
    assert detection_links(fire, 60.0, 20.0 + reach_lon * 0.9)
    assert not detection_links(fire, 60.0, 20.0 + reach_lon * 1.1)
    # A naive fixed-degree threshold would wrongly reject this one — the classic geo bug.
    assert detection_links(fire, 60.0, 20.0 + LINK_KM / 111.32 * 1.5)


def test_detection_links_expands_the_whole_bbox_not_a_centroid() -> None:
    # A long fire front: the box spans 0.4° of latitude, so a detection just past its far end
    # links even though it is ~20 km from the centroid.
    fire = absorb(_point_fire(37.0, -7.0), Detection("d1", 37.4, -7.0, _T, "N20", None))
    assert detection_links(fire, 37.41, -7.0)
    assert not detection_links(fire, 37.5, -7.0)


# --- found_fire ---

def test_found_fire_id_derives_from_the_founding_detection() -> None:
    fire_id, entry = found_fire(Detection("abc123def456", 37.5, -7.0, _T, "N20", 12.5))
    assert fire_id == "F-abc123def456"                          # deterministic across replays
    assert (entry[F_LAT], entry[F_LON]) == (37.5, -7.0)
    assert entry[F_MIN_LAT] == entry[F_MAX_LAT] == 37.5         # footprint starts as a point
    assert entry[F_COUNT] == 1 and entry[F_SATELLITES] == ["N20"]
    assert entry[F_FIRST_SEEN] == entry[F_LAST_SEEN] == _T
    assert entry[F_FRP_SUM] == entry[F_FRP_MAX] == 12.5


def test_found_fire_without_frp_carries_no_frp_keys() -> None:
    _, entry = found_fire(Detection("d0", 37.5, -7.0, _T, "N20", None))
    assert F_FRP_SUM not in entry and F_FRP_MAX not in entry     # absent, not 0.0


# --- absorb ---

def test_absorb_grows_the_footprint_and_running_centroid() -> None:
    fire = _point_fire(37.0, -7.0, frp=10.0)
    grown = absorb(fire, Detection("d1", 37.02, -6.98, _T, "N21", 30.0))
    assert grown[F_COUNT] == 2
    assert grown[F_LAT] == pytest.approx(37.01) and grown[F_LON] == pytest.approx(-6.99)
    assert (grown[F_MIN_LAT], grown[F_MAX_LAT]) == (37.0, 37.02)
    assert (grown[F_MIN_LON], grown[F_MAX_LON]) == (-7.0, -6.98)
    assert grown[F_SATELLITES] == ["N20", "N21"]                # cross-satellite confirmation
    assert grown[F_FRP_SUM] == 40.0 and grown[F_FRP_MAX] == 30.0


def test_absorb_centroid_is_count_weighted() -> None:
    fire = _point_fire(0.0, 0.0)
    for i in range(3):
        fire = absorb(fire, Detection(f"d{i}", 1.0, 0.0, _T, "N20", None))
    assert fire[F_LAT] == pytest.approx(0.75)                   # (0 + 1 + 1 + 1) / 4


def test_absorb_last_seen_never_regresses_on_late_data() -> None:
    # An NRT slice delivering an OLDER pixel must not make a live fire look stale.
    fire = _point_fire(37.0, -7.0, when=_T)
    late = absorb(fire, Detection("d1", 37.0, -7.0, _T - timedelta(hours=6), "N20", None))
    assert late[F_LAST_SEEN] == _T                               # max fold
    assert late[F_FIRST_SEEN] == _T - timedelta(hours=6)         # min fold moved it back


def test_absorb_first_seen_never_advances() -> None:
    fire = _point_fire(37.0, -7.0, when=_T)
    later = absorb(fire, Detection("d1", 37.0, -7.0, _T + timedelta(hours=3), "N20", None))
    assert later[F_FIRST_SEEN] == _T and later[F_LAST_SEEN] == _T + timedelta(hours=3)


def test_absorb_folds_frp_around_absent_values() -> None:
    fire = _point_fire(37.0, -7.0)                              # no FRP at all
    still_none = absorb(fire, Detection("d1", 37.0, -7.0, _T, "N20", None))
    assert F_FRP_SUM not in still_none
    first = absorb(still_none, Detection("d2", 37.0, -7.0, _T, "N20", 5.0))
    assert first[F_FRP_SUM] == 5.0 and first[F_FRP_MAX] == 5.0   # starts from the first real value
    assert absorb(first, Detection("d3", 37.0, -7.0, _T, "N20", None))[F_FRP_SUM] == 5.0


def test_absorb_does_not_mutate_the_input() -> None:
    fire = _point_fire(37.0, -7.0, frp=10.0)
    absorb(fire, Detection("d1", 38.0, -6.0, _T, "N21", 99.0))
    assert fire[F_COUNT] == 1 and fire[F_MAX_LAT] == 37.0 and fire[F_SATELLITES] == ["N20"]


# --- merge_fires ---

def _aged_fire(lat: float, lon: float, first_seen: datetime, **kw) -> dict:
    return _point_fire(lat, lon, when=first_seen, **kw)


def test_merge_wildfire_survivor_is_the_earliest_first_seen() -> None:
    entries = {
        "F-young": _aged_fire(37.0, -7.0, _T),
        "F-old": _aged_fire(37.1, -7.1, _T - timedelta(hours=5)),
    }
    survivor, merged, absorbed = merge_fires(entries)
    assert survivor == "F-old" and absorbed == ["F-young"]
    assert merged[F_FIRST_SEEN] == _T - timedelta(hours=5) and merged[F_LAST_SEEN] == _T
    assert merged[F_COUNT] == 2
    assert (merged[F_MIN_LAT], merged[F_MAX_LAT]) == (37.0, 37.1)


def test_merge_wildfire_breaks_first_seen_ties_lexicographically() -> None:
    entries = {"F-bbb": _aged_fire(37.0, -7.0, _T), "F-aaa": _aged_fire(37.1, -7.1, _T)}
    survivor, _, absorbed = merge_fires(entries)
    assert survivor == "F-aaa" and absorbed == ["F-bbb"]         # never dict-order dependent


def test_merge_wildfire_folds_frp_and_satellites_across_parts() -> None:
    a = _aged_fire(37.0, -7.0, _T, frp=10.0)
    b = absorb(_aged_fire(37.1, -7.1, _T + timedelta(hours=1)),
               Detection("d9", 37.1, -7.1, _T + timedelta(hours=1), "N21", 50.0))
    _, merged, _ = merge_fires({"F-a": a, "F-b": b})
    assert merged[F_FRP_SUM] == 60.0 and merged[F_FRP_MAX] == 50.0
    assert merged[F_SATELLITES] == ["N20", "N21"]
    assert merged[F_COUNT] == 3


def test_merge_wildfire_centroid_is_weighted_by_detection_count() -> None:
    heavy = _aged_fire(0.0, 0.0, _T)
    for i in range(3):
        heavy = absorb(heavy, Detection(f"h{i}", 0.0, 0.0, _T, "N20", None))   # count 4 at 0.0
    light = _aged_fire(1.0, 0.0, _T)                                          # count 1 at 1.0
    _, merged, _ = merge_fires({"F-h": heavy, "F-l": light})
    assert merged[F_LAT] == pytest.approx(0.2)                   # (4·0 + 1·1) / 5


def test_merge_wildfire_of_one_is_the_identity_fold() -> None:
    only = _aged_fire(37.0, -7.0, _T, frp=3.0)
    survivor, merged, absorbed = merge_fires({"F-only": only})
    assert survivor == "F-only" and absorbed == []
    assert merged[F_COUNT] == 1 and merged[F_FRP_SUM] == 3.0


def test_merge_wildfire_without_frp_anywhere_stays_frp_free() -> None:
    _, merged, _ = merge_fires({"F-a": _aged_fire(37.0, -7.0, _T),
                                "F-b": _aged_fire(37.1, -7.1, _T)})
    assert F_FRP_SUM not in merged and F_FRP_MAX not in merged


# --- expired ---

def test_expired_is_measured_in_event_time() -> None:
    fire = _point_fire(37.0, -7.0, when=_T)
    assert not expired(fire, _T + EXTINGUISH_AFTER)              # exactly at the horizon: alive
    assert expired(fire, _T + EXTINGUISH_AFTER + timedelta(seconds=1))
    assert not expired(fire, _T)                                 # a sweep at its own last sighting


def test_expired_respects_an_injected_ttl() -> None:
    fire = _point_fire(37.0, -7.0, when=_T)
    assert expired(fire, _T + timedelta(hours=2), ttl=timedelta(hours=1))


# --- evict_stalest (the fire-bucket hard cap) ---

def test_evict_stalest_under_the_cap_is_identity() -> None:
    fires = {"F-a": _point_fire(37.0, -7.0)}
    kept, evicted = evict_stalest(fires, cap=1)
    assert kept is fires and evicted == {}


def test_evict_stalest_removes_the_oldest_last_seen_first() -> None:
    fires = {
        "F-old": _point_fire(37.0, -7.0, when=_T - timedelta(hours=6)),
        "F-mid": _point_fire(37.2, -7.0, when=_T - timedelta(hours=3)),
        "F-new": _point_fire(37.4, -7.0, when=_T),
    }
    kept, evicted = evict_stalest(fires, cap=1)
    assert set(kept) == {"F-new"} and set(evicted) == {"F-old", "F-mid"}


def test_evict_stalest_breaks_last_seen_ties_by_id() -> None:
    fires = {"F-b": _point_fire(37.0, -7.0), "F-a": _point_fire(37.2, -7.0)}
    kept, evicted = evict_stalest(fires, cap=1)
    assert set(kept) == {"F-b"} and set(evicted) == {"F-a"}    # never dict-order dependent


def test_max_fires_leaves_changelog_headroom() -> None:
    # ~322 bytes of JSON per fire entry, measured on live savanna data (2026-07-26); the whole
    # bucket is one changelog record and must stay well under the ~1 MB ceiling.
    assert MAX_FIRES * 330 < 700_000


# --- run_tracker (bare async generator, hand-built State/messages) ---

def _detection_event(lat: float, lon: float, *, identity: str = "d0",
                     acquired: datetime = _T, satellite: str = "N20",
                     frp: float | None = 10.0) -> Event:
    event = Event({
        KIND: "detection", REGION: REGION_SLUG, DETECTION_ID: identity,
        LAT: lat, LON: lon, ACQUIRED_AT: acquired, SATELLITE: satellite,
        INSTRUMENT: "VIIRS", CONFIDENCE: "n", SCAN: 0.4, TRACK: 0.4,
        DAYNIGHT: "D", FETCHED_AT: acquired,
    })
    if frp is not None:
        event[FRP] = frp
    return event


def _sweep_event(sweep_at: datetime, new_detections: int = 0) -> Event:
    return Event({KIND: "sweep", REGION: REGION_SLUG,
                  SWEEP_AT: sweep_at, NEW_DETECTIONS: new_detections})


def _msg(value: Event) -> IncomingMessage:
    return IncomingMessage(key=REGION_SLUG, offset=0, partition=0, timestamp=None,
                           topic="wildfire-detections", value=value)


async def _drive(state: State, value: Event) -> tuple[list[Message], State | None]:
    """Run ``run_tracker`` over one record; split its yields into messages and the final state."""
    messages: list[Message] = []
    new_state: State | None = None
    async for item in run_tracker(state, _msg(value)):
        if isinstance(item, State):
            new_state = item
        else:
            messages.append(item)
    return messages, new_state


def _by_topic(messages: list[Message], topic: str) -> list[Event]:
    return [m.value for m in messages if m.topic == topic]


async def test_first_detection_ignites_and_emits_no_status() -> None:
    messages, state = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    events = _by_topic(messages, EVENTS_TOPIC)
    assert len(events) == 1 and events[0][KIND] == IGNITION
    assert events[0][FIRE_ID] == "F-aaa" and events[0][OCCURRED_AT] == _T
    assert (events[0][LAT], events[0][LON]) == (37.0, -7.0)
    assert _by_topic(messages, STATUS_TOPIC) == []               # status is sweep-paced, not here
    assert state is not None and set(state[FIRES]) == {"F-aaa"}


async def test_nearby_detection_joins_the_same_fire_silently() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    # ~500 m north — well inside the 2 km link distance.
    messages, s2 = await _drive(s1, _detection_event(37.0045, -7.0, identity="bbb"))
    assert messages == []                                       # growth is not an event
    assert set(s2[FIRES]) == {"F-aaa"}                           # no new fire
    assert s2[FIRES]["F-aaa"][F_COUNT] == 2


async def test_distant_detection_founds_a_second_fire() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    messages, s2 = await _drive(s1, _detection_event(37.5, -7.0, identity="bbb"))   # ~55 km away
    ignitions = _by_topic(messages, EVENTS_TOPIC)
    assert len(ignitions) == 1 and ignitions[0][FIRE_ID] == "F-bbb"
    assert set(s2[FIRES]) == {"F-aaa", "F-bbb"}


async def test_bridging_detection_merges_two_fires() -> None:
    # Two fires 0.03° apart in latitude: each reaches 0.01797° (2 km), so they do NOT link to
    # each other, but a detection between them links to both.
    _, s1 = await _drive(State(), _detection_event(37.50, -7.0, identity="aaa",
                                                   acquired=_T - timedelta(hours=2)))
    _, s2 = await _drive(s1, _detection_event(37.53, -7.0, identity="bbb"))
    assert set(s2[FIRES]) == {"F-aaa", "F-bbb"}                   # genuinely separate first

    messages, s3 = await _drive(s2, _detection_event(37.515, -7.0, identity="ccc"))
    merges = _by_topic(messages, EVENTS_TOPIC)
    assert len(merges) == 1 and merges[0][KIND] == MERGED
    # The survivor is the earlier fire; the younger one is reported as merged into it.
    assert merges[0][FIRE_ID] == "F-bbb" and merges[0][MERGED_INTO] == "F-aaa"
    assert set(s3[FIRES]) == {"F-aaa"}                            # one fire remains
    assert s3[FIRES]["F-aaa"][F_COUNT] == 3                       # 1 + 1 + the bridge


async def test_sweep_emits_one_active_status_per_fire() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa", frp=42.0))
    _, s2 = await _drive(s1, _detection_event(37.5, -7.0, identity="bbb", frp=7.0))
    sweep_at = _T + timedelta(minutes=5)
    messages, s3 = await _drive(s2, _sweep_event(sweep_at, new_detections=2))

    statuses = _by_topic(messages, STATUS_TOPIC)
    assert len(statuses) == 2 and _by_topic(messages, EVENTS_TOPIC) == []
    assert {s[FIRE_ID] for s in statuses} == {"F-aaa", "F-bbb"}
    assert all(s[STATUS] == ACTIVE and s[AS_OF] == sweep_at for s in statuses)
    by_id = {s[FIRE_ID]: s for s in statuses}
    assert by_id["F-aaa"][FRP_MAX] == 42.0 and by_id["F-aaa"][DETECTIONS] == 1
    assert by_id["F-aaa"][FIRST_SEEN] == _T and by_id["F-aaa"][LAST_SEEN] == _T
    assert set(s3[FIRES]) == {"F-aaa", "F-bbb"}                   # nothing retired


async def test_sweep_heartbeats_even_when_nothing_was_found() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    messages, _ = await _drive(s1, _sweep_event(_T + timedelta(minutes=5), new_detections=0))
    assert len(_by_topic(messages, STATUS_TOPIC)) == 1            # a quiet poll still ticks


async def test_sweep_past_the_timeout_extinguishes_and_removes() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa", frp=42.0))
    _, s2 = await _drive(s1, _detection_event(37.5, -7.0, identity="bbb"))
    # Keep one fire alive by re-detecting it just before the late sweep.
    late = _T + EXTINGUISH_AFTER + timedelta(hours=1)
    _, s3 = await _drive(s2, _detection_event(37.5, -7.0, identity="ccc", acquired=late))

    messages, s4 = await _drive(s3, _sweep_event(late))
    events = _by_topic(messages, EVENTS_TOPIC)
    assert len(events) == 1 and events[0][KIND] == EXTINGUISHED
    assert events[0][FIRE_ID] == "F-aaa" and events[0][OCCURRED_AT] == late
    assert events[0][DETECTIONS] == 1 and events[0][FRP_MAX] == 42.0
    assert events[0][FIRST_SEEN] == _T and events[0][LAST_SEEN] == _T

    statuses = {s[FIRE_ID]: s for s in _by_topic(messages, STATUS_TOPIC)}
    assert statuses["F-aaa"][STATUS] == EXTINGUISHED              # final snapshot flips the map
    assert statuses["F-bbb"][STATUS] == ACTIVE
    assert set(s4[FIRES]) == {"F-bbb"}                            # the dead one is gone


async def test_last_fire_out_tombstones_the_region() -> None:
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    messages, state = await _drive(s1, _sweep_event(_T + EXTINGUISH_AFTER + timedelta(minutes=1)))
    assert len(_by_topic(messages, EVENTS_TOPIC)) == 1
    assert len(_by_topic(messages, STATUS_TOPIC)) == 1            # the final snapshot still flows
    assert state is not None and not state                        # falsy State() → tombstone


async def test_sweep_against_empty_state_yields_nothing_at_all() -> None:
    # A marker replaying after the tombstone, or a region with no fires yet: not even a State,
    # so a quiet region never churns the changelog.
    messages, state = await _drive(State(), _sweep_event(_T))
    assert messages == [] and state is None


async def test_detection_past_the_fire_cap_force_extinguishes_the_stalest(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """The bucket bound: one changelog record cannot grow without limit, so past MAX_FIRES the
    stalest fire is forced out — the same self-healing degradation as a false extinction."""
    monkeypatch.setattr("examples.wildfire_watch.tracker.MAX_FIRES", 2)
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa",
                                                   acquired=_T - timedelta(hours=6)))
    _, s2 = await _drive(s1, _detection_event(37.5, -7.0, identity="bbb"))
    messages, s3 = await _drive(s2, _detection_event(38.0, -7.0, identity="ccc"))

    events = _by_topic(messages, EVENTS_TOPIC)
    assert [e[KIND] for e in events] == [IGNITION, EXTINGUISHED]
    assert events[0][FIRE_ID] == "F-ccc"
    assert events[1][FIRE_ID] == "F-aaa"                      # the stalest fire went
    assert events[1][OCCURRED_AT] == _T                       # never before its own last_seen
    statuses = _by_topic(messages, STATUS_TOPIC)
    assert len(statuses) == 1 and statuses[0][STATUS] == EXTINGUISHED   # flips off the live map
    assert set(s3[FIRES]) == {"F-bbb", "F-ccc"}


async def test_detection_after_extinction_founds_a_new_fire() -> None:
    """The documented self-healing case: a false extinction repairs itself."""
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa"))
    _, s2 = await _drive(s1, _sweep_event(_T + EXTINGUISH_AFTER + timedelta(minutes=1)))
    assert not s2                                                 # region tombstoned

    reborn = _T + EXTINGUISH_AFTER + timedelta(hours=2)
    messages, s3 = await _drive(State(), _detection_event(37.0, -7.0, identity="ddd",
                                                          acquired=reborn))
    events = _by_topic(messages, EVENTS_TOPIC)
    assert len(events) == 1 and events[0][KIND] == IGNITION
    assert events[0][FIRE_ID] == "F-ddd"                          # a NEW id, same place
    assert set(s3[FIRES]) == {"F-ddd"}


async def test_state_round_trips_through_json() -> None:
    # Each State is one changelog record: it must serialize as plain JSON scalars.
    _, s1 = await _drive(State(), _detection_event(37.0, -7.0, identity="aaa", frp=1.5))
    _, s2 = await _drive(s1, _detection_event(37.0045, -7.0, identity="bbb", frp=2.5,
                                              acquired=_T + timedelta(hours=1)))
    revived = State.wrap(json.loads(json.dumps(s2.raw)))
    entry = revived[FIRES]["F-aaa"]
    assert entry[F_FIRST_SEEN] == "2026-07-25T18:00:00Z"          # DATETIME's ISO rendering
    assert entry[F_LAST_SEEN] == "2026-07-25T19:00:00Z"
    assert entry[F_COUNT] == 2 and entry[F_FRP_SUM] == 4.0


async def test_real_capture_collapses_the_big_cluster_into_one_fire() -> None:
    """End-to-end over the committed capture: 21 real pixels become 8 fires."""
    state: State | None = State()
    for row in parse_area_csv(N20_CSV):
        _, state = await _drive(state, normalize_detection(row, REGION_SLUG, _T))
    fires = state[FIRES]
    assert len(fires) == 8
    assert sum(f[F_COUNT] for f in fires.values()) == 21          # every pixel accounted for
    # The 11-pixel front around 37.54–37.56 N, -7.06 W is ONE fire, not eleven.
    biggest = max(fires.values(), key=lambda f: f[F_COUNT])
    assert biggest[F_COUNT] == 11
    assert 37.54 < biggest[F_LAT] < 37.56 and -7.08 < biggest[F_LON] < -7.05
    assert biggest[F_FRP_MAX] == 187.95

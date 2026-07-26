"""The world tiling — how ``request-wildfire world`` turns the planet into watch regions.

A single world-sized region cannot work here, and the reasons are the two per-key records this
example is built around: the ingest seen-set and the tracker's fire bucket are each **one
changelog record** with a hard ~1 MB ceiling (the aiokafka producer's ``max_request_size``,
which the pinned framework does not expose). The planet currently produces ~215 k VIIRS
detections a day across the two birds — a world seen-set would be ~6 MB, and the worst single
10° cell (the Angola–Zambia savanna belt, measured 2026-07-26) clusters 27 k detections/day
into a 1.9 MB fire bucket all by itself. The architecture's own answer is the right one:
**the world is just many ordinary regions.** Every tile below becomes a normal config record;
the ingest and tracker stages need no world-specific code at all, state and clustering cost
shard per tile, and a second ingest instance splits the planet by config partition for free.

Two design points:

* **The grid is adaptive, not uniform.** A uniform grid fine enough for the savanna belt
  (~2.5° or finer) would need thousands of tiles globally; a coarse one crashes exactly where
  the world burns most. So :func:`quadtree` starts from 10° base cells over land and splits any
  cell whose **live 24 h detection count** exceeds :data:`SPLIT_THRESHOLD`, halving down to
  :data:`MIN_TILE_DEG`. The counts come from FIRMS' *public, keyless* daily global snapshot
  (:data:`PUBLIC_24H_URLS`) — tiling the world costs none of the map key's quota.
* **The tiling is a snapshot of a lookup**, exactly like a geocoded bounding box cached in a
  config record: tiles fit the burn belt *as it is today*. When the season moves it (Africa's
  belt crosses the equator twice a year; the boreal fires come and go), re-run
  ``request-wildfire world`` — it re-tiles from the live snapshot and retires world tiles that
  fell out of the set. A stale tiling degrades politely (seen-set trimming and fire-bucket
  eviction, both bounded and warned about), never fatally.

:data:`LAND_CELLS` was generated once, 2026-07-26, by rasterizing Natural Earth 110 m land
polygons (5×5 point-in-polygon samples per 10° cell), excluding everything south of 60°S —
Antarctica doesn't burn and its tiles would only spend quota. Offshore cells are *not* listed:
gas flares and island fires are picked up at tiling time, because any cell holding a detection
in the public snapshot joins the base set automatically.
"""
from typing import Final, NamedTuple

import csv
import io

import httpx

BASE_DEG: Final = 10
"""Base cell size in degrees. 10° is the sweet spot: ~270 land cells cover every continent,
each within FIRMS' cheap-request regime (and exactly at ``request.MAX_BBOX_DEG``, so a base
tile is by construction never an oversized box)."""

MIN_TILE_DEG: Final = 1.25
"""The finest tile the quadtree will produce (three splits: 10° → 5° → 2.5° → 1.25°, ~140 km).
Still ~70× the 2 km cluster-link distance, so a tile can't fragment a single fire much, and by
then even the Zambian savanna is under the threshold (measured 2026-07-26: the worst 2.5° leaf
held 8.8 k detections/24 h; one more split ends it)."""

SPLIT_THRESHOLD: Final = 6_000
"""Split a tile while its live 24 h detection count exceeds this. The number is sized backwards
from the two per-key records: ≤ 6 k detections/day means ≤ ~12 k window ids in the seen-set
(~180 KB, comfortably under ``ingest.SEEN_HARD_CAP``) and ≤ ~1.5 k clustered fires (measured
322 B/fire → ~480 KB bucket, under ``tracking.MAX_FIRES``). Both caps still guard the tail —
a tile at :data:`MIN_TILE_DEG` may exceed the threshold on a violent day — but with this
tiling they should rarely bite."""

PUBLIC_24H_URLS: Final = (
    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv",
    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_Global_24h.csv",
)
"""FIRMS' public daily global snapshots for the same two VIIRS birds the ingest polls.
No MAP_KEY, no quota — which is what makes request-time tiling free."""

CSV_HEADER_PREFIX: Final = "latitude,longitude"
"""Same guard as the area API's (the public file has 13 columns to the area CSV's 14 — no
``instrument`` — but the two we read lead both)."""

LAND_CELLS: Final[tuple[tuple[int, int], ...]] = (
    (-100, 80), (-90, 80), (-80, 80), (-70, 80), (-60, 80), (-50, 80), (-40, 80), (-30, 80), (-20, 80), (90, 80),
    (-180, 70), (-160, 70), (-130, 70), (-120, 70), (-110, 70), (-100, 70), (-90, 70), (-80, 70), (-70, 70), (-60, 70), (-50, 70), (-40, 70), (-30, 70), (-20, 70), (10, 70), (20, 70), (50, 70), (60, 70), (70, 70), (80, 70), (90, 70), (100, 70), (110, 70), (120, 70), (130, 70), (140, 70), (150, 70), (170, 70),
    (-180, 60), (-170, 60), (-160, 60), (-150, 60), (-140, 60), (-130, 60), (-120, 60), (-110, 60), (-100, 60), (-90, 60), (-80, 60), (-70, 60), (-60, 60), (-50, 60), (-40, 60), (-30, 60), (-20, 60), (0, 60), (10, 60), (20, 60), (30, 60), (40, 60), (50, 60), (60, 60), (70, 60), (80, 60), (90, 60), (100, 60), (110, 60), (120, 60), (130, 60), (140, 60), (150, 60), (160, 60), (170, 60),
    (-170, 50), (-160, 50), (-140, 50), (-130, 50), (-120, 50), (-110, 50), (-100, 50), (-90, 50), (-80, 50), (-70, 50), (-60, 50), (-10, 50), (0, 50), (10, 50), (20, 50), (30, 50), (40, 50), (50, 50), (60, 50), (70, 50), (80, 50), (90, 50), (100, 50), (110, 50), (120, 50), (130, 50), (140, 50), (150, 50), (160, 50),
    (-130, 40), (-120, 40), (-110, 40), (-100, 40), (-90, 40), (-80, 40), (-70, 40), (-60, 40), (-10, 40), (0, 40), (10, 40), (20, 40), (30, 40), (40, 40), (50, 40), (60, 40), (70, 40), (80, 40), (90, 40), (100, 40), (110, 40), (120, 40), (130, 40), (140, 40),
    (-130, 30), (-120, 30), (-110, 30), (-100, 30), (-90, 30), (-80, 30), (-10, 30), (0, 30), (10, 30), (20, 30), (30, 30), (40, 30), (50, 30), (60, 30), (70, 30), (80, 30), (90, 30), (100, 30), (110, 30), (120, 30), (130, 30), (140, 30),
    (-120, 20), (-110, 20), (-100, 20), (-90, 20), (-80, 20), (-20, 20), (-10, 20), (0, 20), (10, 20), (20, 20), (30, 20), (40, 20), (50, 20), (60, 20), (70, 20), (80, 20), (90, 20), (100, 20), (110, 20), (120, 20),
    (-110, 10), (-100, 10), (-90, 10), (-80, 10), (-70, 10), (-20, 10), (-10, 10), (0, 10), (10, 10), (20, 10), (30, 10), (40, 10), (50, 10), (70, 10), (80, 10), (90, 10), (100, 10), (120, 10),
    (-90, 0), (-80, 0), (-70, 0), (-60, 0), (-20, 0), (-10, 0), (0, 0), (10, 0), (20, 0), (30, 0), (40, 0), (70, 0), (80, 0), (90, 0), (100, 0), (110, 0), (120, 0),
    (-90, -10), (-80, -10), (-70, -10), (-60, -10), (-50, -10), (-40, -10), (0, -10), (10, -10), (20, -10), (30, -10), (40, -10), (100, -10), (110, -10), (120, -10), (130, -10), (140, -10), (150, -10), (160, -10),
    (-80, -20), (-70, -20), (-60, -20), (-50, -20), (-40, -20), (10, -20), (20, -20), (30, -20), (40, -20), (120, -20), (130, -20), (140, -20), (160, -20),
    (-80, -30), (-70, -30), (-60, -30), (-50, -30), (10, -30), (20, -30), (30, -30), (40, -30), (110, -30), (120, -30), (130, -30), (140, -30), (150, -30), (160, -30),
    (-80, -40), (-70, -40), (-60, -40), (10, -40), (20, -40), (110, -40), (120, -40), (130, -40), (140, -40), (150, -40), (170, -40),
    (-80, -50), (-70, -50), (60, -50), (140, -50), (160, -50), (170, -50),
    (-80, -60), (-70, -60),
)
"""(west, south) corners of the 10° base cells containing land, 60°S–90°N — see the module
docstring for provenance."""


class Tile(NamedTuple):
    """One watch tile: its slug path and its bounding box (a plain west/south/east/north)."""

    slug: str
    west: float
    south: float
    east: float
    north: float


def cell_slug(west: int, south: int) -> str:
    """A 10° base cell's slug from its (west, south) corner: ``(20, -20)`` → ``e020s20``.

    Children extend the path with quadrant digits (``e020s20-3``, ``e020s20-3-1``; 0=SW 1=SE
    2=NW 3=NE), so a slug reads as its own lineage and never needs fractional degrees. The
    numeric box travels in the config record, as every region's does."""
    ew = f"e{west:03d}" if west >= 0 else f"w{-west:03d}"
    ns = f"n{south:02d}" if south >= 0 else f"s{-south:02d}"
    return ew + ns


def parse_points(text: str) -> list[tuple[float, float]]:
    """The public 24 h CSV as (lon, lat) points — pure, with the same loud non-CSV guard the
    area parser uses (a maintenance page must not read as "the planet is quiet")."""
    body = text.lstrip("﻿")
    first_line = body.splitlines()[0] if body.strip() else ""
    if not first_line.startswith(CSV_HEADER_PREFIX):
        raise RuntimeError(
            f"FIRMS did not return the public 24h CSV (expected a {CSV_HEADER_PREFIX!r} "
            f"header, got {text[:200]!r})")
    return [(float(row["longitude"]), float(row["latitude"]))
            for row in csv.DictReader(io.StringIO(body))]


async def fetch_world_points(client: httpx.AsyncClient) -> list[tuple[float, float]]:
    """Both birds' public 24 h snapshots as one point cloud. Failures propagate — with no
    counts there is nothing honest to tile from."""
    points: list[tuple[float, float]] = []
    for url in PUBLIC_24H_URLS:
        response = await client.get(url)
        response.raise_for_status()
        points += parse_points(response.text)
    return points


def quadtree(points: list[tuple[float, float]], *, land_cells: tuple[tuple[int, int], ...] = LAND_CELLS,
             threshold: int = SPLIT_THRESHOLD, min_deg: float = MIN_TILE_DEG) -> list[Tile]:
    """The world tiling for a given detection snapshot — pure and deterministic.

    Base cells are the land grid **plus any cell that holds a detection** (offshore gas flares,
    island fires — anything the land raster missed announces itself). A cell over ``threshold``
    splits into its four quadrants, *all four kept* — an empty quadrant still needs watching,
    fires ignite where there were none — recursively down to ``min_deg``. Points south of 60°S
    are ignored, matching the land grid's cut. Output is sorted by slug, so the same snapshot
    always yields the same tile list (and re-tiling diffs cleanly against the config topic)."""
    by_cell: dict[tuple[int, int], list[tuple[float, float]]] = {}
    for lon, lat in points:
        if lat < -60.0:
            continue
        x = min(int(lon // BASE_DEG) * BASE_DEG, 180 - BASE_DEG)
        y = min(int(lat // BASE_DEG) * BASE_DEG, 90 - BASE_DEG)
        by_cell.setdefault((x, y), []).append((lon, lat))

    tiles: list[Tile] = []

    def split(slug: str, west: float, south: float, size: float,
              cell_points: list[tuple[float, float]]) -> None:
        if len(cell_points) <= threshold or size <= min_deg:
            tiles.append(Tile(slug, west, south, west + size, south + size))
            return
        half = size / 2
        quads: tuple[list[tuple[float, float]], ...] = ([], [], [], [])
        for lon, lat in cell_points:
            quads[(1 if lon >= west + half else 0) + (2 if lat >= south + half else 0)].append((lon, lat))
        for quadrant, (dx, dy) in enumerate(((0.0, 0.0), (half, 0.0), (0.0, half), (half, half))):
            split(f"{slug}-{quadrant}", west + dx, south + dy, half, quads[quadrant])

    for west, south in sorted(set(land_cells) | set(by_cell)):
        split(cell_slug(west, south), float(west), float(south), float(BASE_DEG),
              by_cell.get((west, south), []))
    return sorted(tiles)

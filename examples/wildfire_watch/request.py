"""Request (or retire) a watch region — validated, then written to ``wildfire-regions``.

    uv run poe request-wildfire "Alentejo, Portugal"
    uv run poe request-wildfire "Some Valley" -8.9 36.9 -6.8 39.2   # explicit west south east north
    uv run poe request-wildfire world                                # the whole planet, tiled
    uv run poe request-wildfire retire alentejo-portugal
    uv run poe request-wildfire retire world

The ops step that replaces a hard-coded seed (the ADS-B ``request-region`` pattern). Give it a
place name and it geocodes that to a bounding box; pass four numbers to pin the box yourself.

**The resolved box is written into the config record.** This tool geocodes *here*, at request
time, and caches all four edges in the record it produces — so the record fully describes the
region it asks for, and nothing has to guess later. Two things follow, both deliberate:

* ``ingest.enrich_config`` becomes a **fallback**, not the normal path. It still geocodes any
  record that arrives without a box (a hand-produced one from Kafbat, say), which is what keeps
  a name-only config a first-class way to ask for a region.
* The record is a **snapshot of a lookup**. If OSM boundaries move, or you change
  :data:`~.ingest.PAD_DEG`, already-written regions keep the box they were created with — re-run
  this command to refresh one. That is the price of a self-describing config record, and it buys
  the overlap check below, which cannot work without knowing the geometry up front.

**Validated before writing.** It resolves the name and *shows you both the box it got and what
the query actually matched* (Nominatim's ``display_name`` + ``addresstype``), because geocoding
a place name is genuinely ambiguous: Nominatim returns one best guess, a typo like
``"Bordeux, France"`` silently matches a *street* in Picardy, a hit backed by an OSM node
yields a synthetic ±1° box rather than a real boundary, and a country name resolves to its whole
OSM relation — famously, "France" spans Kerguelen to French Polynesia because the Republic does.
A side wider than :data:`MAX_BBOX_DEG` gets a loud warning — quota is charged per request and a
*large* box can count as several transactions — but it is a warning, not a block: the operator
may well mean it. A side wider than :data:`REFUSE_BBOX_DEG` is **refused**: a region that size
cannot survive the per-key state records (see :data:`REFUSE_BBOX_DEG`), and the right tool for
"everything" is the world watch below.

**The world watch.** ``request-wildfire world`` tiles the planet into a few hundred ordinary
watch regions (an adaptive quadtree over :mod:`.tiles`' land grid, split where the live public
24 h snapshot shows heavy fire activity) and writes them all; ``retire world`` tombstones them
all. Re-running ``world`` re-tiles from the current snapshot and retires tiles that fell out of
the set — do that when the season moves the burn belt. The stages need no world-specific code:
to them, the world is just many regions.

**It also warns when the box overlaps a region you already watch**, by reading the compacted
config topic and intersecting rectangles. Overlap is legal but has a real cost: a fire inside two
boxes occupies two independent state buckets and is tracked twice (the dashboard de-duplicates by
``fire_id``, but per-region counts and events do not). Cheap to check, easy to fix at request
time, annoying to discover later.

If ``FIRMS_MAP_KEY`` is set it also **previews the live detection count** for the box, which is
the quickest way to tell a sensible watch region from a typo. Without a key that step is skipped
with a hint; requesting a region needs no credentials, only running the ingest stage does.

The record is keyed by the region **slug** on the compacted config topic, so re-requesting a
name updates it and ``retire <slug>`` writes a tombstone that removes it. Any producer works too
(Kafbat UI included) — this is just the convenient, checked one.
"""
import asyncio
import json
import os
import re
import sys
from collections import Counter

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from flechtwerk import Config, Event

from examples._setup import quiet_fresh_topic_produce_race

from .attributes import EAST, NAME, NORTH, REGION, REGIONS_TOPIC, SOUTH, WEST
from .geocoding import NominatimGeocoder
from .ingest import DAY_RANGE, FIRMS_BASE_URL, PAD_DEG, SOURCES, parse_area_csv
from .tiles import fetch_world_points, quadtree

BOOTSTRAP_SERVERS = "localhost:9092"

MAP_KEY_ENV = "FIRMS_MAP_KEY"

MAX_BBOX_DEG = 10.0
"""Widest box side (degrees) that passes without a warning — ~1100 km, a large but plausible
watch area. Beyond it you are probably polling a continent by accident: FIRMS bills large areas
as multiple transactions, the seen-set grows toward its cap, and a single "region" stops being a
meaningful unit for the tracker to reason about."""

REFUSE_BBOX_DEG = 60.0
"""Widest box side (degrees) the tool will write at all — a subcontinent.

Past this the warning becomes a refusal, because the failure is structural, not stylistic: a
region's dedupe seen-set and its fire bucket are each ONE changelog record with a hard ~1 MB
ceiling, sized by everything the box contains. A 60°+ box holds enough of the planet's fires to
blow both — a watch on "France" (whose Nominatim box spans the whole Republic, Kerguelen to
Polynesia) once crashlooped both stages this way. The right tool for "everything" is
``request-wildfire world``, which tiles the planet into regions each of which fits."""

WORLD_PREFIX = "world-"
"""Slug prefix of every world-watch tile region — what ``world`` writes, what ``retire world``
tombstones, and how re-tiling tells its own tiles from a user's named regions."""


def slugify(name: str) -> str:
    """A place name reduced to its region slug — the wire key and every table's grouping key.

    Lowercased, runs of non-alphanumerics collapsed to single hyphens, ends trimmed:
    ``"Alentejo, Portugal"`` → ``"alentejo-portugal"``. Deterministic, so re-requesting the same
    name updates the same compacted record instead of creating a second region."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


Box = tuple[float, float, float, float]
"""A watch bounding box as ``(west, south, east, north)`` — the order the FIRMS area API takes."""


def overlap(a: Box, b: Box) -> Box | None:
    """The intersection of two boxes, or ``None`` when they are disjoint — pure.

    Plain rectangle intersection: the overlap's west/south are the larger of each pair and its
    east/north the smaller, and there is an overlap only if that leaves a positive extent.
    Touching edges (``east == west``) count as disjoint — a shared boundary line holds no area, so
    no detection can fall inside both.

    Antimeridian-crossing boxes (west > east) are out of scope, as they are for the area API
    itself; ``main`` rejects them before anything gets this far."""
    west, south = max(a[0], b[0]), max(a[1], b[1])
    east, north = min(a[2], b[2]), min(a[3], b[3])
    return (west, south, east, north) if west < east and south < north else None


def box_of(config: Config) -> Box | None:
    """A config record's box, or ``None`` if it is name-only (its box is resolved at runtime)."""
    edges = tuple(config.get(edge) for edge in (WEST, SOUTH, EAST, NORTH))
    return edges if None not in edges else None  # type: ignore[return-value]


async def watched_regions(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> dict[str, Config]:
    """Every region currently on the compacted config topic, as ``slug -> Config``.

    Reads the topic from the beginning with **no group id** (this is a read-only peek — it must
    never commit offsets or disturb the extractor's own consumption) and keeps the last value per
    key, so compaction semantics are reproduced: a later record wins and a tombstone removes the
    region. An absent topic (setup not run yet) yields ``{}``."""
    # Subscribing via the constructor (rather than assign()) lets aiokafka own the assignment,
    # which is the form the integration tests use; a group-less consumer built with assign()
    # raises CancelledError out of stop() from its coordinator's internal reset task.
    consumer = AIOKafkaConsumer(REGIONS_TOPIC, bootstrap_servers=bootstrap_servers,
                                group_id=None, enable_auto_commit=False,
                                auto_offset_reset="earliest")
    await consumer.start()
    try:
        assignment = list(consumer.assignment())
        if not assignment:
            return {}  # topic absent — setup has not run yet
        starts, ends = (await consumer.beginning_offsets(assignment),
                        await consumer.end_offsets(assignment))
        pending = sum(ends[tp] - starts[tp] for tp in assignment)
        if pending <= 0:
            return {}  # topic exists but is empty — nothing requested yet
        latest: dict[str, dict | None] = {}
        while pending > 0:
            batch = await consumer.getmany(timeout_ms=2000)
            if not batch:
                break  # nothing more forthcoming; take what we have
            for records in batch.values():
                for record in records:
                    pending -= 1
                    latest[record.key.decode()] = json.loads(record.value) if record.value else None
        return {slug: Config.wrap(raw) for slug, raw in latest.items() if raw is not None}
    finally:
        await consumer.stop()


def _warn_if_overlapping(slug: str, box: Box, existing: dict[str, Config]) -> None:
    """List the regions already watched and warn about any whose box intersects this one."""
    others = {s: c for s, c in existing.items() if s != slug}
    if not others:
        return
    print(f"\nAlready watching {len(others)} region(s):")
    clashes: list[tuple[str, Box]] = []
    for other_slug, config in sorted(others.items()):
        other_box = box_of(config)
        if other_box is None:
            print(f"  {other_slug:24s} (name-only — its box is resolved when ingest picks it up)")
            continue
        print(f"  {other_slug:24s} west={other_box[0]:.4f} south={other_box[1]:.4f} "
              f"east={other_box[2]:.4f} north={other_box[3]:.4f}")
        if (shared := overlap(box, other_box)) is not None:
            clashes.append((other_slug, shared))
    for other_slug, shared in clashes:
        print(f"\n  ⚠️  Overlaps {other_slug!r}: west={shared[0]:.4f} south={shared[1]:.4f} "
              f"east={shared[2]:.4f} north={shared[3]:.4f} "
              f"({shared[2] - shared[0]:.2f}° × {shared[3] - shared[1]:.2f}°).\n"
              "      A fire in there is tracked TWICE — one fire object per region, each with its\n"
              "      own state bucket, status heartbeat, and lifecycle events. The dashboard\n"
              "      de-duplicates by fire_id, but per-region counts double up. Prefer disjoint\n"
              "      boxes unless you want both views.")


def check_box_size(west: float, south: float, east: float, north: float) -> None:
    """Warn about a large box (:data:`MAX_BBOX_DEG`); **refuse** an absurd one
    (:data:`REFUSE_BBOX_DEG`, a ``SystemExit`` before anything is written)."""
    width, height = east - west, north - south
    if max(width, height) > REFUSE_BBOX_DEG:
        raise SystemExit(
            f"\n  ✗ This box is {width:.1f}° × {height:.1f}° — more than {REFUSE_BBOX_DEG:.0f}° on a"
            " side, and one watch region\n"
            "    that size cannot work: its dedupe seen-set and its fire bucket are each ONE\n"
            "    ~1 MB changelog record, sized by everything the box contains. (The classic\n"
            "    accident is a country name — Nominatim's box for 'France' spans the whole\n"
            "    Republic, Kerguelen to Polynesia.) Instead:\n"
            '      - name a narrower area:           "France métropolitaine", "Gironde, France"\n'
            '      - or pin an explicit box:         request-wildfire "<name>" <west> <south> <east> <north>\n'
            "      - or watch the planet, properly:  request-wildfire world")
    if max(width, height) > MAX_BBOX_DEG:
        print(f"\n  ⚠️  This box is {width:.1f}° × {height:.1f}° — wider than {MAX_BBOX_DEG:.0f}° "
              f"on a side.\n"
              "      FIRMS may bill a large area as several transactions per poll, and one huge\n"
              "      'region' makes the tracker's per-region state and its fire ids less useful.\n"
              "      Consider a tighter box, or several smaller regions.")


async def _preview(name: str, bbox: Box | None) -> Box:
    """Resolve (or accept) the box, print it, and — with a key — the live detection count.

    Returns the box that was validated, so the caller can echo it. Geocoding failures propagate:
    a name that matches nothing is a config error and nothing should be written for it. What the
    name *matched* is printed alongside the box — the one line that tells a typo's street or an
    overseas-spanning country from the region you meant."""
    if bbox is None:
        geocoder = NominatimGeocoder()
        try:
            match = await geocoder.resolve(name)
        finally:
            await geocoder.aclose()
        west, south = match.west - PAD_DEG, match.south - PAD_DEG
        east, north = match.east + PAD_DEG, match.north + PAD_DEG
        kind = f" — {match.addresstype}" if match.addresstype else ""
        print(f"Geocoded {name!r} (Nominatim, +{PAD_DEG}° pad):")
        print(f"  matched: {match.display_name}{kind}")
    else:
        west, south, east, north = bbox
        print(f"Using the bounding box you gave for {name!r} (no geocoding):")
    print(f"  west={west:.4f} south={south:.4f} east={east:.4f} north={north:.4f}")
    check_box_size(west, south, east, north)

    map_key = os.environ.get(MAP_KEY_ENV, "").strip()
    if not map_key:
        print(f"\n  (Set {MAP_KEY_ENV} to also preview how many detections this box holds — "
              f"https://firms.modaps.eosdis.nasa.gov/api/map_key/)")
        return west, south, east, north

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
        print(f"\nCurrent FIRMS detections in the box (last {DAY_RANGE} day(s)):")
        for source in SOURCES:
            url = (f"{FIRMS_BASE_URL}/api/area/csv/{map_key}/{source}/"
                   f"{west},{south},{east},{north}/{DAY_RANGE}")
            response = await client.get(url)
            response.raise_for_status()
            print(f"  {source:20s} {len(parse_area_csv(response.text)):5d}")
    return west, south, east, north


async def request_watch(name: str, bbox: Box | None) -> None:
    """Validate, preview, warn about overlap, then write the config record keyed by the slug."""
    slug = slugify(name)
    if not slug:
        raise SystemExit(f"{name!r} has no alphanumeric characters — it yields an empty slug.")
    box = await _preview(name, bbox)
    _warn_if_overlapping(slug, box, await watched_regions())

    # The resolved box is cached in the record (see the module docstring): the config then fully
    # describes the region, enrich_config is only a fallback for name-only records, and the
    # overlap check above has geometry to work with.
    west, south, east, north = box
    record = Event({REGION: slug, NAME: name,
                    WEST: west, SOUTH: south, EAST: east, NORTH: north})

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            await producer.send_and_wait(REGIONS_TOPIC, key=slug.encode(),
                                         value=json.dumps(record.raw).encode())
        print(f"\nRequested watch region {slug!r} ({name!r})")
    finally:
        await producer.stop()


async def request_world() -> None:
    """Tile the planet from the live public snapshot and write every tile as a watch region.

    Tiling costs no MAP_KEY quota (the snapshot is FIRMS' public daily file), and the result is
    just ordinary config records — the stages never learn the word "world". Re-running re-tiles
    from today's snapshot and retires tiles the new tiling no longer contains, so the watch
    follows the burn belt at the operator's pace."""
    print("Fetching the public 24 h global snapshots (both satellites, no MAP_KEY) ...")
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0), follow_redirects=True) as client:
        points = await fetch_world_points(client)
    tiles = quadtree(points)
    fresh = {WORLD_PREFIX + tile.slug: tile for tile in tiles}

    existing = await watched_regions()
    stale = sorted(slug for slug in existing
                   if slug.startswith(WORLD_PREFIX) and slug not in fresh)
    named = sorted(slug for slug in existing if not slug.startswith(WORLD_PREFIX))

    sizes = Counter(tile.east - tile.west for tile in tiles)
    print(f"Tiled {len(points):,} detections (24 h) into {len(tiles)} tiles: "
          + ", ".join(f"{count} × {size:g}°" for size, count in sorted(sizes.items(), reverse=True)))
    requests_per_poll = len(SOURCES) * len(tiles)
    print(f"\nEvery ingest poll round will cost {requests_per_poll} FIRMS requests"
          f" (~{2 * requests_per_poll} per 10-minute\nquota window at the 5-minute interval;"
          " the default quota is 5000 transactions and a\nrequest can bill as several —"
          " watch mapserver/mapkey_status/?MAP_KEY=<your key>).")
    if named:
        print(f"\n  ⚠️  {len(named)} named region(s) sit inside the world watch and will be tracked"
              f" TWICE:\n      {', '.join(named)}.\n"
              "      The dashboard's fire_id de-duplication is unreliable across different boxes\n"
              "      (each may found the fire from a different detection). For exact counts keep\n"
              "      either the world watch or the named regions, not both.")

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for slug, tile in sorted(fresh.items()):
                record = Event({REGION: slug,
                                NAME: f"World tile {tile.slug} ({tile.east - tile.west:g}°)",
                                WEST: tile.west, SOUTH: tile.south,
                                EAST: tile.east, NORTH: tile.north})
                await producer.send_and_wait(REGIONS_TOPIC, key=slug.encode(),
                                             value=json.dumps(record.raw).encode())
        for slug in stale:
            await producer.send_and_wait(REGIONS_TOPIC, key=slug.encode(), value=None)
    finally:
        await producer.stop()
    print(f"\nRequested the world watch: {len(fresh)} tile region(s) written"
          + (f", {len(stale)} stale tile(s) retired" if stale else "")
          + ". Re-run when the season\nmoves the burn belt — the tiling is a snapshot of today's fire map.")


async def retire(slug: str) -> None:
    """Write a tombstone (null value) for a region, keyed by slug — removes it from the config.

    Compacted-topic tombstone: the extractor's config bootstrap treats an empty value as a
    deletion, so the region drops out of every instance's active set on the next config drain.
    Fires the tracker already holds for it stay in state until they age out normally."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(REGIONS_TOPIC, key=slug.encode(), value=None)
        print(f"Retired watch region {slug!r} (tombstone written)")
    finally:
        await producer.stop()


async def retire_world() -> None:
    """Tombstone every ``world-*`` tile in one pass — the world watch's ``retire``."""
    existing = await watched_regions()
    tiles = sorted(slug for slug in existing if slug.startswith(WORLD_PREFIX))
    if not tiles:
        print("No world tiles are being watched — nothing to retire.")
        return
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        for slug in tiles:
            await producer.send_and_wait(REGIONS_TOPIC, key=slug.encode(), value=None)
    finally:
        await producer.stop()
    print(f"Retired the world watch ({len(tiles)} tile region(s) tombstoned)")


def main() -> None:
    argv = sys.argv[1:]
    if argv == ["world"]:
        asyncio.run(request_world())
        return
    if argv == ["retire", "world"]:
        asyncio.run(retire_world())
        return
    if len(argv) == 2 and argv[0] == "retire":
        asyncio.run(retire(argv[1]))
        return
    if len(argv) == 1:
        asyncio.run(request_watch(argv[0], None))
        return
    if len(argv) == 5:
        try:
            west, south, east, north = (float(value) for value in argv[1:])
        except ValueError:
            sys.exit("the four bounding-box arguments must be numbers: west south east north")
        if west >= east or south >= north:
            sys.exit(f"bounding box must satisfy west < east and south < north, got "
                     f"west={west} south={south} east={east} north={north}")
        asyncio.run(request_watch(argv[0], (west, south, east, north)))
        return
    sys.exit('usage: python -m examples.wildfire_watch.request "<place name>" '
             '[west south east north]\n'
             '   or: python -m examples.wildfire_watch.request world\n'
             '   or: python -m examples.wildfire_watch.request retire <slug>\n'
             '   or: python -m examples.wildfire_watch.request retire world')


if __name__ == "__main__":
    main()

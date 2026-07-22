"""Static GTFS loader — an ``Extractor`` that turns the long-distance static feed
into one profile per trip.

Stage 0. A ``gtfs-static-sources`` config record names a static GTFS zip URL; each
poll downloads it (only when its ``ETag``/``Last-Modified`` version differs from the
resume cursor — an unchanged feed is a no-op), parses the CSV tables, and emits one
**profile** per rail trip to the compacted ``gtfs-trip-profiles`` topic, keyed by
``trip_id``. A profile is the trip's schedule made self-contained: its line, its
final destination, and its ordered stops each carrying a name, coordinates, and the
scheduled arrival/departure seconds. That is exactly what the delay stage needs to
turn a live per-stop delay into "which station, how late, where on the map" — with
**no route geometry** (none is published for free; see the package docstring).

**The cursor is the feed version, and the whole rebuild is one transaction.** The
static feed is republished wholesale, so a poll re-emits every profile (idempotent
on a compacted topic) and yields the version ``State`` **last** — profiles first,
cursor last, one transaction. A crash mid-rebuild leaves the cursor unadvanced, so
the re-poll rebuilds from scratch. (Scoped to long-distance the feed is tiny —
~5k trips — so one transaction is comfortable; a national-scale feed would page
under a version+offset cursor, an explicit extension point in the README.)

The projection lives in the pure functions :func:`parse_gtfs_time` and
:func:`build_profiles` — no framework, no I/O — so the logic tier drives them
straight off the committed fixture zip. :class:`StaticGtfsLoader` is the thin shell
that fetches, checks the version, and delegates.
"""
import csv
import io
import logging
import zipfile
from collections.abc import AsyncIterator, Iterator

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from .attributes import (
    DESTINATION,
    LINE,
    ROUTE_TYPE,
    STATIC_VERSION,
    STOP_ARR_S,
    STOP_DEP_S,
    STOP_ID,
    STOP_LAT,
    STOP_LON,
    STOP_NAME,
    STOP_SEQ,
    STOPS,
    TRIP_ID,
    URL,
)

log = logging.getLogger(__name__)

STATIC_SOURCES_CONFIG_TOPIC = "gtfs-static-sources"
"""Compacted config topic, one record per static feed to load (keyed by a name like
``fernverkehr``), seeded by ``setup.py``. Each entry drives one poll target."""

PROFILES_TOPIC = "gtfs-trip-profiles"
"""Compacted dimension topic: one profile per ``trip_id``. Co-partitioned with
``gtfs-trip-updates`` so a trip's profile and its live updates meet on one task."""

RAIL_ROUTE_TYPES = frozenset({2})
"""GTFS route types kept as "long-distance rail" — ``2`` (rail). Widen this (or point
the config at ``rv_free``/``de_full``) to cover regional/local, the README extension."""


def parse_gtfs_time(hhmmss: str) -> int:
    """Parse a GTFS ``HH:MM:SS`` clock into seconds since local midnight.

    Hours may exceed 24 (``25:14:00`` → ``90840``): GTFS expresses a trip that runs
    past midnight in the *service day's* clock, so noon-based conversion downstream
    keeps it on the right calendar day. Pure and total on well-formed input."""
    hours, minutes, seconds = (int(part) for part in hhmmss.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def build_profiles(
    zip_bytes: bytes,
    version: str,
    *,
    route_types: frozenset[int] = RAIL_ROUTE_TYPES,
) -> Iterator[tuple[str, Event]]:
    """Project a static GTFS zip into ``(trip_id, profile)`` pairs — pure, I/O-free.

    Joins ``trips`` → ``routes`` (line + type, filtered to ``route_types`` first, so
    a national feed's buses never inflate memory) and ``stop_times`` → ``stops`` (name
    + coordinates). Stops are ordered by ``stop_sequence``; arrival/departure clocks
    become seconds. The destination is the last stop's name (``trips.txt`` carries no
    headsign). A stop whose id is absent from ``stops.txt`` is skipped defensively
    rather than placed at (0, 0)."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def rows(name: str) -> Iterator[dict[str, str]]:
        with zf.open(name) as handle:
            yield from csv.DictReader(io.TextIOWrapper(handle, "utf-8-sig"))

    routes = {r["route_id"]: r for r in rows("routes.txt")}
    stops = {s["stop_id"]: s for s in rows("stops.txt")}
    kept = {
        t["trip_id"]: routes[t["route_id"]]
        for t in rows("trips.txt")
        if t["route_id"] in routes and int(routes[t["route_id"]]["route_type"]) in route_types
    }

    by_trip: dict[str, list[dict[str, str]]] = {}
    for st in rows("stop_times.txt"):
        if st["trip_id"] in kept:
            by_trip.setdefault(st["trip_id"], []).append(st)

    for trip_id, stop_times in by_trip.items():
        stop_times.sort(key=lambda r: int(r["stop_sequence"]))
        stops_out: list[dict[str, object]] = []
        for st in stop_times:
            stop = stops.get(st["stop_id"])
            if stop is None:
                continue
            stops_out.append({
                STOP_SEQ: int(st["stop_sequence"]),
                STOP_ID: st["stop_id"],
                STOP_NAME: stop["stop_name"],
                STOP_LAT: float(stop["stop_lat"]),
                STOP_LON: float(stop["stop_lon"]),
                STOP_ARR_S: parse_gtfs_time(st["arrival_time"]),
                STOP_DEP_S: parse_gtfs_time(st["departure_time"]),
            })
        if not stops_out:
            continue
        route = kept[trip_id]
        profile = Event({
            TRIP_ID: trip_id,
            LINE: route["route_short_name"],
            ROUTE_TYPE: int(route["route_type"]),
            DESTINATION: stops_out[-1][STOP_NAME],
            STOPS: stops_out,
            STATIC_VERSION: version,
        })
        yield trip_id, profile


class StaticGtfsLoader(Extractor):
    """Downloads a static GTFS feed and emits one profile per rail trip.

    Subclasses ``Extractor`` (rather than ``@extractor``) to own the ``httpx``
    client: built in ``__aenter__``, closed in ``__aexit__``; tests inject a stub
    transport, so no network is touched off the live path."""

    config_topics = [STATIC_SOURCES_CONFIG_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 profiles_topic: str = PROFILES_TOPIC,
                 route_types: frozenset[int] = RAIL_ROUTE_TYPES) -> None:
        super().__init__()
        self._client = client
        self._topic = profiles_topic
        self._route_types = route_types

    async def __aenter__(self) -> "StaticGtfsLoader":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(120.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Rebuild every profile when the feed version changed; otherwise do nothing.

        ``If-None-Match`` gives a cheap 304 when the feed is unchanged; the version
        (``ETag`` or ``Last-Modified``) is also compared against the cursor so a feed
        without an ``ETag`` still skips a re-parse. Profiles are emitted first and the
        version ``State`` last, so the whole rebuild commits atomically."""
        assert self._client is not None, "client is opened in __aenter__ or injected"
        cursor = state.get(STATIC_VERSION)
        headers = {"If-None-Match": cursor} if cursor else {}
        response = await self._client.get(config[URL], headers=headers)
        if response.status_code == 304:
            return
        response.raise_for_status()
        version = response.headers.get("ETag") or response.headers.get("Last-Modified") or ""
        if version and version == cursor:
            return

        count = 0
        for trip_id, profile in build_profiles(response.content, version, route_types=self._route_types):
            count += 1
            yield Message(key=trip_id, topic=self._topic, value=profile)
        log.info("Loaded %d trip profiles (version %s)", count, version or "?")
        yield State({STATIC_VERSION: version})


stage = StaticGtfsLoader()
"""The stage the dispatcher runs (``python -m examples.gtfs_delay_monitor loader``)."""

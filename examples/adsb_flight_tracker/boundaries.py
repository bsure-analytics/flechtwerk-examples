"""ADS-B boundary loader — an ``Extractor`` that provisions the reverse-geocoding maps.

Reverse geocoding is **staged**, driven by where the aircraft actually are (not by which
region you poll), so an aircraft over *any* country gets a fine label:

1. **World map (all countries).** At startup this loader downloads a single small global
   ADM0 file (Natural Earth admin-0) into ``world_boundaries_dict`` — a point → its
   country. The enrich stage uses it to detect which country each aircraft is over.
2. **Per-country maps, just-in-time.** When enrich sees traffic over a country it has no
   fine map for, it writes that country's ISO-3 code to the ``adsb-countries`` topic. This
   loader consumes those requests and downloads *all* of that country's admin levels
   (ADM1…ADM5, whichever it publishes) into one per-level polygon dictionary each — a point
   → its admin area at every level. The enrich stage stacks the per-level hits into one
   hierarchical label (``"Le Bourget; Marne; Grand Est"``). So only the countries with
   actual traffic are ever downloaded.

The download is deliberately **out-of-band from enrichment**: enrich only *detects and
requests* (a cheap dictGet + one compacted-topic write); the slow fetch+import happens
here, so it never blocks the enrich poll loop. enrich uses whatever is loaded and misses
gracefully until a country's map arrives.

Both maps are refreshed when stale (the world monthly, countries weekly — boundaries move
slowly). No Nominatim, no pycountry: the country comes from the world map as an ISO-3 code,
which is exactly what the geoBoundaries per-country API takes.

The GeoJSON → ClickHouse-rows transforms live in the plain functions :func:`world_rows`
and :func:`boundary_rows` — no framework machinery, no I/O — so the logic tier drives them
directly. :class:`CountryLoader` is the thin shell that fetches, loads, and reloads.
"""
import json
import logging
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from flechtwerk import Config, Extractor, Message, State
from flechtwerk.attribute import Record

from .attributes import CHECKED_AT, GJ_DOWNLOAD_URL, ISO3, SIMPLIFIED_GEOJSON_URL
from .geocoding import USER_AGENT

log = logging.getLogger(__name__)

COUNTRIES_TOPIC = "adsb-countries"
"""Compacted topic of ISO-3 country codes with live traffic — written by the enrich stage
(one record per country it detects), consumed here as the loader's poll targets. Keyed by
ISO-3 so a country is requested at most once (compaction) and re-requests are no-ops."""

CLICKHOUSE_URL = "http://localhost:8123"
"""The shared-stack ClickHouse HTTP endpoint (default user, no password). Injectable so
tests drive it over a ``MockTransport`` and the integration tier points at a testcontainer."""
GEOBOUNDARIES_API = "https://www.geoboundaries.org/api/current/gbOpen"
"""geoBoundaries open (CC-BY) release; ``…/{ISO3}/{ADM}/`` returns metadata carrying the
per-country GeoJSON download URL. Attribution belongs in the README."""
WORLD_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_0_countries.geojson"
"""Natural Earth 1:50m admin-0 — one small global countries file (~3 MB) for the world map;
its ``NAME`` is the country name and ``ADM0_A3`` the ISO-3 code (which matches geoBoundaries'
per-country codes for the on-demand fine maps). geoBoundaries' own global ADM0 (CGAZ) is
full-resolution (~400 MB) — far too heavy to download and import at startup."""
ADMIN_LEVELS = ("ADM1", "ADM2", "ADM3", "ADM4", "ADM5")
"""The sub-country admin levels the loader downloads — *all* that geoBoundaries has for a
country, coarsest → finest. Each becomes its own polygon dictionary (see :func:`region_dict`);
the enrich stage queries them all and concatenates the hits into a hierarchical area label
(e.g. ``"Le Bourget; Marne; Grand Est"``). A single polygon dict can't do this: it returns
only the finest containing polygon, so one dict per level is required."""
CHECK_INTERVAL = timedelta(hours=1)
"""How often a country poll re-checks freshness against ClickHouse. Between checks, a poll
is a no-op — the changelog timer short-circuits it."""
MAX_AGE_HOURS = 24 * 7
"""Reload a country's boundaries when its newest row is older than this (a week) or absent."""
WORLD_MAX_AGE_HOURS = 24 * 30
"""Reload the world map when it is older than this (a month) or absent — it changes rarely."""

WORLD_TABLE = "flechtwerk.world_boundaries"
WORLD_DICT = "flechtwerk.world_boundaries_dict"
"""Global ADM0 polygon dictionary (point → country); the enrich stage's country detector."""
BOUNDARY_TABLE = "flechtwerk.region_boundaries"
"""All countries' admin areas at all levels (tagged by ``iso3`` + ``admin_level``), the
source table behind the per-level dictionaries."""


def region_dict(level: str) -> str:
    """The per-level polygon dictionary name for a geoBoundaries admin level (e.g. ``ADM3``
    → ``flechtwerk.region_adm3_dict``). One dict per level so a point resolves at *each*
    level — a single polygon dict returns only the finest containing polygon."""
    return f"flechtwerk.region_{level.lower()}_dict"


def _multipolygon(geometry: dict[str, Any]) -> list:
    """Normalise a GeoJSON geometry's coordinates to MultiPolygon nesting.

    ClickHouse's polygon dictionary stores one geometry per row; storing every feature as a
    MultiPolygon (``[polygon][ring][point]``) keeps the column type uniform whether the
    source feature was a ``Polygon`` (wrapped once) or already a ``MultiPolygon`` (used
    as-is). GeoJSON coordinates are already ``[lon, lat]`` — the order ClickHouse's
    ``(x, y)`` point wants — so no swap is needed, and ``[lon, lat]`` pairs land as
    ``Tuple(Float64, Float64)`` via JSONEachRow.
    """
    coordinates = geometry.get("coordinates") or []
    return coordinates if geometry.get("type") == "MultiPolygon" else [coordinates]


def _features(geojson: dict[str, Any]) -> list[dict[str, Any]]:
    """The (Multi)Polygon features of a GeoJSON FeatureCollection (others skipped)."""
    return [feature for feature in (geojson.get("features") or [])
            if (feature.get("geometry") or {}).get("type") in ("Polygon", "MultiPolygon")]


def world_rows(geojson: dict[str, Any], loaded_at: int) -> list[dict[str, Any]]:
    """Project a Natural Earth admin-0 FeatureCollection into ``world_boundaries`` rows.

    One row per country: the normalised geometry, its ``NAME`` (country name) and
    ``ADM0_A3`` (ISO-3). Pure and I/O-free. ``loaded_at`` is Unix seconds.
    """
    rows: list[dict[str, Any]] = []
    for feature in _features(geojson):
        properties = feature.get("properties") or {}
        rows.append({
            "geometry": _multipolygon(feature["geometry"]),
            "country": properties.get("NAME") or "",
            "iso3": properties.get("ADM0_A3") or "",
            "loaded_at": loaded_at,
        })
    return rows


def boundary_rows(geojson: dict[str, Any], iso3: str, admin_level: str, loaded_at: int) -> list[dict[str, Any]]:
    """Project a per-country geoBoundaries FeatureCollection into ``region_boundaries`` rows.

    One row per admin area: the normalised geometry, its ``shapeName``, the owning country's
    ISO-3 (for per-country replacement/freshness), and the ``admin_level`` this
    FeatureCollection is — the loader calls this once per level a country publishes, and the
    level tags each row so its per-level dictionary can filter on it. Pure and I/O-free.
    """
    return [{"geometry": _multipolygon(feature["geometry"]),
             "name": (feature.get("properties") or {}).get("shapeName") or "",
             "iso3": iso3, "admin_level": admin_level, "loaded_at": loaded_at}
            for feature in _features(geojson)]


class CountryLoader(Extractor):
    """Loads the world map at startup and each requested country's fine map just-in-time.

    Subclassing (rather than ``@extractor``) because it owns the HTTP client used for the
    geoBoundaries downloads and the ClickHouse writes; it is opened in ``__aenter__`` (which
    also loads the world map) and closed in ``__aexit__``. Tests inject a client (a stubbed
    transport) before the stage is entered, so no network is touched off the live path.
    """

    config_topics = [COUNTRIES_TOPIC]

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        clickhouse_url: str = CLICKHOUSE_URL,
        clickhouse_auth: tuple[str, str] | None = None,
        api_base: str = GEOBOUNDARIES_API,
        world_url: str = WORLD_URL,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._clickhouse_url = clickhouse_url
        self._ch_auth = httpx.BasicAuth(*clickhouse_auth) if clickhouse_auth else None
        self._api_base = api_base
        self._world_url = world_url
        self._now = now or (lambda: datetime.now(timezone.utc))

    async def __aenter__(self) -> "CountryLoader":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=httpx.Timeout(60.0),
                # geoBoundaries GeoJSON is GitHub-LFS: its download URL 302-redirects
                # (github.com/raw → media.githubusercontent.com), so the client must follow.
                follow_redirects=True,
            )
        await self._ensure_world()  # the world map must exist before enrich can detect countries
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def _ch(self, statement: str) -> str:
        """POST one statement to ClickHouse over HTTP and return the body (raising on error
        — a ClickHouse failure is a real fault, not best-effort like enrichment)."""
        assert self._client is not None, "client is opened in __aenter__ or injected"
        kwargs: dict[str, Any] = {"content": statement.encode()}
        if self._ch_auth is not None:
            kwargs["auth"] = self._ch_auth
        response = await self._client.post(self._clickhouse_url, **kwargs)
        response.raise_for_status()
        return response.text

    async def _fresh(self, table: str, where: str, max_age_hours: int) -> bool:
        """Whether ``table`` has a row matching ``where`` newer than ``max_age_hours``."""
        answer = await self._ch(
            f"SELECT count() > 0 AND max(loaded_at) > now() - INTERVAL {max_age_hours} HOUR "
            f"FROM {table} WHERE {where} FORMAT TabSeparated")
        return answer.strip() == "1"

    async def _insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        if rows:
            body = "\n".join(json.dumps(row) for row in rows)
            await self._ch(f"INSERT INTO {table} FORMAT JSONEachRow\n{body}")

    async def _download(self, url: str) -> dict[str, Any]:
        response = await self._client.get(url)  # type: ignore[union-attr]
        response.raise_for_status()
        return response.json()

    async def _ensure_world(self) -> None:
        """Download the world ADM0 map into ``world_boundaries`` if absent or stale, then
        reload its dictionary. Called at startup and re-checked on every country poll."""
        if await self._fresh(WORLD_TABLE, "1", WORLD_MAX_AGE_HOURS):
            return
        rows = world_rows(await self._download(self._world_url), int(self._now().timestamp()))
        await self._ch(f"TRUNCATE TABLE {WORLD_TABLE}")
        await self._insert(WORLD_TABLE, rows)
        await self._ch(f"SYSTEM RELOAD DICTIONARY {WORLD_DICT}")
        log.info("loaded %d country boundaries into the world map", len(rows))

    async def _fetch_country_levels(self, iso3: str) -> list[tuple[str, dict[str, Any]]]:
        """Fetch *all* of a country's admin levels from geoBoundaries → ``[(level, geojson)]``.

        Downloads every level in ``ADMIN_LEVELS`` that the country actually publishes (metadata
        neither 404s nor lacks a download URL); the rest are simply absent — no fallback, since
        we want the whole hierarchy, not the finest. Prefers the simplified geometry. A genuine
        failure (not a 404) propagates and the loader restarts.
        """
        fetched: list[tuple[str, dict[str, Any]]] = []
        for level in ADMIN_LEVELS:
            metadata = await self._client.get(f"{self._api_base}/{iso3}/{level}/")  # type: ignore[union-attr]
            if metadata.status_code == 404:
                continue  # geoBoundaries doesn't cut this country at this level
            metadata.raise_for_status()
            body = metadata.json()
            info = Record.wrap(body[0] if isinstance(body, list) else body)
            url = info.get(SIMPLIFIED_GEOJSON_URL) or info.get(GJ_DOWNLOAD_URL)
            if url:
                fetched.append((level, await self._download(url)))
        return fetched

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Ensure the requested country's boundaries (all levels) are present and fresh.

        The config is a country ISO-3 (from the enrich stage). A fast no-op when the changelog
        timer says we confirmed this country within ``CHECK_INTERVAL``; otherwise re-check the
        world map's staleness (cheap) and, if the country is absent or stale, download *every*
        admin level it publishes, replace its rows, and reload the per-level dictionaries the
        enrich stage stacks into a hierarchical label. A country the world map names but
        geoBoundaries has no map for (a disputed "-99", a tiny territory) loads nothing and is
        skipped — aircraft over it still get the country from the world map. The ``State``
        records the check time so the dedup survives a restart and a crash mid-load resumes; no
        data message is emitted — the loader's product is the dictionaries.
        """
        iso3 = config[ISO3]
        now = self._now()
        checked = state.get(CHECKED_AT)
        if checked is not None and now - checked < CHECK_INTERVAL:
            return
        await self._ensure_world()
        if not await self._fresh(BOUNDARY_TABLE, f"iso3 = '{iso3}'", MAX_AGE_HOURS):
            fetched = await self._fetch_country_levels(iso3)
            if not fetched:
                log.warning("geoBoundaries has no boundaries for %s — skipping (country label only)", iso3)
            else:
                rows = [row for level, geojson in fetched
                        for row in boundary_rows(geojson, iso3, level, int(now.timestamp()))]
                await self._ch(f"ALTER TABLE {BOUNDARY_TABLE} DELETE WHERE iso3 = '{iso3}' SETTINGS mutations_sync = 1")
                await self._insert(BOUNDARY_TABLE, rows)
                for level, _ in fetched:
                    await self._ch(f"SYSTEM RELOAD DICTIONARY {region_dict(level)}")
                log.info("loaded %d boundaries for %s across %s", len(rows), iso3, [level for level, _ in fetched])
        yield State({CHECKED_AT: now})


stage = CountryLoader()
"""The stage the dispatcher runs (``python -m examples.adsb_flight_tracker boundaries``).

Loads the world map at startup and each country's fine map on demand, against the shared-stack
ClickHouse (``localhost:8123``); inject a client / credentials to point it elsewhere.
"""

"""Forward geocoding — a place name → a watch **bounding box** — via Nominatim ``/search``.

Used by :meth:`ingest.FirmsIngest.enrich_config` to turn a name-only region config
(``{"name": "Alentejo, Portugal"}``) into the four bbox edges the FIRMS area API needs. Like
ADS-B's geocoder this runs **once per config record** (a framework hook, not per poll) and its
result is *essential* — without a box the region cannot be polled — so it keeps the framework's
plain "let it crash" behaviour: a timeout or 5xx propagates and the orchestrator restarts; a
name that matches nothing is a config error, raised as such. No in-process retry, no swallowing.

**What's new here.** ADS-B reads Nominatim's ``lat``/``lon`` to get a *point*; this reads the
``boundingbox`` field, which no other example touches — the natural fit for an area API. Two
things about it are worth knowing, both verified against the live service while writing this:

* the order is ``[south, north, west, east]`` (latitudes first, then longitudes) and every
  element is a **string**, not a number;
* a hit backed by an OSM *boundary relation* returns that boundary's true extent, but a hit
  backed by a *node* (many informal region names resolve to one) returns a synthetic ±1° box
  around the point. Both are usable watch boxes; the second is just coarser than it looks,
  which is one reason ``request.py`` prints the resolved box for a human to eyeball.

This is a deliberate **copy** of ADS-B's ``NominatimGeocoder`` shape rather than an import:
examples stay self-contained (only ``clickhouse_sink`` reaches into another example, and that
is a wire-schema coupling, not code reuse).
"""
from typing import Final, Protocol

import httpx
from flechtwerk.attribute import Attribute, LIST, STR
from flechtwerk.attribute import Record

USER_AGENT: Final = "flechtwerk-examples/0 (+https://github.com/bsure-analytics/flechtwerk-examples)"
"""Sent on every Nominatim request — the community service's usage policy asks for an
identifying agent, and anonymous traffic gets throttled or blocked."""

NOMINATIM_BASE_URL: Final = "https://nominatim.openstreetmap.org"
"""Public Nominatim host. Point ``NominatimGeocoder(search_url=…)`` elsewhere to run against a
self-hosted instance (the public one is rate-limited to ~1 request/second)."""

BOUNDING_BOX: Final = Attribute("boundingbox", LIST(STR))
"""Nominatim's ``boundingbox`` — ``[south, north, west, east]`` as strings.

Declared here, at the edge that reads it, rather than in ``attributes.py``: that module owns
*our* schema, and this is one field of a **foreign** response we project into ours immediately.
Wrapping the hit in a ``Record`` keeps the house rule that no naive dict travels past an HTTP
boundary."""


class Geocoder(Protocol):
    """The narrow surface :class:`ingest.FirmsIngest` needs — real over HTTP, a fake in tests.

    Resolves a place name to a watch bounding box; raises when nothing matches (a config error)
    or the upstream fails (let it crash)."""

    async def bbox(self, query: str) -> tuple[float, float, float, float]: ...


class NominatimGeocoder:
    """Resolves a place name to ``(south, north, west, east)`` via Nominatim ``/search``.

    ``client`` / ``search_url`` are injectable so tests drive it over a ``MockTransport`` and a
    caller can point it at a self-hosted Nominatim.
    """

    SEARCH_URL = NOMINATIM_BASE_URL + "/search"

    def __init__(self, client: httpx.AsyncClient | None = None, *, search_url: str | None = None) -> None:
        self._client = client or httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=httpx.Timeout(8.0),
        )
        self.search_url = search_url or self.SEARCH_URL

    async def bbox(self, query: str) -> tuple[float, float, float, float]:
        """The best Nominatim hit's bounding box as ``(south, north, west, east)`` floats.

        ``limit=1`` asks for only the top match — the geocoder does not try to disambiguate,
        which is why ``request.py`` shows the operator what it resolved to before anything is
        written. An empty result is a config error (the name matches no place) → ``LookupError``;
        a timeout or 5xx propagates (let it crash).
        """
        response = await self._client.get(
            self.search_url, params={"q": query, "format": "jsonv2", "limit": 1})
        response.raise_for_status()
        results = response.json()
        if not results:
            raise LookupError(f"Nominatim found no match for region {query!r}")
        south, north, west, east = Record.wrap(results[0])[BOUNDING_BOX]
        return float(south), float(north), float(west), float(east)

    async def aclose(self) -> None:  # pragma: no cover — live path
        await self._client.aclose()

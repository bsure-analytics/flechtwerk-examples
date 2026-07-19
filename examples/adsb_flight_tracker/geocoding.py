"""Forward geocoding — a place name → coordinates — via Nominatim ``/search``.

Forward geocoding (name → coordinates) is used by :meth:`ingest.AdsbIngest.enrich_config`
to resolve a name-only region to the centre its poll needs. It uses the community Nominatim
service under a proper ``User-Agent``; the constant lives here. Reverse geocoding is *not*
here — it is a staged local ClickHouse polygon-dictionary lookup in the enrich stage
(world map → country, then per-country map → area; see ``enrich.py`` / ``boundaries.py``),
so no Nominatim (nor pycountry) is on the reverse path.

It lets a region config carry **just a name** — ``{"name": "London"}`` — and have the poll
centre resolved from it. Unlike the enrich stage's *decorative*, per-position reverse
geocode (best-effort), this runs **once per config record** and its result is **essential**
— without a centre the region cannot be polled — so it keeps the framework's plain "let it
crash" behaviour: a timeout / 5xx propagates and the orchestrator restarts; a name that
matches nothing is a config error, raised as such. No in-process retry, no swallowing.

The stage injects the geocoder (real over HTTP, a fake in tests), exactly as the
enrich stage injects its ``Enricher`` — so no network is touched off the live path.
"""
from typing import Protocol

import httpx
from flechtwerk.attribute import Record

from .attributes import SRC_LAT, SRC_LON

USER_AGENT = "flechtwerk-examples/0 (+https://github.com/bsure-analytics/flechtwerk-examples)"
"""Sent on every request this example makes (adsb.lol, Wikidata, Nominatim) — the
community feeds' usage policies ask for an identifying agent. One string, imported by
``ingest`` (its adsb.lol client + this geocoder), the boundary loader (its geoBoundaries
download client), and ``enrich`` (its Wikidata client), so the identity can't drift between stages."""

NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org"
"""Public Nominatim host for forward geocoding (``/search``): the ingest stage's region
centre and the boundary loader's region → country resolution. Reverse geocoding does not
use it (it is a local ClickHouse polygon dictionary). Point it elsewhere to run the
forward lookups against a self-hosted Nominatim."""


class Geocoder(Protocol):
    """The narrow forward-geocoding surface :class:`ingest.AdsbIngest` needs — real over
    HTTP, a fake in tests. Resolves a place name to ``(lat, lon)``; raises when nothing
    matches (a config error) or the upstream fails (let it crash)."""

    async def locate(self, query: str) -> tuple[float, float]: ...


class NominatimGeocoder:
    """Resolves a place name to coordinates via Nominatim ``/search`` (forward geocode).

    ``client`` / ``search_url`` are injectable so tests drive it over a
    ``MockTransport`` and a caller can point it at a self-hosted Nominatim instead of
    the public, rate-limited one.
    """

    SEARCH_URL = NOMINATIM_BASE_URL + "/search"

    def __init__(self, client: httpx.AsyncClient | None = None, *, search_url: str | None = None) -> None:
        self._client = client or httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=httpx.Timeout(8.0),
        )
        self.search_url = search_url or self.SEARCH_URL

    async def locate(self, query: str) -> tuple[float, float]:
        """The best Nominatim hit for ``query`` as ``(lat, lon)``.

        ``limit=1`` asks for only the top match. An empty result is a config error
        (the name matches no place) → ``LookupError``; a timeout / 5xx propagates
        (let it crash). The hit's ``lat``/``lon`` share the aircraft feed's wire keys,
        so they read through the existing ``SRC_LAT``/``SRC_LON`` handles (``ANY`` — the
        values arrive as strings), coerced here to the float the config wants.
        """
        response = await self._client.get(self.search_url, params={"q": query, "format": "jsonv2", "limit": 1})
        response.raise_for_status()
        results = response.json()
        if not results:
            raise LookupError(f"Nominatim found no match for region {query!r}")
        hit = Record.wrap(results[0])
        return float(hit[SRC_LAT]), float(hit[SRC_LON])

    async def aclose(self) -> None:  # pragma: no cover — live path
        await self._client.aclose()

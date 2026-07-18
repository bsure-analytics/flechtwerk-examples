"""Forward geocoding — a place name → coordinates — via Nominatim ``/search``.

The *forward* companion to the enrich stage's *reverse* geocoder
(:meth:`enrich.WikidataNominatimEnricher.geocode`, a position → its place name):
both talk to the same community Nominatim service under the same ``User-Agent``
policy, so the shared constants live here and the enrich stage imports them (one
copy, no drift).

It lets a region config carry **just a name** — ``{"name": "London"}`` — and
have :meth:`ingest.AdsbIngest.enrich_config` resolve the coordinates the poll needs.
Unlike the enrich stage's *decorative*, per-position reverse lookups (best-effort,
circuit-broken, bounded per poll), this runs **once per config record** and its
result is **essential** — without a centre the region cannot be polled — so it keeps
the framework's plain "let it crash" behaviour: a timeout / 5xx propagates and the
orchestrator restarts; a name that matches nothing is a config error, raised as such.
No in-process retry, no swallowing.

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
``ingest`` (its adsb.lol client + this geocoder) and ``enrich`` (its Wikidata/Nominatim
client), so the identity can't drift between stages."""

NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org"
"""Public Nominatim host, shared by the forward geocoder here (``/search``) and the
enrich stage's reverse geocoder (``/reverse``). Point either at the opt-in self-hosted
instance (the ``geocoder`` compose profile, ``http://localhost:8091``) to escape the
public service's rate limit — see the README."""


class Geocoder(Protocol):
    """The narrow forward-geocoding surface :class:`ingest.AdsbIngest` needs — real over
    HTTP, a fake in tests. Resolves a place name to ``(lat, lon)``; raises when nothing
    matches (a config error) or the upstream fails (let it crash)."""

    async def locate(self, query: str) -> tuple[float, float]: ...


class NominatimGeocoder:
    """Resolves a place name to coordinates via Nominatim ``/search`` (forward geocode).

    ``client`` / ``search_url`` are injectable so tests drive it over a
    ``MockTransport`` and a caller can point it at the self-hosted Nominatim (the
    ``geocoder`` compose profile) instead of the public, rate-limited one.
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

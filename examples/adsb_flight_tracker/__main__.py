"""Run an ADS-B pipeline stage against the shared stack.

    uv run poe setup-adsb          # topics, region seed, ClickHouse schema
    uv run poe run-adsb            # stage 1: ingest adsb.lol -> adsb.raw
    uv run poe run-adsb-enrich     # stage 2: unroll + live-enrich -> aircraft/events/cells
    uv run poe run-adsb-conflict   # stage 3: baby-TCAS conflict detection over adsb.cells

Each target selects a stage by name (``python -m examples.adsb_flight_tracker
<stage>``) and runs it through the shared ``examples._runner``. The demo constants
live here, in the ops caller — the framework reads nothing from the environment.
The ``metrics_port``s match the ADS-B targets in ``prometheus/prometheus.yml``.

Geocoding auto-routes to the self-hosted Nominatim when it is up: if the opt-in
``geocoder`` compose profile is running and done importing, both geocoders (ingest's
forward ``/search``, enrich's reverse ``/reverse``) point at ``localhost:8091`` with
no code change; otherwise they use the public service. That environment probe lives
*here*, in the ops caller — never inside a stage, which the framework keeps free of
environment magic (all its config is injected). See :func:`self_hosted_nominatim`.
"""
from datetime import timedelta

import httpx

from examples._runner import dispatch, run

from .conflict import stage as conflict_stage
from .enrich import AdsbEnrich, WikidataNominatimEnricher
from .enrich import stage as enrich_stage
from .geocoding import NominatimGeocoder
from .ingest import stage as ingest_stage

# adsb.lol is a free community API with no SLA — poll gently to stay under its
# rate limit (a too-eager cadence earns HTTP 429, which the ingest stage lets crash).
POLL_INTERVAL = timedelta(seconds=10)

NOMINATIM_LOCAL = "http://localhost:8091"
"""Host address of the opt-in self-hosted Nominatim (the ``geocoder`` compose profile)."""


def self_hosted_nominatim(client: httpx.Client | None = None) -> str | None:
    """The self-hosted Nominatim base URL when its ``geocoder`` profile is up and done
    importing, else ``None`` (fall back to the public service).

    Probing ``/status`` (not ``/search``) is deliberate: it is exactly what the compose
    healthcheck hits, and Nominatim returns 200 there only once the OSM import has
    finished — so a container that is still importing correctly reads as "not ready" and
    we stay on public. Anything unreachable (profile not started → connection refused)
    is likewise ``None``. A short timeout keeps a normal run's startup snappy. ``client``
    is injectable so a test can drive the branch over a ``MockTransport``.
    """
    probe = client or httpx.Client(timeout=1.0)
    try:
        return NOMINATIM_LOCAL if probe.get(f"{NOMINATIM_LOCAL}/status").status_code == 200 else None
    except httpx.HTTPError:
        return None
    finally:
        if client is None:
            probe.close()


def run_ingest() -> None:
    """Ingest, with its forward geocoder repointed at the self-hosted Nominatim if up.

    ``geocoder`` is a public attribute on the stage, so the ops caller repoints the
    canonical module-level instance in place (the same mechanism a library user would
    use); left untouched it defaults to public Nominatim in ``__aenter__``.
    """
    if base := self_hosted_nominatim():
        ingest_stage.geocoder = NominatimGeocoder(search_url=f"{base}/search")
        print(f"ADS-B ingest: forward-geocoding via self-hosted Nominatim ({base})")
    run(ingest_stage, application_id="adsb-ingest", client_id="adsb-ingest-0",
        metrics_port=9101, poll_interval=POLL_INTERVAL)


def run_enrich() -> None:
    """Enrich, with its reverse geocoder repointed at the self-hosted Nominatim if up.

    The enricher is a *constructor* dependency (so it can also carry the Wikidata
    breaker), so — unlike ingest's settable ``geocoder`` — the self-hosted variant is a
    freshly built stage; the default path runs the canonical module-level ``enrich_stage``
    (public Wikidata + Nominatim). This is the systematic per-position geocode the public
    service rate-limits, so it is the one that most wants the self-hosted instance.
    """
    stage = enrich_stage
    if base := self_hosted_nominatim():
        stage = AdsbEnrich(enricher=WikidataNominatimEnricher(nominatim_url=f"{base}/reverse"))
        print(f"ADS-B enrich: reverse-geocoding via self-hosted Nominatim ({base})")
    run(stage, application_id="adsb-enrich", client_id="adsb-enrich-0", metrics_port=9105)


if __name__ == "__main__":
    dispatch({
        "ingest": run_ingest,
        "enrich": run_enrich,
        "conflict": lambda: run(conflict_stage, application_id="adsb-conflict", client_id="adsb-conflict-0",
                                metrics_port=9106),
    })

"""Run an ADS-B pipeline stage against the shared stack.

    uv run poe setup-adsb          # topics + ClickHouse schema (no region seeded)
    uv run poe request-region "X"  # write a region to poll to adsb-regions
    uv run poe run-adsb-boundaries # stage 0: load the world map + per-country maps on demand
    uv run poe run-adsb-ingest     # stage 1: ingest adsb.lol -> adsb-raw
    uv run poe run-adsb-enrich     # stage 2: unroll + live-enrich -> aircraft/events/cells
    uv run poe run-adsb-conflict   # stage 3: baby-TCAS conflict detection over adsb-cells

Each target selects a stage by name (``python -m examples.adsb_flight_tracker
<stage>``) and runs it through the shared ``examples._runner``. The demo constants
live here, in the ops caller — the framework reads nothing from the environment.
The ``metrics_port``s match the ADS-B targets in ``prometheus/prometheus.yml``.

Reverse geocoding is staged over two local ClickHouse polygon dictionaries (see
``boundaries.py`` and ``enrich.py``): the loader downloads a world map at startup and
each country's fine map on demand as enrich detects traffic; both talk to the
shared-stack ClickHouse (``localhost:8123``) by default. Forward geocoding of a
name-only region (``ingest``) uses public Nominatim.
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .boundaries import stage as boundaries_stage
from .conflict import stage as conflict_stage
from .enrich import stage as enrich_stage
from .ingest import stage as ingest_stage

# adsb.lol is a free community API with no SLA — poll gently to stay under its
# rate limit (a too-eager cadence earns HTTP 429, which the ingest stage lets crash).
ADSB_POLL_INTERVAL = timedelta(seconds=10)

# The boundary loader consumes adsb-countries (countries enrich has seen traffic over) and
# ensures each one's fine map is loaded; poll often enough that a newly seen country warms
# quickly, but the work is deduplicated + timer-gated (see boundaries.py), so most polls are
# cheap no-ops. (The world map loads once at startup, in the loader's __aenter__.)
BOUNDARY_POLL_INTERVAL = timedelta(seconds=15)


if __name__ == "__main__":
    dispatch({
        "boundaries": lambda: run(boundaries_stage, application_id="adsb-boundaries",
                                  client_id="adsb-boundaries-0", metrics_port=9107,
                                  poll_interval=BOUNDARY_POLL_INTERVAL),
        "ingest": lambda: run(ingest_stage, application_id="adsb-ingest", client_id="adsb-ingest-0",
                              metrics_port=9101, poll_interval=ADSB_POLL_INTERVAL),
        "enrich": lambda: run(enrich_stage, application_id="adsb-enrich", client_id="adsb-enrich-0",
                              metrics_port=9105),
        "conflict": lambda: run(conflict_stage, application_id="adsb-conflict", client_id="adsb-conflict-0",
                                metrics_port=9106),
    })

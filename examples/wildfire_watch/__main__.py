"""Run a Wildfire Watch stage against the shared stack.

    uv run poe setup-wildfire        # config topic + three data topics + ClickHouse schema
    uv run poe request-wildfire …    # curate a watch region (validated); see request.py
    uv run poe run-wildfire-ingest   # stage 1: poll both VIIRS birds -> wildfire-detections
    uv run poe run-wildfire-tracker  # stage 2: cluster detections -> wildfire-status / wildfire-events

Each target selects a stage by name (``python -m examples.wildfire_watch <stage>``) and runs it
through the shared ``examples._runner``. The demo constants live here, in the ops caller. The
``metrics_port``s match the fires targets in ``prometheus/prometheus.yml``.

**The credential exception, stated plainly.** Stages never read the environment — that is the
framework's rule, and every other example honours it absolutely. Here the ingest stage needs
NASA's ``FIRMS_MAP_KEY``, and a secret is exactly the kind of value that must not be baked into
a module or committed to a config topic in the clear. So the *ops caller* reads it from the
environment and **injects it as a constructor argument**: ``FirmsIngest(map_key=…)``. The stage
itself still touches no ``os.environ``, so the rule holds where it matters — configuration
enters through the caller. This mirrors the ``chaos_harness`` precedent (whose ``__main__``
reads an env-driven ``application_id`` to prove transactional fencing), restated for secrets.

The key is read **lazily, per stage**, so ``run-wildfire-tracker`` works without one — only the
poller needs NASA credentials. ``flechtwerk`` also ships a keyring/secrets facility that could
carry the key as an encrypted config attribute; that deserves its own example (see the README's
extension points) rather than being smuggled in here.
"""
import os
import sys
from datetime import timedelta

from examples._runner import dispatch, run

from .ingest import FirmsIngest
from .tracker import stage as tracker_stage

MAP_KEY_ENV = "FIRMS_MAP_KEY"
"""Environment variable holding the NASA FIRMS MAP_KEY, read by this ops caller only."""

POLL_INTERVAL = timedelta(minutes=5)
"""Two GETs per region per poll (NOAA-20 + NOAA-21).

NRT data lands up to ~3 h after acquisition and a satellite revisit is hours apart, so polling
faster would mostly re-fetch the same rows. At 5 minutes even 20 regions cost ~480 requests per
10-minute interval against NASA's 5000-transaction budget — and note a *large* bounding box can
count as several transactions, so the headroom is real rather than theoretical."""


def _map_key() -> str:
    """The FIRMS MAP_KEY from the environment, or a crisp exit explaining how to get one.

    Fails once, loudly, at startup rather than letting every poll 400 — a stage that crashed on
    each poll would just restart-spam the supervisor with an opaque HTTP error."""
    key = os.environ.get(MAP_KEY_ENV, "").strip()
    if not key:
        sys.exit(
            f"{MAP_KEY_ENV} is not set — the FIRMS ingest stage needs a (free, instant) NASA "
            f"MAP_KEY.\n"
            f"  1. Request one at https://firms.modaps.eosdis.nasa.gov/api/map_key/\n"
            f"  2. export {MAP_KEY_ENV}=<your key>\n"
            f"Then re-run. (The tracker stage needs no key: uv run poe run-wildfire-tracker.)")
    return key


if __name__ == "__main__":
    dispatch({
        # The lambda defers both the env read and the client construction, so selecting the
        # tracker never demands a key.
        "ingest": lambda: run(FirmsIngest(map_key=_map_key()), application_id="wildfire-ingest",
                              client_id="wildfire-ingest-0", metrics_port=9120,
                              poll_interval=POLL_INTERVAL),
        "tracker": lambda: run(tracker_stage, application_id="wildfire-tracker",
                               client_id="wildfire-tracker-0", metrics_port=9121),
    })

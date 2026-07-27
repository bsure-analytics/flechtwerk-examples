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

POLL_INTERVAL = timedelta(minutes=15)
"""Two GETs per region per poll (NOAA-20 + NOAA-21).

Freshness costs nothing here: NRT data lands up to ~3 h after acquisition and a satellite revisit
is hours apart, so a faster cadence mostly re-fetches the same rows. **NASA's meter sets the
number.** The quota is 5000 transactions per 10-minute interval, an area request is billed as
several transactions (~4, measured), and the framework polls *every* active config concurrently
per cycle — so one round costs ``regions × 2 × ~4`` transactions in a burst, and the interval has
to keep two rounds out of one 10-minute window. The world watch is the binding case: ~350 tiles
≈ 2800 transactions per round, which at the original 5 minutes put **two rounds inside every
window (~5600 > 5000)** and exhausted the key — after which FIRMS 400s everything and the crash
loop re-spends immediately, so the quota never drains. 15 minutes leaves a full round of headroom
for exactly that restart. A handful of named regions is nowhere near the meter (20 regions ≈ 160
transactions per round) and may be polled faster if you want a livelier demo."""


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

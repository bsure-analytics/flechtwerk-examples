"""Run a Wildfire Watch stage against the shared stack.

    uv run poe setup-wildfire        # config topic + three data topics + ClickHouse schema
    uv run poe request-wildfire …    # curate a watch region (validated); see request.py
    uv run poe run-wildfire-ingest   # stage 1: poll both VIIRS birds -> wildfire-detections
    uv run poe run-wildfire-tracker  # stage 2: cluster detections -> wildfire-status / wildfire-events

Each target selects a stage by name (``python -m examples.wildfire_watch <stage>``) and runs it
through the shared ``examples._runner``. The demo constants live here, in the ops caller. The
``metrics_port``s match the fires targets in ``prometheus/prometheus.yml``.

**The credential rides encrypted on a config topic.** The ingest stage needs NASA's FIRMS
MAP_KEY. ``request-wildfire key`` writes it to ``wildfire-credentials`` as an ``ENCRYPTED(STR)``
attribute, and the stage decrypts it on read with the process keyring — the ``the_movie_database``
pattern. What this ops caller injects is therefore the **keyring** (``_runner.run(keyring=...)``),
loaded by ``examples._setup.load_keyring``; it reads only the keyring *path* from the environment
(``FLECHTWERK_KEYRING``). Key material never enters a stage module or a topic in the clear, and the
stages touch no ``os.environ``. The keyring is loaded lazily, per stage, so ``run-wildfire-tracker``
needs none.

**The key is part of every FIRMS URL** (``/api/area/csv/<MAP_KEY>/…``), so httpx's INFO request
log would print it on every poll. ``_run_ingest`` raises the ``httpx`` logger to WARNING —
encrypting a secret on the topic and then logging it in plaintext would defeat the point. An
HTTP *error* still names the URL in its exception (let it crash), which is worth knowing before
you paste a traceback.
"""
import logging
from datetime import timedelta

from examples._runner import dispatch, run
from examples._setup import load_keyring

from .ingest import stage as ingest_stage
from .tracker import stage as tracker_stage

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


def _run_ingest() -> None:
    logging.getLogger("httpx").setLevel(logging.WARNING)  # its request log would print the MAP_KEY
    run(ingest_stage, application_id="wildfire-ingest", client_id="wildfire-ingest-0",
        metrics_port=9120, poll_interval=POLL_INTERVAL, keyring=load_keyring())


if __name__ == "__main__":
    dispatch({
        "ingest": _run_ingest,  # the only stage that needs the keyring
        "tracker": lambda: run(tracker_stage, application_id="wildfire-tracker",
                               client_id="wildfire-tracker-0", metrics_port=9121),
    })

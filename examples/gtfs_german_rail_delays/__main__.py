"""Run a GTFS delay-monitor stage against the shared stack.

    uv run poe setup-trains         # topics + feed configs + ClickHouse schema
    uv run poe run-trains-loader    # stage 0: fv static feed -> gtfs-trip-profiles
    uv run poe run-trains-ingest    # stage 1: poll the RT protobuf feed -> gtfs-trip-updates
    uv run poe run-trains-delays    # stage 2: join updates x profiles -> gtfs-train-delays

Each target selects a stage by name (``python -m examples.gtfs_german_rail_delays <stage>``)
and runs it through the shared ``examples._runner``. The demo constants live here, in
the ops caller — the framework reads nothing from the environment. The ``metrics_port``s
match the GTFS targets in ``prometheus/prometheus.yml``.
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .delays import stage as delays_stage
from .ingest import stage as ingest_stage
from .loader import stage as loader_stage

# The RT feed refreshes every 10 s, but a full ~52 MB national snapshot per poll is heavy
# and we don't need every one — 60 s keeps the map fresh without hammering the free feed.
INGEST_POLL_INTERVAL = timedelta(seconds=60)
# The static schedule changes at most daily; re-check a few times a day (an unchanged
# ETag makes it a no-op) so a mid-day revision is picked up without constant downloads.
LOADER_POLL_INTERVAL = timedelta(hours=6)


if __name__ == "__main__":
    dispatch({
        "loader": lambda: run(loader_stage, application_id="gtfs-loader", client_id="gtfs-loader-0",
                              metrics_port=9114, poll_interval=LOADER_POLL_INTERVAL),
        "ingest": lambda: run(ingest_stage, application_id="gtfs-ingest", client_id="gtfs-ingest-0",
                              metrics_port=9112, poll_interval=INGEST_POLL_INTERVAL),
        "delays": lambda: run(delays_stage, application_id="gtfs-delays", client_id="gtfs-delays-0",
                              metrics_port=9113),
    })

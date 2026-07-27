"""Run an F1 Live Timing stage against the shared stack.

    uv run poe setup-f1        # config topic + four data topics + ClickHouse schema
    uv run poe request-f1 …    # choose which sessions to ingest; see request.py
    uv run poe run-f1-ingest   # stage 1: read/tail the live-timing tape -> f1-timing
    uv run poe run-f1-timing   # stage 2: fold the tape into the board -> f1-status / -events / -telemetry

Each target selects a stage by name (``python -m examples.f1_live_timing <stage>``) and runs it
through the shared ``examples._runner``. The demo constants live here, in the ops caller — no
stage reads the environment, and this example needs no credential to read either.
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .ingest import stage as ingest_stage
from .timing import stage as timing_stage

POLL_INTERVAL = timedelta(seconds=10)
"""How often each session target is polled.

**The interval does not govern backfill throughput** — the per-poll line budget does (see
``ingest.LINE_BUDGET``), so a race backfills in minutes at any sane interval and a *finished*
session costs zero requests per poll regardless. What the interval sets is **live latency**: a
tailing poll adds up to 10 s on top of the archive's own append/CDN delay and the 30 s safety
margin the live frontier keeps, so a live session reaches the dashboard ~15–45 s behind the
television feed.

Ten seconds is also the politeness budget. Every configured session issues one small ranged read
per feed per poll — 14 feeds, or 16 with telemetry — and the framework polls all of them
concurrently. That is fine for the handful of live-or-backfilling sessions this is for, and it is
worth knowing that requesting a whole season means those reads only until each session completes,
after which the cost drops to nothing."""


if __name__ == "__main__":
    dispatch({
        "ingest": lambda: run(ingest_stage, application_id="f1-ingest", client_id="f1-ingest-0",
                              metrics_port=9122, poll_interval=POLL_INTERVAL),
        "timing": lambda: run(timing_stage, application_id="f1-timing", client_id="f1-timing-0",
                              metrics_port=9123),
    })

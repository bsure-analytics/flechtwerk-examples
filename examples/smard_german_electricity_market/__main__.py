"""Run a SMARD electricity-market stage against the shared stack.

    uv run poe setup-smard        # config topic + series configs + ClickHouse schema
    uv run poe run-smard-ingest   # stage 1: poll each series -> smard-observations
    uv run poe run-smard-mix      # stage 2: join series by interval -> smard-mix

Each target selects a stage by name (``python -m examples.smard_german_electricity_market <stage>``)
and runs it through the shared ``examples._runner``. The demo constants live here, in the
ops caller — the framework reads nothing from the environment. The ``metrics_port``s match
the SMARD targets in ``prometheus/prometheus.yml``.
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .ingest import stage as ingest_stage
from .mix import stage as mix_stage

# SMARD publishes a new quarter-hour every 15 min and revises recent ones through the day.
# 120 s keeps the board fresh and the corrections feed lively without hammering a public
# authority (one index GET + one or two small file GETs per series per poll).
INGEST_POLL_INTERVAL = timedelta(seconds=120)


if __name__ == "__main__":
    dispatch({
        "ingest": lambda: run(ingest_stage, application_id="smard-ingest", client_id="smard-ingest-0",
                              metrics_port=9115, poll_interval=INGEST_POLL_INTERVAL),
        "mix": lambda: run(mix_stage, application_id="smard-mix", client_id="smard-mix-0",
                           metrics_port=9116),
    })

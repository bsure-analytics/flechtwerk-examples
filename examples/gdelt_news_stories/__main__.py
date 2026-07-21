"""Run a GDELT news-stories pipeline stage against the shared stack.

    uv run poe setup-gdelt          # topics + feed/outlet configs + ClickHouse schema
    uv run poe run-gdelt-ingest     # stage 1: poll GDELT -> gdelt-{events,mentions,gkg}-raw
    uv run poe run-gdelt-coverage   # stage 2a: Events ⋈ Mentions join -> gdelt-event-coverage
    uv run poe run-gdelt-stories    # stage 2b: cluster GKG -> gdelt-stories
    uv run poe run-gdelt-sink       # stage 3: sink stories + coverage -> ClickHouse

Each target selects a stage by name (``python -m examples.gdelt_news_stories <stage>``) and
runs it through the shared ``examples._runner``. The demo constants live here, in the ops
caller — the framework reads nothing from the environment. The ``metrics_port``s match the
GDELT targets in ``prometheus/prometheus.yml``. (The outlet table is static bundled data,
seeded onto ``gdelt-outlets`` by ``setup.py`` — no runtime stage.)
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .coverage import stage as coverage_stage
from .ingest import stage as ingest_stage
from .sink import stage as sink_stage
from .stories import stage as stories_stage

# GDELT publishes a new 15-minute slice every 15 min; polling the (cheap) pointer once a
# minute picks it up promptly without hammering the free feed.
INGEST_POLL_INTERVAL = timedelta(seconds=60)


if __name__ == "__main__":
    dispatch({
        "ingest": lambda: run(ingest_stage, application_id="gdelt-ingest", client_id="gdelt-ingest-0",
                              metrics_port=9108, poll_interval=INGEST_POLL_INTERVAL),
        "coverage": lambda: run(coverage_stage, application_id="gdelt-coverage", client_id="gdelt-coverage-0",
                                metrics_port=9109),
        "stories": lambda: run(stories_stage, application_id="gdelt-stories", client_id="gdelt-stories-0",
                               metrics_port=9110),
        "sink": lambda: run(sink_stage, application_id="gdelt-sink", client_id="gdelt-sink-0",
                            metrics_port=9111),
    })

"""Run the ClickHouse sink stage against the shared stack.

    uv run poe setup-sink        # ensure the input topic + apply the schema
    uv run poe run-adsb          # (example 1) run the pipeline -> adsb-aircraft
    uv run poe run-sink          # then run this

A transformer needs no poll_interval. See the module docstring in ``sink.py`` for
why the DB write is at-least-once and how the dedup token tames it. The single
stage runs through the shared ``examples._runner``; config is injected here, not
read from the environment.
"""
from examples._runner import dispatch, run

from .sink import stage as sink_stage

if __name__ == "__main__":
    dispatch({
        "sink": lambda: run(sink_stage, application_id="clickhouse-sink", client_id="clickhouse-sink-0",
                            metrics_port=9102),
    })

"""Run a Glucose Monitor stage against the shared stack.

    uv run poe setup-glucose            # config topic + two data topics + ClickHouse schema
    uv run poe request-glucose …        # store a follower account (encrypted) / retire one
    uv run poe run-glucose-ingest       # stage 1: back up each followed patient's graph -> glucose-raw
    uv run poe run-glucose-readings     # stage 2: dedupe the repeating window -> glucose-readings

Each target selects a stage by name (``python -m examples.glucose_monitor <stage>``) and runs it
through the shared ``examples._runner``. The demo constants live here, in the ops caller. The
``metrics_port``s match the glucose targets in ``prometheus/prometheus.yml``.

**The credentials ride encrypted on the config topic**, as in ``the_movie_database``: the
LibreLinkUp email and password are ``ENCRYPTED(STR)`` attributes of each ``glucose-accounts``
record, so what this caller injects is the **keyring** (``_runner.run(keyring=...)``), loaded
from the file ``FLECHTWERK_KEYRING`` names by ``examples._setup.load_keyring`` — and only for
``ingest``: ``readings`` reads no config topic and never sees a credential.
"""
from datetime import timedelta

from examples._runner import dispatch, run
from examples._setup import load_keyring

from .ingest import stage as ingest_stage
from .readings import stage as readings_stage

POLL_INTERVAL = timedelta(minutes=1)
"""A Libre 3 sends a reading to the phone every minute, and the phone uploads it; polling faster
would only fetch the same reading again. Two requests per patient per poll — raise the interval
if LibreView starts answering 429."""

if __name__ == "__main__":
    dispatch({
        # The lambda defers the keyring load, so selecting `readings` never loads one it can't use.
        "ingest": lambda: run(ingest_stage, application_id="glucose-ingest",
                              client_id="glucose-ingest-0", metrics_port=9127,
                              poll_interval=POLL_INTERVAL, keyring=load_keyring()),
        "readings": lambda: run(readings_stage, application_id="glucose-readings",
                                client_id="glucose-readings-0", metrics_port=9128),
    })

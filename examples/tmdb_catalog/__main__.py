"""Run a TMDB Catalog stage against the shared stack.

    uv run poe setup-tmdb            # config topic + six data topics + ClickHouse schema
    uv run poe request-tmdb …        # store the encrypted token / set an epoch / retire a feed
    uv run poe run-tmdb-changes      # stage 1: back up each feed's daily change pages -> tmdb-changes-raw
    uv run poe run-tmdb-split        # stage 2: split pages into per-entity changes -> tmdb-changes
    uv run poe run-tmdb-catalog      # stage 3: fetch, project, version -> tmdb-movies/-tv/-people/-credits

Each target selects a stage by name (``python -m examples.tmdb_catalog <stage>``) and runs it
through the shared ``examples._runner``. The demo constants live here, in the ops caller. The
``metrics_port``s match the tmdb targets in ``prometheus/prometheus.yml``.

**The credential rides encrypted on the config topic.** The TMDB token is an ``ENCRYPTED(STR)`` attribute of each ``tmdb-feeds`` record: ``request-tmdb token``
writes it as ciphertext, and a stage decrypts it on read with the process keyring. What the ops
caller injects is therefore the **keyring** (``_runner.run(keyring=...)``), loaded from a file by
``examples._setup.load_keyring``. This caller reads only the keyring *path* from the environment
(``FLECHTWERK_KEYRING``, defaulting to ``~/.config/flechtwerk-examples/keyring.json``) — key
material never enters a stage module or a topic, and the stages touch no ``os.environ``.
``wildfire_watch`` does the same for NASA's key, but on one credential record of its own topic
rather than on every target record.

The keyring is loaded lazily, per stage, inside the lambdas — and not at all for ``split``, which
reads no config topic and so never sees the token.
"""
from datetime import timedelta

from examples._runner import dispatch, run
from examples._setup import load_keyring

from .catalog import stage as catalog_stage
from .changes import stage as changes_stage
from .split import stage as split_stage

POLL_INTERVAL = timedelta(hours=1)
"""One hourly poll lists *today's* change feed (the open day); the day's closing re-list rides the
first poll after midnight UTC. TMDB's feed is daily-granular and its terms are non-commercial, so
there is nothing to gain from polling faster: freshness is one poll interval. (A closed day is listed once;
ids TMDB adds to it later are caught at their next change — see the README's caveats.)"""

if __name__ == "__main__":
    dispatch({
        # The lambdas defer the keyring load, so selecting a stage never loads one it can't use.
        "changes": lambda: run(changes_stage, application_id="tmdb-changes",
                               client_id="tmdb-changes-0", metrics_port=9124,
                               poll_interval=POLL_INTERVAL, keyring=load_keyring()),
        "split": lambda: run(split_stage, application_id="tmdb-split",
                             client_id="tmdb-split-0", metrics_port=9125),
        "catalog": lambda: run(catalog_stage, application_id="tmdb-catalog",
                               client_id="tmdb-catalog-0", metrics_port=9126,
                               keyring=load_keyring()),
    })

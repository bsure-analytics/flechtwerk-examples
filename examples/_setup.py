"""Shared ops helpers for the examples' one-shot ``setup.py`` scripts.

Only setup-time infrastructure quirks live here — never stage logic. Stages get
their configuration injected (``Flechtwerk.of(...)`` or a config-topic record), as
the framework insists; this is the setup twin of ``examples._runner``.
"""
import contextlib
import logging
import os
import sys
from pathlib import Path

from flechtwerk.keyring import Keyring

KEYRING_ENV = "FLECHTWERK_KEYRING"
DEFAULT_KEYRING = Path("~/.config/flechtwerk-examples/keyring.json")


@contextlib.contextmanager
def quiet_fresh_topic_produce_race():
    """Silence aiokafka's guaranteed-transient warning when seeding a brand-new topic.

    Producing to a just-created topic races the broker finishing leader election:
    ``create_topics`` returns once the *controller* records the topic (metadata
    already names a leader), but the broker's replica manager may not have *become*
    leader for the new partitions yet, so the first produce gets a
    ``NotLeaderForPartitionError``. It is transient on a single broker — aiokafka
    refreshes metadata, retries, and the produce lands — but it logs an alarming
    WARNING that makes an otherwise clean first ``setup-*`` look failed. Metadata-
    level waiting can't close the window (the controller names the leader before the
    broker acts on it), so we quiet just that one logger for the duration of the
    seed. Anything worse than the retry (logged at ERROR) still surfaces.
    """
    sender_log = logging.getLogger("aiokafka.producer.sender")
    previous = sender_log.level
    sender_log.setLevel(logging.ERROR)
    try:
        yield
    finally:
        sender_log.setLevel(previous)


def keyring_path(env_var: str = KEYRING_ENV, default: Path = DEFAULT_KEYRING) -> Path:
    """Where the keyring file is expected: ``$FLECHTWERK_KEYRING`` or ``default``, ``~`` expanded.
    For an ops caller that can do without one (a request tool's optional preview)."""
    return Path(os.environ.get(env_var) or default).expanduser()


def load_keyring(env_var: str = KEYRING_ENV, default: Path = DEFAULT_KEYRING) -> Keyring:
    """Load the keyring file that encrypts and decrypts ``ENCRYPTED`` config attributes.

    The path is ``$FLECHTWERK_KEYRING`` or ``default`` (``~`` expanded); a missing file
    exits with the command that creates one. This is an ops-side environment read — the
    ``wildfire_watch`` / ``chaos_harness`` precedent, restated for key material: it is
    the *path* that comes from the environment, the keys stay in the file, and neither
    may ever enter a stage module or a topic. Stages never call this; the ops caller
    (``__main__.py``, ``request.py``) loads the keyring and injects it
    (``examples._runner.run(keyring=...)``).
    """
    path = keyring_path(env_var, default)
    if not path.is_file():
        sys.exit(
            f"no keyring at {path}\n"
            f"create one: uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) {path}\n"
            f"then: chmod 600 {path}"
        )
    return Keyring.from_json(path.read_text())

"""Shared ops helpers for the examples' one-shot ``setup.py`` scripts.

Only setup-time infrastructure quirks live here — never stage logic. Stages get
their configuration injected (``Flechtwerk.of(...)`` or a config-topic record), as
the framework insists; this is the setup twin of ``examples._runner``.
"""
import contextlib
import logging


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

"""Shared entry-point runner for the example stages.

Every example runs its stages the same way: configure logging, build a
``Flechtwerk`` from injected demo constants, and run it until Ctrl-C. This module
is the *single* copy of that boilerplate. Each example's ``__main__.py`` is a thin
dispatcher that maps a stage name to a call of :func:`run` with that stage's demo
constants (:func:`dispatch` does the name → callable lookup).

Configuration is injected here by the ops caller, never read from the environment
— the framework's rule (see ``CLAUDE.md``). The one exception is ``chaos_harness``,
whose harness spawns short-lived copies with an env-driven ``application_id`` to
prove transactional fencing; its ``__main__`` reads those env values and passes
them straight into :func:`run`.
"""
import logging
import sys
from collections.abc import Callable
from datetime import timedelta

from flechtwerk import Extractor, Flechtwerk, MqttBrokerConfig, Transformer

if sys.platform == "win32":  # uvloop is POSIX-only; stock asyncio elsewhere (e.g. Windows)
    import asyncio as _loop
else:
    import uvloop as _loop

log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"


def run(
    stage: Extractor | Transformer,
    *,
    application_id: str,
    client_id: str,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    metrics_port: int = 0,
    poll_interval: timedelta | None = None,
    mqtt: MqttBrokerConfig | None = None,
) -> None:
    """Configure logging and run one stage on uvloop until interrupted.

    ``client_id`` is the process identity and doubles as the metrics label
    (``metrics_labels`` must be non-empty when ``metrics_port > 0``). Extractors
    pass a ``poll_interval``; transformers omit it. Only the MQTT bridge passes
    ``mqtt``.

    **Ctrl-C** (SIGINT) is turned by ``uvloop.run`` into a cancel of the main task,
    which unwinds ``Flechtwerk.__aexit__`` (consumers leave the group, producers flush,
    open transactions abort) before ``KeyboardInterrupt`` re-raises here and logs
    "Shutting down". **SIGTERM is deliberately left as Python's default — a prompt
    kill** (it is what the ``kill 0`` in the ``run-<example>`` supervisors sends, and
    what K8s sends): a stage cancelled mid-batch can take a while to unwind, so the
    supervisors rely on SIGTERM terminating promptly rather than waiting on a drain.
    Nothing is lost — exactly-once holds because state lives in the Kafka changelog and
    each page/batch is a transaction, so an abrupt kill just aborts the in-flight
    transaction and the next start recovers ("let it crash"). Production wires SIGTERM to
    a graceful drain where the pod's termination grace period bounds it; the demo keeps
    the simpler prompt kill.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        stream=sys.stdout,
    )
    try:
        _loop.run(
            Flechtwerk.of(
                application_id=application_id,
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                metrics_labels={"client_id": client_id},
                metrics_port=metrics_port,
                mqtt=mqtt,
                poll_interval=poll_interval,
                stage=stage,
            ).run()
        )
    except KeyboardInterrupt:
        log.info("Shutting down")


def dispatch(stages: dict[str, Callable[[], None]]) -> None:
    """Run one named stage: ``python -m examples.<package> <name>``.

    With a single stage the name may be omitted (``python -m examples.<package>``).
    """
    argv = sys.argv[1:]
    if not argv and len(stages) == 1:
        name = next(iter(stages))
    elif len(argv) == 1 and argv[0] in stages:
        name = argv[0]
    else:
        sys.exit(f"usage: python -m examples.<package> {{{'|'.join(stages)}}}")
    stages[name]()

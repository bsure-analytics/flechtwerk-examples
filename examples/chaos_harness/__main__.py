"""Run the sequencer transformer — the process the chaos harness SIGKILLs.

This is the ops entrypoint (not a stage), and it is the framework's one deliberate
exception to "no environment-variable magic": it reads its wiring from the
environment because the harness (``chaos.py``) spawns many short-lived copies of
it, and the integration test points them at an ephemeral broker. Every copy uses
the same ``application_id``, so a restart's transactional producer fences the
SIGKILLed one (InitProducerId) and restores state from the changelog before
resuming: exactly-once across the crash.

Metrics are off — a killed process can leave its scrape port in TIME_WAIT, and
rapid restarts would race to rebind it — so ``run`` is called with the default
``metrics_port=0``.
"""
import os

from examples._runner import run

from .transformer import stage

if __name__ == "__main__":
    run(
        stage,
        application_id=os.environ.get("CHAOS_APPLICATION_ID", "chaos-harness"),
        client_id=os.environ.get("CHAOS_CLIENT_ID", "chaos-harness-0"),
        bootstrap_servers=os.environ.get("CHAOS_BOOTSTRAP", "localhost:9092"),
    )

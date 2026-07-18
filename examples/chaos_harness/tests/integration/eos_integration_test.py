"""Tier 3 — the executable exactly-once claim, under real SIGKILLs.

Produces a run of inputs, then runs the actual chaos harness against an
ephemeral Kafka: it spawns the transformer as a subprocess and `SIGKILL`s it
mid-batch, repeatedly, before a final copy drains the rest. Recovery leans on
the real thing — InitProducerId fencing of the killed producer and changelog
restore — with nothing simulated. The output must then carry every input exactly
once with a gap-free counter.

Slow by nature (each restart rejoins the group), so the record count is sized to
guarantee the kills land mid-run.
"""
from uuid import uuid4

import pytest

from examples.chaos_harness.chaos import run_chaos
from examples.chaos_harness.setup import create_topics, produce_input
from examples.chaos_harness.verify import verify

pytestmark = pytest.mark.integration

RECORD_COUNT = 100_000


async def test_exactly_once_under_repeated_sigkill(kafka_bootstrap: str) -> None:
    application_id = f"chaos-{uuid4().hex[:8]}"
    await create_topics(kafka_bootstrap)
    await produce_input(kafka_bootstrap, RECORD_COUNT)

    await run_chaos(
        bootstrap=kafka_bootstrap,
        application_id=application_id,
        client_id=f"{application_id}-0",
        target=RECORD_COUNT,
        kills=3,
    )

    result = await verify(kafka_bootstrap, RECORD_COUNT)
    assert not result["duplicates"], f"duplicates downstream: {result}"
    assert result["complete"], f"gaps — not every input reached the output: {result}"
    assert result["seq_exact"], f"the counter double-counted or skipped: {result}"
    assert result["total"] == RECORD_COUNT, result

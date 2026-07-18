"""The chaos sidecar: SIGKILL the transformer mid-batch, on a loop.

Spawns the transformer as a subprocess, waits until it has made fresh progress
(so the kill lands mid-run, not before it starts or after it finishes),
`SIGKILL`s it, and repeats — then lets a final copy run to completion. Every
copy shares the `application_id`, so a restart fences the killed producer
(InitProducerId) and restores state from the changelog before resuming.

Progress and completion are read from the transformer's own **transactionally
committed** input offset — the exactly-once cursor itself — so the harness never
races the data path.
"""
import asyncio
import os
import sys
from pathlib import Path

from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from .transformer import INPUT_TOPIC

REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SERVERS = "localhost:9092"


async def spawn(bootstrap: str, application_id: str, client_id: str) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "CHAOS_BOOTSTRAP": bootstrap,
        "CHAOS_APPLICATION_ID": application_id,
        "CHAOS_CLIENT_ID": client_id,
    }
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "examples.chaos_harness",
        cwd=str(REPO_ROOT), env=env,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )


async def _committed(admin: AIOKafkaAdminClient, application_id: str) -> int:
    """The transformer's committed input offset — its exactly-once cursor.

    Before the transformer's consumer has joined, the group has no coordinator
    yet; that (and any transient admin hiccup) simply reads as "no progress".
    """
    try:
        offsets = await admin.list_consumer_group_offsets(application_id)
    except Exception:
        return 0
    committed = offsets.get(TopicPartition(INPUT_TOPIC, 0))
    return committed.offset if committed and committed.offset > 0 else 0


async def _kill(proc: asyncio.subprocess.Process | None) -> None:
    if proc is not None and proc.returncode is None:
        proc.kill()
        await proc.wait()


async def run_chaos(*, bootstrap: str, application_id: str, client_id: str, target: int,
                    kills: int = 3, poll_interval: float = 0.1, kill_timeout: float = 90.0,
                    finish_timeout: float = 240.0, log=print) -> None:
    """Kill the transformer the instant it commits its first fresh page.

    The transformer drains fast, so polling the committed cursor slowly would
    keep missing the window and kill only after completion. We poll tightly
    (`poll_interval`) off a reused admin client and SIGKILL at the first offset
    past the pre-spawn baseline — squarely mid-run — then let a final copy drain.
    """
    loop = asyncio.get_running_loop()
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    proc: asyncio.subprocess.Process | None = None
    try:
        for i in range(kills):
            proc = await spawn(bootstrap, application_id, client_id)
            baseline = await _committed(admin, application_id)
            deadline = loop.time() + kill_timeout
            offset = baseline
            while offset <= baseline and offset < target and loop.time() < deadline:
                await asyncio.sleep(poll_interval)
                offset = await _committed(admin, application_id)
            log(f"SIGKILL #{i + 1} — transformer at input offset {offset}/{target}")
            await _kill(proc)
            proc = None
            if offset >= target:
                break  # drained before we could land a kill — no work left to disrupt
        # Recovery: a final copy runs to completion.
        proc = await spawn(bootstrap, application_id, client_id)
        deadline = loop.time() + finish_timeout
        while (offset := await _committed(admin, application_id)) < target:
            if loop.time() > deadline:
                raise TimeoutError(f"stalled at input offset {offset}/{target} after the chaos")
            if proc.returncode is not None:
                proc = await spawn(bootstrap, application_id, client_id)  # crashed on its own — restart
            await asyncio.sleep(1.0)
        log(f"recovered — input fully consumed ({offset}/{target})")
    finally:
        await _kill(proc)
        await admin.close()


async def main() -> None:
    from uuid import uuid4

    from .setup import RECORD_COUNT

    # A fresh application_id per run: its consumer group starts at earliest and
    # its changelog starts empty (counter from 1), so `setup-chaos` need not
    # reset any group offsets. Every spawn within this run shares the id, so a
    # restart still fences the SIGKILLed producer.
    application_id = f"chaos-harness-{uuid4().hex[:8]}"
    await run_chaos(
        bootstrap=BOOTSTRAP_SERVERS,
        application_id=application_id,
        client_id=f"{application_id}-0",
        target=RECORD_COUNT,
    )
    print("Chaos complete — now run: uv run poe verify-chaos")


if __name__ == "__main__":
    asyncio.run(main())

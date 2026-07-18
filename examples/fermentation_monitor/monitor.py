"""The gravity monitor — a stateful transformer over fermentation readings.

Per batch it keeps the gravity curve as `State` and:

- **stalls**: if gravity stops falling for `STALL_READINGS` readings while still
  above the final gravity, it emits a `stall` alert (once, at onset);
- **bottling**: when gravity reaches the final gravity, fermentation is done — it
  emits a `complete` alert and **tombstones** the batch (a falsy `State`), so the
  monitor stops tracking it.

Bottling is the genuine state-tombstone the plan calls for: the batch's state
entry is deleted and a changelog tombstone written.
"""
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, transformer

from .attributes import AT, BATCH, FLAT_COUNT, GRAVITY, KIND, LAST_GRAVITY

READINGS_TOPIC = "fermentation.readings"
ALERT_TOPIC = "fermentation.alerts"

FINAL_GRAVITY = 1.010
"""At or below this, fermentation is finished — time to bottle."""
DROP_THRESHOLD = 0.001
"""A reading must fall at least this much to count as progress."""
STALL_READINGS = 3
"""Consecutive no-drop readings before a stall alert fires."""


@transformer(input_topics=[READINGS_TOPIC])
async def monitor(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    batch = msg.key
    gravity = msg.value[GRAVITY]
    at = msg.value[AT]
    last = state.get(LAST_GRAVITY)

    if gravity <= FINAL_GRAVITY:
        if last is None:
            # Not tracking this batch — either never seen, or already bottled and
            # tombstoned. A lone at-final reading (e.g. the hydrometer left
            # publishing from an emptied vessel) is not a completion event, so we
            # ignore it rather than re-announcing "complete" on every reading.
            return
        yield Message(key=batch, topic=ALERT_TOPIC,
                      value=Event({BATCH: batch, KIND: "complete", GRAVITY: gravity, AT: at}))
        yield State()  # bottling → tombstone the batch's state
        return

    dropped = last is None or last - gravity >= DROP_THRESHOLD
    flat = 0 if dropped else (state.get(FLAT_COUNT) or 0) + 1
    if flat == STALL_READINGS:  # crossed the threshold on this reading
        yield Message(key=batch, topic=ALERT_TOPIC,
                      value=Event({BATCH: batch, KIND: "stall", GRAVITY: gravity, AT: at}))
    yield State({LAST_GRAVITY: gravity, FLAT_COUNT: flat})


stage = monitor
"""The stage the dispatcher runs (``python -m examples.fermentation_monitor monitor``)."""

"""The transformer under test: a stateful exactly-once sequencer.

All input records share one key, so they land in a single bucket and are
processed serially — the state is a genuine running counter. For each input `n`
it emits `{n, seq}` where `seq` comes from the counter, then persists the
counter. Output message, state changelog write, and input-offset commit all ride
one Kafka transaction, so a SIGKILL can only leave a page fully applied or fully
aborted — never half. That is what the chaos harness proves.
"""
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, transformer

from .attributes import COUNT, SEQ

INPUT_TOPIC = "chaos-input"
OUTPUT_TOPIC = "chaos-output"
STATE_KEY = "sequencer"
"""One key for every input record — a single serial bucket, one counter."""


@transformer(input_topics=[INPUT_TOPIC])
async def sequencer(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    count = (state.get(COUNT) or 0) + 1
    # Spread the input forward and stamp the counter — enrich without mutating,
    # and any field the input carried is preserved.
    yield Message(key=msg.key, topic=OUTPUT_TOPIC, value=Event({**msg.value, SEQ: count}))
    yield State({COUNT: count})


stage = sequencer
"""The stage the chaos ``__main__`` runs (the process the harness SIGKILLs)."""

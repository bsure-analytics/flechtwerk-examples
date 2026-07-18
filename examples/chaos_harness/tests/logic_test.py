"""Tier 1 — pure logic. The sequencer as a plain async generator.

Feed the yielded State back in and the counter continues — the same mechanism
that, backed by the changelog, survives a crash in the integration tier.
"""
import json

from flechtwerk import Message, State
from flechtwerk.kafka import parse_message
from flechtwerk.testing import make_record

from examples.chaos_harness.attributes import COUNT, N, SEQ
from examples.chaos_harness.transformer import INPUT_TOPIC, OUTPUT_TOPIC, STATE_KEY, sequencer


def _msg(n: int, *, offset: int = 0):
    return parse_message(make_record(
        key=STATE_KEY, value=json.dumps({"n": n}), topic=INPUT_TOPIC, offset=offset))


async def _run(msg, state):
    items = [item async for item in sequencer.transform(msg, state)]
    return (
        [i for i in items if isinstance(i, Message)],
        [i for i in items if isinstance(i, State)],
    )


async def test_assigns_seq_from_state_and_persists_the_counter() -> None:
    messages, states = await _run(_msg(5), State())

    assert messages[0].topic == OUTPUT_TOPIC
    assert messages[0].value[N] == 5
    assert messages[0].value[SEQ] == 1
    assert states[0][COUNT] == 1


async def test_counter_continues_from_the_yielded_state() -> None:
    _, states = await _run(_msg(5), State())
    messages, states2 = await _run(_msg(6), states[0])

    assert messages[0].value[N] == 6
    assert messages[0].value[SEQ] == 2  # not reset — the counter carries forward
    assert states2[0][COUNT] == 2

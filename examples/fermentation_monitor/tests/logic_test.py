"""Tier 1 — pure logic. The relay and the monitor, no framework, no broker.

The relay is a plain function; the monitor is driven by feeding each yielded
State back in, exactly as the runner would across readings.
"""
import json

import pytest

from flechtwerk import Config, Message, State
from flechtwerk.attribute import MissingAttributeError, Record
from flechtwerk.kafka import parse_message
from flechtwerk.testing import make_record

from examples.fermentation_monitor.attributes import BATCH, BATTERY, FLAT_COUNT, GRAVITY, KIND, LAST_GRAVITY
from examples.fermentation_monitor.bridge import READINGS_TOPIC, to_reading
from examples.fermentation_monitor.monitor import ALERT_TOPIC, monitor

CONFIG = Config.wrap({"topic": "ispindel/batch-42", "name": "batch-42"})


# --- the relay ---

def test_relay_preserves_the_payload_and_stamps_metadata() -> None:
    payload = Record.wrap({"name": "iSpindel42", "gravity": 1.048, "temperature": 20.5,
                           "angle": 35.1, "battery": 3.9})

    message = to_reading(CONFIG, "ispindel/batch-42", payload)

    assert isinstance(message, Message)
    assert message.key == "batch-42"
    assert message.topic == READINGS_TOPIC
    assert message.value[BATCH] == "batch-42"   # ingestion metadata stamped on
    assert message.value[GRAVITY] == 1.048      # payload preserved verbatim...
    assert message.value[BATTERY] == 3.9        # ...including extra telemetry


def test_relay_poison_drops_a_payload_without_gravity() -> None:
    # No gravity → the typed presence check raises → the template ACK-drops it.
    with pytest.raises(MissingAttributeError):
        to_reading(CONFIG, "ispindel/batch-42", Record.wrap({"name": "x", "temperature": 20.0}))


# --- the monitor ---

def _reading(gravity: float, *, offset: int = 0):
    value = {"batch": "batch-42", "gravity": gravity, "temperature": 20.0, "at": "2026-07-17T12:00:00Z"}
    return parse_message(make_record(key="batch-42", value=json.dumps(value), topic=READINGS_TOPIC, offset=offset))


async def _run_curve(gravities: list[float]):
    """Thread state across a curve, as the runner would; collect the alerts."""
    state = State()
    alerts: list[Message] = []
    for offset, gravity in enumerate(gravities):
        async for item in monitor.transform(_reading(gravity, offset=offset), state):
            if isinstance(item, Message):
                alerts.append(item)
            else:
                state = item
    return alerts, state


async def test_steady_fall_raises_no_alert() -> None:
    alerts, state = await _run_curve([1.050, 1.045, 1.040, 1.035])

    assert alerts == []
    assert state[LAST_GRAVITY] == 1.035
    assert state[FLAT_COUNT] == 0


async def test_stall_alerts_once_at_onset() -> None:
    # falls to 1.040, then flat — the stall counter reaches STALL_READINGS (3).
    alerts, _ = await _run_curve([1.050, 1.040, 1.040, 1.040, 1.040])

    assert [a.value[KIND] for a in alerts] == ["stall"]
    assert alerts[0].key == "batch-42"


async def test_reaching_final_gravity_completes_and_tombstones() -> None:
    alerts, state = await _run_curve([1.050, 1.008])

    assert [a.value[KIND] for a in alerts] == ["complete"]
    assert not state  # bottling → the batch's state is tombstoned (falsy State)


async def test_completion_fires_once_even_if_the_sensor_keeps_publishing() -> None:
    # After bottling tombstones the batch, a hydrometer left in the emptied vessel
    # keeps reporting at/below final gravity. Those readings must NOT re-announce
    # "complete" on every message — completion fires once, at the transition.
    alerts, state = await _run_curve([1.050, 1.008, 1.007, 1.006])

    assert [a.value[KIND] for a in alerts] == ["complete"]
    assert not state  # stays tombstoned; the trailing readings are ignored

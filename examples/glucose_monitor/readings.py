"""Glucose readings — a ``Transformer`` that turns repeating 12-hour windows into new readings.

Stage 2. Each ``glucose-raw`` record is one ``graph`` response exactly as LibreLinkUp delivered
it (the extractor's backup). This stage is where a response is first *read*: it takes out the
latest measurement (``kind = live``) and every ``graphData`` point (``kind = history``),
parses each one's UTC instant and the phone's UTC offset (``projection.readings``), and emits
only the readings it has not emitted before (``projection.fresh``) — one ``glucose-readings``
record each, the item spread through as delivered beside the computed fields.

**The seen-set is the state.** One record per patient (the wire key), holding every emitted
timestamp of the last 24 hours of event time, per kind. A poll a minute repeats ~12 hours of
history, so without it every point would be emitted a few hundred times; with it, a point that
LibreLinkUp fills in late, *behind* the newest one, is still caught — which a high-water mark
would miss.

**Why this is a stage of its own.** It is the transformation the extractor must not do. Keeping
the backup raw means a change of rule here — a different horizon, a third kind — is a replay of
``glucose-raw``, and the backup is the only copy of the past: LibreLinkUp forgets everything
older than its 12-hour window.
"""
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, Transformer
from flechtwerk.attribute import Record

from .attributes import (
    FETCHED_AT,
    KIND,
    MEASURED_AT,
    METADATA,
    PATIENT_ID,
    RAW_TOPIC,
    READINGS_TOPIC,
    RESPONSE,
    SEEN,
    UTC_OFFSET_MIN,
)
from .projection import fresh, readings


class GlucoseReadings(Transformer):
    """Emits each patient's new readings once, deduping the repeating window."""

    input_topics = [RAW_TOPIC]

    def __init__(self, *, readings_topic: str = READINGS_TOPIC) -> None:
        super().__init__()
        self._topic = readings_topic

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        if msg.value is None:
            return
        meta = msg.value[METADATA]
        new, seen = fresh(readings(msg.value[RESPONSE].raw), state.get(SEEN) or {})
        for reading in new:
            value = Event({**Record.wrap(reading.item), PATIENT_ID: meta[PATIENT_ID],
                           KIND: reading.kind, MEASURED_AT: reading.measured_at,
                           FETCHED_AT: meta[FETCHED_AT]})
            if reading.utc_offset_min is not None:
                value[UTC_OFFSET_MIN] = reading.utc_offset_min
            yield Message(key=msg.key, topic=self._topic, value=value)
        yield State({SEEN: seen})


stage = GlucoseReadings()

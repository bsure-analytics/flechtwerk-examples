"""The MQTT→Kafka bridge — an `MqttExtractor` over the hydrometer feed.

`to_reading` is the relay: a small function that turns one hydrometer payload into
a fermentation reading. The framework's template drives it and — the headline
guarantee — ACKs the message to the broker only *after* the batch it belongs to
is durable in Kafka. A missing (or wrong-typed) gravity makes the relay raise on
the typed read, which the template turns into a poison-drop (ACK + warn), never a
crash loop.

Unlike the ADS-B enrich stage's pure `project_page()`, the relay stamps ingest
time with `datetime.now()` (an MQTT hydrometer payload carries no server
timestamp), so it is deliberately *not* pure — the tests assert on the fields it
copies through, not on `at`.
"""
from datetime import datetime, timezone

from flechtwerk import Config, Event, Message
from flechtwerk.attribute import Record
from flechtwerk.mqtt import MqttExtractor

from .attributes import AT, BATCH, GRAVITY, NAME

CONFIG_TOPIC = "fermentation-batches"
READINGS_TOPIC = "fermentation-readings"


def to_reading(config: Config, topic: str, payload: Record) -> Message | None:
    """Relay one hydrometer payload to a fermentation reading.

    Best practice for an extractor: preserve the source verbatim, add only
    ingestion metadata. So the payload is spread through unchanged — every field
    the hydrometer sent (gravity, temperature, tilt angle, battery, …) is kept —
    with the batch and ingest time stamped on top. The one interpretation is the
    presence check on ``gravity``: a payload without it (or with a non-float one)
    is poison, and the typed read raises so the template ACK-drops it instead of
    forwarding a junk reading.
    """
    _ = payload[GRAVITY]  # presence/type check — a reading with no gravity is poison
    return Message(
        key=config[NAME],
        topic=READINGS_TOPIC,
        value=Event({**payload, BATCH: config[NAME], AT: datetime.now(timezone.utc)}),
    )


bridge = MqttExtractor.of(config_topics=[CONFIG_TOPIC], relay=to_reading)

stage = bridge
"""The stage the dispatcher runs (``python -m examples.fermentation_monitor bridge``)."""

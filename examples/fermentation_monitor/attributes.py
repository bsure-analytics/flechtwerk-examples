"""Typed fields for the fermentation monitor.

An iSpindel/Tilt hydrometer publishes a specific-gravity reading over MQTT; the
bridge relays it to Kafka and the monitor tracks each batch's gravity curve,
alerting on a stall and tombstoning the batch when fermentation finishes.
"""
from typing import Final

from flechtwerk.attribute import Attribute, DATETIME, FLOAT, INT, STR

# --- Config: one record per fermentation batch (plus the framework-owned
#     `topic` MQTT filter, flechtwerk.mqtt.TOPIC) ---

NAME: Final = Attribute("name", STR)
"""Batch name — the output message key and the monitor's per-batch state key."""

# --- Shared by the inbound hydrometer payload and the fermentation-readings
#     event (same wire keys, same codecs) ---

GRAVITY: Final = Attribute("gravity", FLOAT)
"""Specific gravity — the one essential reading; the bridge validates it (a
payload with no gravity, or a non-float one, is poison). A strict ``FLOAT`` is
safe at this uncontrolled MQTT boundary because the extractor template
poison-drops a type error (ACK + warn), whereas the ADS-B extractor's ``fetch``
would crash on one — which is why that feed widens its numbers to ``ANY`` and this
one does not. Starts ~1.050, falls toward ~1.008."""
TEMPERATURE: Final = Attribute("temperature", FLOAT, optional=True)
"""Telemetry, carried through verbatim if present (like angle/battery)."""
ANGLE: Final = Attribute("angle", FLOAT, optional=True)
"""iSpindel tilt angle — extra telemetry, carried through verbatim if present."""
BATTERY: Final = Attribute("battery", FLOAT, optional=True)
"""Sensor battery voltage — extra telemetry, carried through verbatim if present."""

# --- fermentation-readings adds ---

BATCH: Final = Attribute("batch", STR)
AT: Final = Attribute("at", DATETIME)

# --- fermentation-alerts ---

KIND: Final = Attribute("kind", STR)
"""``"stall"`` (gravity flat too long) or ``"complete"`` (reached final gravity)."""

# --- Monitor state, per batch ---

LAST_GRAVITY: Final = Attribute("last_gravity", FLOAT)
FLAT_COUNT: Final = Attribute("flat_count", INT)
"""Consecutive readings with no meaningful gravity drop — the stall counter."""

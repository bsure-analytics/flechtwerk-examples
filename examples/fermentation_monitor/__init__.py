"""Fermentation monitor — an MQTT→Kafka bridge plus a stateful gravity monitor.

Both stages run through the package dispatcher (``python -m
examples.fermentation_monitor bridge`` / ``monitor``); each stage module exports
a ``stage`` object the dispatcher runs. The bridge's public surface is re-exported
here for the tests; the monitor is imported from its own module.
"""
from .bridge import CONFIG_TOPIC, READINGS_TOPIC, bridge, to_reading

__all__ = ["CONFIG_TOPIC", "READINGS_TOPIC", "bridge", "to_reading"]

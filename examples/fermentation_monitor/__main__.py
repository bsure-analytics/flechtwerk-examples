"""Run a fermentation stage against the shared stack.

    uv run poe setup-fermentation       # topics, batch configs, ClickHouse schema
    uv run poe simulate-fermentation    # publish hydrometer readings over MQTT
    uv run poe run-fermentation         # bridge: MQTT -> Kafka (ACK after Kafka)
    uv run poe run-fermentation-monitor # the stateful gravity monitor

Each target selects a stage by name (``python -m examples.fermentation_monitor
<stage>``) and runs it through the shared ``examples._runner``. For the bridge,
``mqtt`` carries the broker settings and ``client_id`` also names the persistent
MQTT session (stable across restarts); ``poll_interval`` is only the idle cadence,
since arrivals wake the loop. Config is injected here, not read from the
environment.
"""
from datetime import timedelta

from flechtwerk import MqttBrokerConfig

from examples._runner import dispatch, run

from .bridge import stage as bridge_stage
from .monitor import stage as monitor_stage

if __name__ == "__main__":
    dispatch({
        "bridge": lambda: run(bridge_stage, application_id="fermentation-bridge", client_id="fermentation-bridge-0",
                              metrics_port=9104, mqtt=MqttBrokerConfig(broker="localhost", port=1883),
                              poll_interval=timedelta(seconds=5)),
        "monitor": lambda: run(monitor_stage, application_id="fermentation-monitor",
                               client_id="fermentation-monitor-0", metrics_port=9103),
    })

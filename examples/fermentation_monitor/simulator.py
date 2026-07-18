"""Hydrometer simulator — stands in for the iSpindel/Tilt hardware.

    uv run poe simulate-fermentation

Publishes iSpindel-style JSON to `ispindel/<batch>` over MQTT (QoS 1), one
reading per batch per step. ``batch-42`` ferments cleanly to completion;
``batch-43`` stalls partway, so the monitor raises a stall alert.
"""
import json
import time

import paho.mqtt.client as mqtt

from .setup import BATCHES

BROKER = "localhost"
PORT = 1883
START_GRAVITY = 1.050
FINAL_GRAVITY = 1.008
DROP_PER_STEP = 0.003
STALL_AT_STEP = 5
STEPS = 15
INTERVAL_SECONDS = 1.0


def gravity(behavior: str, step: int) -> float:
    """A falling gravity curve; the stalled batch plateaus after STALL_AT_STEP."""
    effective = min(step, STALL_AT_STEP) if behavior == "stall" else step
    return round(max(FINAL_GRAVITY, START_GRAVITY - effective * DROP_PER_STEP), 4)


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="fermentation-simulator")
    client.connect(BROKER, PORT)
    client.loop_start()
    try:
        for step in range(STEPS):
            for batch, behavior in BATCHES.items():
                payload = {
                    "name": batch,
                    "gravity": gravity(behavior, step),
                    "temperature": 20.0,
                    "angle": 35.0,
                    "battery": 3.9,
                }
                client.publish(f"ispindel/{batch}", json.dumps(payload), qos=1).wait_for_publish(5)
            print(f"step {step:2d}: " + ", ".join(f"{b}={gravity(bh, step)}" for b, bh in BATCHES.items()))
            time.sleep(INTERVAL_SECONDS)
    finally:
        client.loop_stop()
        client.disconnect()
    print("Simulation complete")


if __name__ == "__main__":
    main()

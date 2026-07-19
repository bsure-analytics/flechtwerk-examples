"""Request a region to track — writes one config record to ``adsb-regions``.

    uv run poe request-region "London"
    uv run poe request-region "Berlin" 150      # optional search radius, nautical miles

The ops step that replaces a hard-coded seed: run it (any number of times) after
``uv run poe adsb`` to point the tracker at the region(s) you care about. The record is
keyed by name on the compacted config topic, so re-requesting the same name updates it,
and ``ingest`` forward-geocodes a name-only region to its centre (see
``ingest.enrich_config``). Any producer works too (Kafbat UI included) — this is just
the convenient one. Reverse-geocoding coverage is independent of what you request here:
the enrich stage maps whichever countries the aircraft are actually over.
"""
import asyncio
import json
import sys

from aiokafka import AIOKafkaProducer

from examples._setup import quiet_fresh_topic_produce_race

from .ingest import CONFIG_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"


async def request_region(name: str, radius: int | None = None) -> None:
    """Write a ``{"name", [radius]}`` config record to ``adsb-regions``, keyed by name."""
    region: dict[str, object] = {"name": name}
    if radius is not None:
        region["radius"] = radius
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            await producer.send_and_wait(CONFIG_TOPIC, key=name.encode(), value=json.dumps(region).encode())
        print(f"Requested region {region}")
    finally:
        await producer.stop()


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        sys.exit('usage: python -m examples.adsb_flight_tracker.request "<region name>" [radius_nm]')
    asyncio.run(request_region(argv[0], int(argv[1]) if len(argv) > 1 else None))


if __name__ == "__main__":
    main()

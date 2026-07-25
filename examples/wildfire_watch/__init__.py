"""Wildfire Watch — NASA FIRMS active-fire detections clustered into persistent fires.

Two host processes over NASA's Fire Information for Resource Management System:

* ``ingest`` — an Extractor. For each watch region on the compacted ``wildfire-regions`` config
  topic (a place name, geocoded once to a bounding box), it polls **both** NOAA VIIRS satellites
  for the rolling day window, dedupes every 375 m hotspot pixel against a bounded, event-time
  pruned **seen-set**, and emits the new detections followed by exactly one ``sweep`` marker to
  ``wildfire-detections`` — one Kafka transaction per poll.
* ``tracker`` — a Transformer. It clusters those point detections into **persistent fire
  objects** held in keyed state, with a lifecycle: *ignition* when a detection founds a fire,
  growth as nearby pixels join, *merge* when one detection bridges two fires, and *extinction*
  when a fire goes unseen for 12 h of event time. It emits continuous per-fire status snapshots
  (``wildfire-status``) and sparse lifecycle events (``wildfire-events``).

Why this example exists: it teaches three shapes the others don't — **spatiotemporal
sessionization** (entities born, grown, and killed by timeout, in space as well as time), the
**third point on the cursor spectrum** (a source with neither a snapshot's statelessness nor a
feed's monotonic resume mark, so the honest cursor is a pruned dedupe set), and **markers as the
transformer's only clock**, extended from SMARD's one-shot settle marker to a heartbeat that
drives an entire lifecycle. It is also the repo's first example that needs an API key, which
flows in through the ops caller so the stage itself stays env-free.

Read-only public science data. **Not for safety-of-life decisions** — see ``README.md``.
"""
from .attributes import DETECTIONS_TOPIC, EVENTS_TOPIC, REGIONS_TOPIC, STATUS_TOPIC
from .tracking import (
    Detection,
    EXTINGUISH_AFTER,
    LINK_KM,
    absorb,
    detection_links,
    expired,
    found_fire,
    link_spans,
    merge_fires,
)

__all__ = [
    "DETECTIONS_TOPIC",
    "Detection",
    "EVENTS_TOPIC",
    "EXTINGUISH_AFTER",
    "LINK_KM",
    "REGIONS_TOPIC",
    "STATUS_TOPIC",
    "absorb",
    "detection_links",
    "expired",
    "found_fire",
    "link_spans",
    "merge_fires",
]

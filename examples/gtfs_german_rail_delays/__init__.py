"""GTFS German rail delays — a live German long-distance rail delay monitor.

A three-stage pipeline over Germany's free open transit data (gtfs.de / DELFI):

* ``loader``  — an Extractor that parses the long-distance **static** GTFS feed
  into one **profile** per trip (schedule + stops with coordinates), published to
  the compacted ``gtfs-trip-profiles`` dimension topic.
* ``ingest``  — an Extractor that polls the national **GTFS-Realtime** protobuf
  feed, decodes it *at the edge* (protobuf → dict), and emits one delay update
  per trip to ``gtfs-trip-updates``.
* ``delays``  — a Transformer that co-partition-joins updates against profiles by
  ``trip_id``, computes each train's current delay and which station it is
  at/approaching (snapped to that station's coordinates — no interpolation), and
  emits a per-trip delay record to ``gtfs-train-delays``.

The output lands in ClickHouse (Kafka-engine queue → materialized views) and
drives a Grafana board: a delay-coloured map of ~130 concurrent ICE/IC trains,
network punctuality (DB's "pünktlich" = under 6 minutes late), and a
network-delay timeseries.

Why *delays* and not *positions*: no free German feed ships route geometry
(``shapes.txt``), and DB blocks the HAFAS/vendo APIs that could supply polylines
(``OPS_BLOCKED``). Delays, on the other hand, are rich and free. See ``README.md``.
"""

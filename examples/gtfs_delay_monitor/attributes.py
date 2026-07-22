"""Typed attributes for the GTFS delay monitor.

Following the framework's rule (and the ADS-B / GDELT precedent): declare an
``Attribute`` only for the fields the stages *compute with*; let the rest of the
GTFS-Realtime message ride through verbatim under its wire name. The incoming
TripUpdate is the uncontrolled upstream — its ``stop_time_update`` list and the
nested ``arrival``/``departure`` blocks are read as plain dicts at the compute
site (``LIST(DICT(ANY))``), never declared per sub-field, exactly as GDELT keeps
its raw ``ROW`` and parses polymorphic sub-values where it uses them.

Timestamps are real ``datetime``s at the typed edge (the ``DATETIME`` codec
renders ISO-8601 on the wire, which ClickHouse ingests directly). The GTFS-RT
feed's ``header.timestamp`` (POSIX epoch) becomes the authoritative **event
time** ``FEED_TS`` — never wall-clock.
"""
from typing import Final

from flechtwerk.attribute import ANY, Attribute, DATETIME, DICT, FLOAT, INT, LIST, RECORD, STR

# --- Config topics (one record per source/feed; the wire key is its name) ---

URL: Final = Attribute("url", STR)
"""The feed URL a config record carries — shared by ``gtfs-rt-feeds`` (the
GTFS-Realtime protobuf endpoint) and ``gtfs-static-sources`` (the static GTFS
zip). Injected by ``setup.py``; any producer may update it live."""

# --- Resume cursors + the event-time stamp ---

FEED_TS: Final = Attribute("feed_ts", DATETIME)
"""GTFS-Realtime ``header.timestamp`` as an aware UTC datetime — the authoritative
event time. Stamped by ``ingest`` onto every update, carried onto every delay
record, and (as an epoch-comparable value) the ``ingest`` resume cursor: a poll
whose snapshot is not newer than the cursor is skipped."""
ETAG: Final = Attribute("etag", STR, optional=True)
"""The RT response ``ETag`` — half of ``ingest``'s cursor, for a cheap 304."""
STATIC_VERSION: Final = Attribute("static_version", STR, optional=True)
"""The static feed's ``ETag``/``Last-Modified`` — ``loader``'s resume cursor and a
field on each profile; an unchanged version makes a loader poll a no-op."""

# --- Incoming TripUpdate: the fields the delay logic reads (rest spread verbatim) ---

TRIP: Final = Attribute("trip", RECORD)
"""The TripUpdate's ``trip`` descriptor (nested), holding ``TRIP_ID`` / ``START_DATE``
/ ``TRIP_REL``."""
TRIP_ID: Final = Attribute("trip_id", STR)
"""The GTFS trip id — the message key on every topic and the join/state key. It is
also the ``gtfs-trip-profiles`` and ``gtfs-train-delays`` identity."""
START_DATE: Final = Attribute("start_date", STR, optional=True)
"""The trip's service day ``YYYYMMDD`` (nested in ``TRIP``) — anchors the schedule's
local times to a calendar day (the noon−12 h rule handles DST)."""
TRIP_REL: Final = Attribute("schedule_relationship", STR, optional=True)
"""``SCHEDULED`` | ``CANCELED`` | … — read at trip level (a CANCELED trip emits
nothing) and, under the same wire name, per stop-time-update (a ``SKIPPED`` stop)."""
STOP_TIME_UPDATE: Final = Attribute("stop_time_update", LIST(DICT(ANY)), optional=True)
"""The per-stop predictions, as raw dicts (``stop_sequence``, ``stop_id``,
``arrival``/``departure`` → ``{delay, time}``, ``schedule_relationship``). Read at
the compute site; ``delay`` is an int32 (number), coerced only where used."""

# --- Trip profile (loader output → gtfs-trip-profiles → delays state) ---

LINE: Final = Attribute("line", STR)
"""The train's line/number from the static ``route_short_name`` (e.g. ``ICE 29``)."""
ROUTE_TYPE: Final = Attribute("route_type", INT)
"""The GTFS route type (``2`` = rail) — the scope filter and a dashboard facet."""
DESTINATION: Final = Attribute("destination", STR, optional=True)
"""The trip's final stop name (``trips.txt`` carries no headsign, so we derive it)."""
STOPS: Final = Attribute("stops", LIST(DICT(ANY)))
"""The ordered schedule, one raw dict per stop with keys ``seq``, ``stop_id``,
``name``, ``lat``, ``lon``, ``arr_s``, ``dep_s`` (GTFS seconds since local midnight,
possibly > 86400). Read at the compute site."""

# Raw per-stop keys inside a STOPS element (read at the compute site, not declared
# as attributes — they live inside the LIST(DICT(ANY)) and never collide with ours).
STOP_SEQ: Final = "seq"
STOP_ID: Final = "stop_id"
STOP_NAME: Final = "name"
STOP_LAT: Final = "lat"
STOP_LON: Final = "lon"
STOP_ARR_S: Final = "arr_s"
STOP_DEP_S: Final = "dep_s"

# --- Delay record (delays output → gtfs-train-delays) ---

DELAY_S: Final = Attribute("delay_s", INT)
"""Current delay in seconds at the train's next stop (negative = early)."""
STATUS: Final = Attribute("status", STR)
"""``early`` | ``on_time`` | ``late`` | ``severe`` — bucketed from ``DELAY_S``
(``on_time`` boundary = DB's < 6 min "pünktlich")."""
NEXT_STOP: Final = Attribute("next_stop", STR, optional=True)
"""Name of the stop the train is at or approaching — where the marker is placed."""
LAT: Final = Attribute("lat", FLOAT, optional=True)
"""Latitude of ``NEXT_STOP`` — the map marker (snapped to the station, not
interpolated between stations)."""
LON: Final = Attribute("lon", FLOAT, optional=True)
"""Longitude of ``NEXT_STOP``."""
STOPS_TOTAL: Final = Attribute("stops_total", INT)
"""Number of scheduled stops on the trip."""
STOPS_DONE: Final = Attribute("stops_done", INT)
"""How many stops the train has already departed — journey progress."""
SKIPPED: Final = Attribute("skipped", INT)
"""Count of ``SKIPPED`` (cancelled) stops on the trip so far."""
TERMINUS_DELAY_S: Final = Attribute("terminus_delay_s", INT, optional=True)
"""Predicted delay at the final stop — the "will it arrive late?" headline."""

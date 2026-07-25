"""Typed attributes, topic names, and state keys for the Wildfire Watch example.

Like SMARD and Odds (and unlike GTFS/GDELT, which spread a foreign upstream schema), the
wire records here are **ours**: the ingest stage reads NASA FIRMS' 14-column CSV at the edge
and *constructs* a normalized detection from scratch, and the tracker constructs the status
and lifecycle-event records. There is no foreign JSON schema to spread through — so,
deliberately, every field a detection, a sweep, a status snapshot, an event, or the tracker's
state carries is a declared ``Attribute``. The framework's "declare only what you compute
with" rule is about not re-declaring a schema you don't own; here we own the whole schema.

**Every clock is upstream-owned.** ``ACQUIRED_AT`` is the satellite's own acquisition instant
(``acq_date`` + ``acq_time``, UTC); ``SWEEP_AT`` / ``FETCHED_AT`` are the server ``Date`` of
the poll that produced the record. No stage ever reads a wall clock, so every code path stays
drivable from the logic tier. Timestamps are aware-UTC ``datetime``s at the typed edge (the
``DATETIME`` codec renders ISO-8601, which ClickHouse ingests directly).

Codecs are exact-type: ``FLOAT`` rejects ``int`` (the CSV parser wraps every number in
``float()`` — FIRMS delivers ``bright_ti4`` as a bare ``367`` often enough to matter) and
``INT`` rejects ``bool``.
"""
from typing import Final

from flechtwerk.attribute import ANY, Attribute, DATETIME, DICT, FLOAT, INT, LIST, STR

# --- Topics (the wire contract, shared by both stages) ---

REGIONS_TOPIC: Final = "wildfire-regions"
"""Compacted config topic, one record per watch region (keyed by the region slug), seeded by
nobody — a user requests each with ``uv run poe request-wildfire`` (or any producer, Kafbat
included). Each record names a place; ``enrich_config`` fills in its bounding box."""
DETECTIONS_TOPIC: Final = "wildfire-detections"
"""Partitioned stream of raw 375 m hotspot pixels **and** the per-poll ``sweep`` markers,
both keyed by the region slug so a region's detections and its clock co-partition onto the one
tracker task that owns its fires."""
STATUS_TOPIC: Final = "wildfire-status"
"""Continuous per-fire status snapshots — one per active fire per sweep. The heartbeat that
drives the map and the FRP timeline; ``extinguished`` is the final snapshot a fire ever gets."""
EVENTS_TOPIC: Final = "wildfire-events"
"""Sparse lifecycle stream: ``ignition``, ``merged``, ``extinguished``. A fire's life story,
kept without a TTL — rare and precious, the same rationale as ``odds-signals``."""

# --- Config record (wildfire-regions; wire key = region slug) ---

REGION: Final = Attribute("region", STR)
"""The region slug (e.g. ``alentejo-portugal``) — the wire key of every record this example
produces, the tracker's state-bucket identity, and the ClickHouse grouping key. Duplicated
into the config value so a stage reads it without decoding the Kafka key (SMARD does the
same)."""
NAME: Final = Attribute("name", STR)
"""The human place name the user asked to watch, e.g. ``"Alentejo, Portugal"`` — the query
Nominatim resolves. Carried through for the dashboard."""
WEST: Final = Attribute("west", FLOAT, optional=True)
"""West edge of the watch bounding box (decimal degrees).

``request.py`` resolves the box at request time and **caches all four edges here**, so a
requested config fully describes its region (which is also what lets the request tool warn about
overlapping regions — it can only intersect boxes it knows). Optional because a *hand-produced*
record (from Kafbat, say) may carry only a name: ``ingest.enrich_config`` then fills the four
edges from Nominatim's ``boundingbox``, padded by ``ingest.PAD_DEG``, exactly once per config
arrival. Either way a poll always has a box — the difference is only who resolved it, and when.
Cached edges are a snapshot: re-request a region to pick up a boundary or ``PAD_DEG`` change."""
SOUTH: Final = Attribute("south", FLOAT, optional=True)
"""South edge of the watch bounding box — see :data:`WEST`."""
EAST: Final = Attribute("east", FLOAT, optional=True)
"""East edge of the watch bounding box — see :data:`WEST`."""
NORTH: Final = Attribute("north", FLOAT, optional=True)
"""North edge of the watch bounding box — see :data:`WEST`."""

# --- Detection + sweep records (wildfire-detections; key = region slug) ---

KIND: Final = Attribute("kind", STR)
"""The record's self-description, so one topic can carry more than one shape and the
materialized views stay explicit (the SMARD idiom). ``detection`` | ``sweep`` on
``wildfire-detections``; ``ignition`` | ``merged`` | ``extinguished`` on ``wildfire-events``."""
DETECTION_ID: Final = Attribute("detection_id", STR)
"""A 12-hex digest of the **raw CSV strings** ``latitude,longitude,acq_date,acq_time,
satellite`` — see ``ingest.detection_id``. FIRMS ships **no unique row id**, so identity is
derived; hashing the raw strings (never reparsed floats) keeps it immune to float-formatting
drift, which is what makes the dedupe seen-set trustworthy across restarts."""
LAT: Final = Attribute("lat", FLOAT)
"""Detection latitude — the **centre of a 375 m pixel** that contained fire, not a point
source (see the README's caveats)."""
LON: Final = Attribute("lon", FLOAT)
"""Detection longitude — see :data:`LAT`. Reused on status/event records as the fire
cluster's centroid."""
ACQUIRED_AT: Final = Attribute("acquired_at", DATETIME)
"""The satellite's acquisition instant (UTC), from ``acq_date`` + ``acq_time``. **The event
time** — the tracker folds it into a fire's first/last-seen bounds and never consults a wall
clock. FIRMS' ``acq_time`` is an unpadded integer HHMM (``230`` = 02:30), which the parser
handles."""
SATELLITE: Final = Attribute("satellite", STR)
"""Which bird saw it: ``N20`` (NOAA-20) or ``N21`` (NOAA-21). Part of the detection identity —
the two satellites see the same fire on different passes, and both sightings are real data."""
INSTRUMENT: Final = Attribute("instrument", STR)
"""The sensor, ``VIIRS`` for both configured sources. Carried through, not computed with."""
CONFIDENCE: Final = Attribute("confidence", STR)
"""VIIRS confidence as the **letter** FIRMS ships — ``l`` | ``n`` | ``h`` (low/nominal/high).
Deliberately stored as a string and never decoded to a number: MODIS uses 0–100 for the same
column, so a numeric reading would be wrong the moment a MODIS source is added."""
FRP: Final = Attribute("frp", FLOAT, optional=True)
"""Fire radiative power in megawatts — how *hot* the pixel is, the dashboard's colour and the
fire's headline size metric. Optional defensively: FIRMS populated it on every one of the
3 600+ rows captured while writing this example, but an **empty** field must land as absent
rather than as a fabricated 0.0 — and note that a genuine ``0.0`` does occur, so absence is
tested by the empty string, never by falsiness."""
BRIGHT_TI4: Final = Attribute("bright_ti4", FLOAT, optional=True)
"""VIIRS I-4 channel brightness temperature (K) — the mid-infrared band the detection fires
on; saturates at 367 K over intense fires. Optional on the same empty-field grounds as
:data:`FRP`."""
BRIGHT_TI5: Final = Attribute("bright_ti5", FLOAT, optional=True)
"""VIIRS I-5 channel brightness temperature (K) — the thermal band used to reject false
alarms. Optional on the same grounds as :data:`FRP`."""
SCAN: Final = Attribute("scan", FLOAT)
"""Pixel footprint along-scan, in km — the pixel grows toward the swath edge, so this and
:data:`TRACK` say how *coarse* this particular detection's position is."""
TRACK: Final = Attribute("track", FLOAT)
"""Pixel footprint along-track, in km — see :data:`SCAN`."""
DAYNIGHT: Final = Attribute("daynight", STR)
"""``D`` or ``N`` — whether the pass was on the day or night side. Night passes detect smaller
fires (no solar background), so this is real signal about detection sensitivity, not decor."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""The server ``Date`` of the poll that delivered this detection (aware UTC). Distinct from
:data:`ACQUIRED_AT`: the gap between them **is** the NRT pipeline latency (up to ~3 h), which
is exactly why re-polling the same day window keeps yielding new rows for old times."""

SWEEP_AT: Final = Attribute("sweep_at", DATETIME)
"""Event time of a ``sweep`` marker — the poll's ``fetched_at``. **The tracker's only clock.**
The framework has no timers, so the poller emits one sweep per region per poll *even when it
found nothing new*, and the tracker hangs its whole lifecycle (status heartbeats and extinction
timeouts) off it. No input → no time → no extinction; the trade is explicit and documented."""
NEW_DETECTIONS: Final = Attribute("new_detections", INT)
"""How many detections this poll found that the seen-set had never seen. Rides on the sweep so
the dashboard can show poll productivity, and so a quiet sweep is visibly a *quiet* sweep
rather than a missing one."""

# --- Status record (wildfire-status; key = region slug) ---
# (REGION, LAT, LON reused — the fire's centroid; FRP_MAX/FIRST_SEEN/LAST_SEEN reused on events.)

FIRE_ID: Final = Attribute("fire_id", STR)
"""The persistent fire identity: ``F-`` + the ``detection_id`` of the detection that founded
it. Derived from the founding detection rather than a counter or a random id, so a replay of
the same input stream reconstructs **the same** fire ids — determinism the EOS story needs."""
STATUS: Final = Attribute("status", STR)
"""``active`` while the fire is still being detected, ``extinguished`` on its final snapshot.
Flipping to ``extinguished`` is what drops it off the live map (``wildfire_active`` is read
``FINAL WHERE status = 'active'``)."""
DETECTIONS: Final = Attribute("detections", INT)
"""How many hotspot pixels have been absorbed into this fire so far — its rough size, and the
geomap's marker-size field. Counts *pixel sightings*, not distinct ground area: the same fire
seen by both satellites on two passes contributes twice, by design."""
FRP_SUM: Final = Attribute("frp_sum", FLOAT, optional=True)
"""Sum of every absorbed detection's FRP (MW) — the fire's cumulative radiative output as
observed. Absent until at least one absorbed detection carried an FRP (never a fabricated 0)."""
FRP_MAX: Final = Attribute("frp_max", FLOAT, optional=True)
"""The single hottest pixel ever absorbed (MW) — the intensity measure the dashboard colours
by, robust to a fire being seen a different number of times. Absent like :data:`FRP_SUM`."""
FIRST_SEEN: Final = Attribute("first_seen", DATETIME)
"""Earliest ``ACQUIRED_AT`` absorbed — the fire's observed ignition time. A ``min`` fold, so a
*late-arriving* earlier detection correctly moves it back rather than being ignored."""
LAST_SEEN: Final = Attribute("last_seen", DATETIME)
"""Latest ``ACQUIRED_AT`` absorbed — the fire's liveness. A ``max`` fold, so an out-of-order
older detection can never make a live fire look stale. This is what extinction is measured
against."""
AS_OF: Final = Attribute("as_of", DATETIME)
"""The ``SWEEP_AT`` of the sweep that produced this snapshot — the status stream's event time
and the ``ReplacingMergeTree`` version on ``wildfire_active``. Status is **sweep-paced**, not
detection-paced, so this advances on every poll even for a fire nothing new was seen of."""

# --- Event record (wildfire-events; key = region slug) ---
# (KIND, FIRE_ID, REGION, LAT, LON, DETECTIONS, FRP_MAX, FIRST_SEEN, LAST_SEEN reused.)

OCCURRED_AT: Final = Attribute("occurred_at", DATETIME)
"""When the lifecycle transition happened in **event time**: the triggering detection's
``ACQUIRED_AT`` for ``ignition``/``merged``, the sweep's ``SWEEP_AT`` for ``extinguished``.
Never a wall clock, so the event log replays identically."""
MERGED_INTO: Final = Attribute("merged_into", STR, optional=True)
"""On a ``merged`` event: the ``FIRE_ID`` of the survivor this fire was folded into. Absent on
every other kind. Two fires merge when a new detection bridges them — one blaze that the
satellites first caught as two separate hotspot groups."""

# --- Extractor state: the bounded, event-time-pruned dedupe seen-set ---

SEEN: Final = Attribute("seen", DICT(LIST(STR)))
"""``{acq_date: [detection_id, …]}`` — the ingest stage's whole cursor.

FIRMS is the **third point on the cursor spectrum**: not a stateless snapshot (ADS-B, Odds)
and not a monotonic feed with a resume mark (GDELT, SMARD). The area API returns a rolling
day-window snapshot into which late detections keep arriving, with no row id and nothing
monotonic to resume from — so "what's new" can only be answered by remembering what was
already emitted. Bucketing by ``acq_date`` makes pruning trivial: once a date falls out of the
``DAY_RANGE`` window it can never reappear, so the whole bucket drops. Hard-capped as well
(``ingest.SEEN_HARD_CAP``) because each State is ONE changelog record under the broker's ~1 MB
ceiling — the GDELT lesson, here as a first-class teaching point."""

# --- Transformer state: the persistent fire objects ---

FIRES: Final = Attribute("fires", DICT(DICT(ANY)))
"""The region's live fires: ``fire_id → entry``, the session-window state. Each entry holds
only what the clustering fold needs (see the ``F_*`` keys below). A fire leaves the dict when a
sweep declares it extinguished; when the last one goes, the region's whole bucket is
tombstoned and rebuilt by the next ignition — so the store stays bounded to *burning* regions
rather than to every region ever watched."""

# Raw keys inside a FIRES entry. Read at the compute site rather than declared as attributes —
# they live inside the DICT(DICT(ANY)) and can never collide with ours (the odds L_* idiom).
# `tracking.py` folds these as real `datetime` objects; `tracker.py` encodes them to ISO
# strings with the DATETIME codec at the state boundary, because a State nests only JSON
# scalars. FRP keys are ABSENT until a detection carrying FRP is absorbed.
F_LAT: Final = "lat"
"""Cluster centroid latitude — a detection-count-weighted running mean."""
F_LON: Final = "lon"
"""Cluster centroid longitude — a detection-count-weighted running mean."""
F_MIN_LAT: Final = "min_lat"
"""South edge of the fire's detection bounding box (the box link-distance expands)."""
F_MAX_LAT: Final = "max_lat"
"""North edge of the fire's detection bounding box."""
F_MIN_LON: Final = "min_lon"
"""West edge of the fire's detection bounding box."""
F_MAX_LON: Final = "max_lon"
"""East edge of the fire's detection bounding box."""
F_FIRST_SEEN: Final = "first_seen"
"""Earliest absorbed ``ACQUIRED_AT`` (a ``min`` fold) — also the merge tie-breaker."""
F_LAST_SEEN: Final = "last_seen"
"""Latest absorbed ``ACQUIRED_AT`` (a ``max`` fold) — what extinction is measured against."""
F_COUNT: Final = "count"
"""Absorbed detection count — the centroid's weight and the fire's size proxy."""
F_FRP_SUM: Final = "frp_sum"
"""Running sum of absorbed FRP (MW); absent until one arrives."""
F_FRP_MAX: Final = "frp_max"
"""Running max of absorbed FRP (MW); absent until one arrives."""
F_SATELLITES: Final = "satellites"
"""Sorted list of the satellites that have seen this fire — evidence of cross-satellite
confirmation, and the reason a two-bird example is worth the second GET."""

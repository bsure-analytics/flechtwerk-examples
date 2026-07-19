"""Typed attributes for the ADS-B flight-tracker pipeline.

The guiding rule (see the framework's [typed attributes](https://bsure-analytics.github.io/flechtwerk/concepts/typed-attributes/)):
**declare an ``Attribute`` only for data a stage actually computes with** — reads
to decide, or writes as a derived value. Every *other* field the adsb.lol feed
sends (``r``, ``track``, ``seen``, ``mlat``, ``rssi``, … and anything adsb.lol
adds next month) is carried through untouched by *spreading* the raw record (see
``enrich.py``), so it needs no attribute. The ClickHouse sink reads each message
whole into a ``JSON`` column, so it lands natively — nested feed structure included,
no flattening. That keeps this list short and the pipeline robust to upstream schema
changes: a new source field flows straight to ClickHouse instead of being dropped
or breaking a hand-declared schema.

Consequences worth knowing:

- Feed fields keep their **wire names** end to end (``flight``, ``alt_baro``,
  ``gs``, ``t``, ``r``); the Grafana dashboards alias them at query time
  (``trimBoth(flight) AS callsign``). No renaming in the pipeline.
- Codecs are exact-typed, so the *uncontrolled* feed is read through ``ANY``
  handles; the one field whose wire *type* is polymorphic (``alt_baro`` is a
  number **or** the string ``"ground"``) is carried through **faithfully** — the
  raw value reaches ClickHouse untouched (a ``Dynamic`` column keeps ``"ground"``
  verbatim, so a parked aircraft stays distinguishable from one at sea level) — and
  coerced to feet only where a stage actually computes with it, via
  :func:`altitude_ft`, which reads ``"ground"`` as ``None`` (no numeric altitude),
  never a fabricated ``0``.
"""
from datetime import timedelta
from typing import Final

from flechtwerk.attribute import ANY, Attribute, Codec, DATETIME, DICT, FLOAT, INT, LIST, RECORD, SET, STR

# A custom codec — a small showcase of the Attributes feature's extensibility: a
# duration is fractional seconds on the wire (a JSON number; ``total_seconds()``
# keeps sub-millisecond precision), a ``timedelta`` in code.
DURATION: Final = Codec(
    decode=lambda seconds: timedelta(seconds=seconds),
    encode=lambda duration: duration.total_seconds(),
)

# --- Config: one record per region to poll (wire key = region name) ---

NAME: Final = Attribute("name", STR)
"""Region label; the config wire key, the ``adsb-raw`` message key, and the enrich
stage's per-region state key."""
LAT: Final = Attribute("lat", FLOAT)
"""Region-centre latitude in the config (aircraft positions keep the wire ``lat``)."""
LON: Final = Attribute("lon", FLOAT)
"""Region-centre longitude in the config (aircraft positions keep the wire ``lon``)."""
RADIUS: Final = Attribute("radius", INT, optional=True)
"""Search radius in nautical miles; a config may omit it — ``enrich_config``
supplies the default and clamps it to adsb.lol's maximum (see ``ingest.py``)."""

ISO3: Final = Attribute("iso3", STR)
"""ISO 3166-1 alpha-3 country code. The value on an ``adsb-countries`` record (the
enrich stage's country-load request → the boundary loader's poll target), and the
world map's ``dictGet`` result identifying the country an aircraft is over."""

# --- Raw record: one poll on adsb-raw (ingest output → enrich input) ---
#
# Three nested Records so the *uncontrolled* feed schema keeps its own namespace
# and can never collide with our fields (see ``ingest.wrap_response``).

RESPONSE: Final = Attribute("response", RECORD)
"""The whole adsb.lol response body, wrapped verbatim — read ``[AC]``/``[NOW]`` off it."""
CONFIG: Final = Attribute("config", RECORD)
"""The poll's region config, nested — reuses the config's ``NAME``/``LAT``/… handles."""
METADATA: Final = Attribute("metadata", RECORD)
"""Poll provenance, nested: ``FETCHED_AT`` and ``FETCH_DURATION``."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""Wall-clock time ingest received the response (in ``METADATA``) — feed-latency provenance."""
FETCH_DURATION: Final = Attribute("fetch_duration", DURATION)
"""adsb.lol request duration, a ``timedelta`` via the custom ``DURATION`` codec
(seconds on the wire), in ``METADATA`` — poll-health provenance."""
AC: Final = Attribute("ac", LIST(RECORD))
"""The aircraft array on the response — a list of nested Records; ``[]`` when empty."""
NOW: Final = Attribute("now", ANY)
"""Feed timestamp in epoch milliseconds; becomes each event's ``polled_at``.
``ANY`` — JSON may deliver it int or float — coerced where it is read."""

# --- adsb.lol per-aircraft fields we READ (wire names, uncontrolled) ---
#
# Only the fields a stage computes with get a handle; every other feed field
# passes through via spreading, under its wire name, with no attribute.
# Numeric handles are ``ANY`` because JSON does not distinguish 420 from 420.0.

HEX: Final = Attribute("hex", STR)
"""ICAO 24-bit address — the aircraft identity and the ``adsb-aircraft`` message key."""
SRC_CALLSIGN: Final = Attribute("flight", STR, optional=True)
"""Callsign — read (stripped) to derive the airline code; passes through as ``flight``."""
SRC_TYPE: Final = Attribute("t", STR, optional=True)
"""ICAO type designator, e.g. ``"A320"`` — the enrichment key; passes through as ``t``."""
SRC_ALTITUDE: Final = Attribute("alt_baro", ANY, optional=True)
"""Barometric altitude: feet as a number, or the string ``"ground"``. ``ANY`` so the
polymorphic wire value passes through *faithfully* (a spotter can tell a parked
aircraft from one at sea level); coerce to feet with :func:`altitude_ft` at the
points that do arithmetic (vertical rate, the conflict check)."""


def altitude_ft(value: object) -> int | None:
    """Interpret the polymorphic ``alt_baro`` wire value as numeric feet, or ``None``.

    A number is feet. The string ``"ground"`` has **no** numeric altitude — adsb.lol
    replaced the figure with the word, and "ground" means "on the surface" at whatever
    field elevation, **not** 0 ft MSL — so it reads as ``None``, never a fabricated
    ``0``. ``None``/absent likewise stays ``None`` (unknown). The faithful wire value
    is preserved upstream (see ``SRC_ALTITUDE``); this is only for the arithmetic that
    needs feet. The ClickHouse sink does the same in SQL with ``toInt32OrNull``.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None  # "ground", absent, or anything non-numeric → no numeric altitude
    return int(value)
SRC_LAT: Final = Attribute("lat", ANY, optional=True)
"""Aircraft latitude — read (coerced) for the grid cell, geocode, and conflict check."""
SRC_LON: Final = Attribute("lon", ANY, optional=True)
"""Aircraft longitude — read (coerced) for the grid cell, geocode, and conflict check."""
SRC_GROUND_SPEED: Final = Attribute("gs", ANY, optional=True)
"""Ground speed in knots — passed through by enrich; read by the ClickHouse sink (example 2)."""
SQUAWK: Final = Attribute("squawk", STR, optional=True)
"""Mode-A transponder code, e.g. ``"7700"`` — read to flag emergencies; passes through."""

# --- Derived output: our own fields, added to every aircraft event ---

REQUESTED_REGION: Final = Attribute("requested_region", STR)
"""The poll target a message came from — the region *searched* (the config ``NAME``,
e.g. "London"), not where the aircraft actually is. The aircraft's real location is the
reverse-geocoded ``NEAREST_PLACE`` (+ ``OVER_COUNTRY``); the ``requested_region`` name
keeps the two distinct in ClickHouse / Kafka / Grafana."""
POLLED_AT: Final = Attribute("polled_at", DATETIME)
"""Feed timestamp of the poll that produced this event."""
IS_DELETED: Final = Attribute("is_deleted", INT)
"""``0`` for a live position, ``1`` for a departure tombstone (see the README)."""
EMERGENCY: Final = Attribute("emergency", INT)
"""``1`` if the current squawk is an emergency code, else ``0``."""
VERTICAL_RATE: Final = Attribute("vertical_rate", FLOAT, optional=True)
"""Feet per minute, derived from consecutive altitudes — needs the per-aircraft
prior in ``State`` (something adsb.lol's per-sample view cannot show)."""

# Live-cached enrichment — looked up once per entity, reused from State forever.
AIRLINE: Final = Attribute("airline", STR, optional=True)
"""Operator name resolved from the callsign's ICAO airline designator."""
AIRLINE_WIKI: Final = Attribute("airline_wiki", STR, optional=True)
"""Wikipedia URL for the airline (resolved from Wikidata)."""
AIRCRAFT_TYPE_NAME: Final = Attribute("aircraft_type_name", STR, optional=True)
"""Human aircraft model resolved from the ICAO type designator."""
TYPE_WIKI: Final = Attribute("type_wiki", STR, optional=True)
"""Wikipedia URL for the aircraft type (resolved from Wikidata)."""
OVER_COUNTRY: Final = Attribute("over_country", STR, optional=True)
"""ISO-3 country the aircraft is currently over, reverse-geocoded from its position via
the ClickHouse polygon dictionary (the boundary map's ``country`` attribute)."""
NEAREST_PLACE: Final = Attribute("nearest_place", STR, optional=True)
"""The admin area the aircraft is currently over — the boundary map's containing polygon
(geoBoundaries level, default ADM3, e.g. a municipality/town). This is the
aircraft's actual region, as opposed to ``REQUESTED_REGION`` (the poll target)."""

# --- Events: derived aviation events on adsb-events (enrich + conflict output) ---

AT: Final = Attribute("at", DATETIME)
"""When the event was observed (the poll's feed timestamp)."""
EVENT_TYPE: Final = Attribute("event_type", STR)
"""``emergency`` | ``rapid_descent`` | ``going_dark`` | ``conflict``."""
DETAIL: Final = Attribute("detail", STR)
"""Human-readable description, e.g. ``"squawk 7700 (general emergency)"``."""

# --- Cells: positions re-keyed by grid cell on adsb-cells (enrich → conflict) ---

CELL: Final = Attribute("cell", STR)
"""Grid-cell key an aircraft position falls in — the ``adsb-cells`` message key."""

# --- Enrich state: per region (wire key = region name) ---

TRACKED: Final = Attribute("tracked", DICT(RECORD))
"""Per-region roster with priors: ICAO → last ``{alt_baro, squawk, lat, lon,
vertical_rate, polled_at}``. Its keys are the roster (diff them to detect
departures); its values carry the priors that make vertical-rate, squawk-onset,
and going-dark derivable — the payoff of keeping state the raw feed does not."""
AIRLINE_CACHE: Final = Attribute("airline_cache", DICT(RECORD))
"""ICAO airline designator → cached ``{airline, airline_wiki}``. A present key (even
an empty Record — a resolved 'unknown') means 'already looked up', so the live
lookup runs only on a miss. Survives restart via the changelog: the whole point."""
TYPE_CACHE: Final = Attribute("type_cache", DICT(RECORD))
"""ICAO type designator → cached ``{aircraft_type_name, type_wiki}``. (Reverse geocoding
is not cached — it is a per-poll ClickHouse batch from each aircraft's exact position;
see ``AdsbEnrich._geocode``.)"""

# --- Conflict state: per grid cell (wire key = cell) ---

POSITIONS: Final = Attribute("positions", DICT(RECORD))
"""Per-cell recent positions: ICAO → ``{lat, lon, alt_baro, polled_at}``. The
self-join checks pairwise separation across this map."""
ACTIVE_PAIRS: Final = Attribute("active_pairs", SET(STR))
"""Conflict pairs currently in violation (``"hexA|hexB"``), so a sustained
near-miss emits one event at onset, not one per poll."""

# --- Boundary loader state: per country (wire key = ISO-3) ---

CHECKED_AT: Final = Attribute("checked_at", DATETIME, optional=True)
"""When the loader last confirmed this country's boundaries were present and fresh — the
check-cadence timer, so most polls are a cheap no-op (see ``boundaries.py``)."""

# --- Enrich state: countries already requested (so each is emitted to adsb-countries once) ---

COUNTRIES_REQUESTED: Final = Attribute("countries_requested", SET(STR))
"""ISO-3 codes this region's enrich has already written to ``adsb-countries`` — so a
country with ongoing traffic is requested once, not every poll (see ``enrich.py``)."""

# --- Third-party response boundaries (Wikidata SPARQL, geoBoundaries) ---
#
# Unlike everything above, these describe *third-party* JSON bodies, not the
# pipeline's own topics/state — the shapes the enricher and the boundary loader
# parse. They live here so all of the example's typed handles sit in one file, but
# they never ride a topic: they exist only so external responses are read through
# the same typed-attribute discipline (no bare dict indexing past ``Record.wrap``)
# as every pipeline edge. The *forward* geocoder's ``/search`` hit (``ingest`` →
# ``geocoding``, resolving a name-only region) reads its ``lat`` / ``lon`` through the
# existing ``SRC_LAT`` / ``SRC_LON`` handles — same wire keys, same ``ANY`` codec
# (Nominatim delivers them as strings), coerced to the config's centre. Reverse
# geocoding parses no upstream body: it is a ClickHouse ``dictGet`` whose columns are
# aliased to ``over_country`` / ``iso3`` / ``nearest_place`` (read through the existing
# handles), so no reverse-response attributes are needed.

RESULTS: Final = Attribute("results", RECORD)
"""SPARQL results envelope on a Wikidata response — holds the ``bindings`` rows."""
BINDINGS: Final = Attribute("bindings", LIST(RECORD))
"""The SPARQL result rows; ``[]`` when the query matched nothing."""
ITEM_LABEL: Final = Attribute("itemLabel", RECORD)
"""A row's ``?itemLabel`` binding, a ``{"value": ...}`` cell — read ``[VALUE]`` off it."""
ARTICLE: Final = Attribute("article", RECORD)
"""A row's ``?article`` binding (the Wikipedia URL cell), same ``{"value": ...}`` shape."""
VALUE: Final = Attribute("value", STR)
"""The ``value`` string inside a SPARQL binding cell."""

GJ_DOWNLOAD_URL: Final = Attribute("gjDownloadURL", STR)
"""Full-resolution GeoJSON download URL in a geoBoundaries API metadata response."""
SIMPLIFIED_GEOJSON_URL: Final = Attribute("simplifiedGeometryGeoJSON", STR, optional=True)
"""Simplified (smaller) GeoJSON URL in a geoBoundaries API metadata response — the
loader prefers it when present (faster download; coarse boundaries are the point)."""

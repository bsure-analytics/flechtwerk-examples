"""ADS-B enrich — a stateful ``Transformer`` that turns raw responses into events.

Stage 2 of the pipeline. It consumes ``adsb-raw`` (one wrapped response per region,
region-keyed), unrolls the ``ac[]`` array, and produces three streams:

- ``adsb-aircraft`` — one event per aircraft, re-keyed to ICAO hex: the **whole raw
  record spread through** (every feed field, verbatim, under its wire name — so a
  new adsb.lol field appears downstream with no code change) plus our own derived
  fields (``vertical_rate``, ``emergency``, ``requested_region``/``polled_at``/
  ``is_deleted``) and live-cached enrichment (airline / aircraft-type / geocode, with
  Wikipedia links). Departures become tombstones (``is_deleted=1``).
- ``adsb-events`` — derived aviation events (``emergency`` squawk onset,
  ``rapid_descent`` onset, ``going_dark``) — the "stop viewing, start deriving"
  payoff a raw-feed viewer can't show.
- ``adsb-cells`` — positioned aircraft re-keyed by grid cell, so the conflict
  self-join (``conflict.py``) sees every aircraft in a cell on one partition.

Only the fields a stage computes with have an attribute; everything else rides
through by spreading the raw record (see ``attributes.py``). Feed fields keep their
wire names; the ClickHouse sink reads each message whole into a ``JSON`` column, so
nested feed structure is preserved natively (no flattening needed) and the dashboards
alias what they read at query time.

**The state store is a live enrichment cache.** Airline + aircraft-type names (with
Wikipedia links) resolve from Wikidata — each looked up *once* and cached in the
region's ``State`` (``AIRLINE_CACHE`` / ``TYPE_CACHE``). Because that state is
changelog-backed, the cache **survives a restart**: a re-launched enrich stage
re-issues zero lookups for entities it has already resolved. This is the headline
showcase — look up once, remember forever, restore from the log. (Types are
best-effort — Wikidata has no ICAO type-designator property, so most don't resolve;
the raw designator ``t`` is always present.) Positions reverse-geocode against a local
ClickHouse polygon dictionary — the boundary "map" the loader provisions (see
``boundaries.py``) — every poll from each aircraft's *exact* position; that is cheap and
more accurate than a cached grid cell, so it is deliberately **not** cached.

**Enrichment is best-effort — a deliberate exception to "let it crash".** A
Wikidata timeout/5xx or a ClickHouse hiccup is swallowed: the aircraft is emitted
un-enriched and the miss is *not* cached (so it retries next poll). Enrichment is
a decoration; a flaky lookup must never stall live telemetry. The
position/roster/event logic keeps the framework's strict let-it-crash behaviour —
only the decorative lookups are softened. The guards apply where the cost is: the
**remote Wikidata** lookups run *inside the consumer's poll loop* before any message is
emitted, so they sit behind a **circuit breaker** (fast-fail while a 429 storm is open)
*and* a per-poll cap (``LIVE_LOOKUPS_PER_POLL``) — an unbounded backlog of slow misses
would block the loop past ``max.poll.interval.ms`` and get the stage evicted. The
**reverse geocode** needs neither: it is a single *local* ClickHouse ``dictGet`` batch
(all uncached cells in one round-trip, one timeout), so it is fast, unbounded, and fills
the map in a poll rather than trickling at the Wikidata rate.

The interesting logic — spread+enrich projection, the roster diff, vertical-rate,
squawk-onset, going-dark, and the cell fan-out — lives in the pure async generator
:func:`project_page`, which touches no framework machinery, no I/O, and no live
enricher (it reads an already-filled cache). The :class:`AdsbEnrich` stage is a
thin shell that fills the cache (the only I/O) and delegates. That split is what
makes the pure-logic test tier possible.
"""
import json
import logging
import math
import time
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

import httpx
from flechtwerk import Event, IncomingMessage, Message, State, Transformer
from flechtwerk.attribute import Attribute, Record

from .attributes import (
    AC,
    AIRCRAFT_TYPE_NAME,
    AIRLINE,
    AIRLINE_CACHE,
    AIRLINE_WIKI,
    altitude_ft,
    ARTICLE,
    AT,
    BINDINGS,
    CELL,
    CONFIG,
    COUNTRIES_REQUESTED,
    DETAIL,
    EMERGENCY,
    EVENT_TYPE,
    HEX,
    IS_DELETED,
    ISO3,
    ITEM_LABEL,
    NAME,
    NEAREST_PLACE,
    NOW,
    OVER_COUNTRY,
    POLLED_AT,
    REQUESTED_REGION,
    RESPONSE,
    RESULTS,
    SQUAWK,
    SRC_ALTITUDE,
    SRC_CALLSIGN,
    SRC_LAT,
    SRC_LON,
    SRC_TYPE,
    TRACKED,
    TYPE_CACHE,
    TYPE_WIKI,
    VALUE,
    VERTICAL_RATE,
)
from .boundaries import ADMIN_LEVELS, COUNTRIES_TOPIC, WORLD_DICT, region_dict
from .geocoding import USER_AGENT
from .ingest import RAW_TOPIC

log = logging.getLogger(__name__)

AIRCRAFT_TOPIC = "adsb-aircraft"
EVENTS_TOPIC = "adsb-events"
CELLS_TOPIC = "adsb-cells"
"""The three enrich outputs. ``CELLS_TOPIC`` re-keys positions by grid cell so the
conflict stage (``conflict.py``) can self-join within a partition."""

CELL_SIZE_DEG = 0.25
"""Grid-cell edge in degrees (~15 nm of latitude). Larger than the conflict
detection radius so most in-cell pairs are caught without a neighbour halo."""

EMERGENCY_SQUAWKS = {"7500": "hijack", "7600": "radio failure", "7700": "general emergency"}
"""Mode-A codes that mean trouble — the reason plane-spotters watch a raw feed."""
RAPID_DESCENT_FPM = -3000.0
"""At or below this vertical rate (feet/min) an aircraft is descending sharply —
possible emergency descent. Fired once, at onset."""
GOING_DARK_ALT_FT = 5000
"""A departed aircraft last seen above this was airborne, not landing — so its
disappearance is 'lost contact', not a routine touchdown."""
LIVE_LOOKUPS_PER_POLL = 12
"""Cap on live *Wikidata* lookups (airline + type) attempted per poll. Each is a slow
remote call inside the consumer's poll loop *before* any message is emitted, so an
unbounded backlog of cache misses would block the loop past ``max.poll.interval.ms``,
get the stage evicted, and silently stall it. Bounding the work per poll keeps the loop
responsive; excess misses just retry next poll, and the circuit breaker fast-fails a
sustained outage — so under normal operation this budget is barely touched (a warm cache
issues almost no lookups). Reverse geocoding is *not* under this cap: it is a single local
ClickHouse batch (see ``_fill_caches``), fast enough to do every uncached cell per poll."""
ENRICHER_COOLDOWN = timedelta(seconds=60)
"""How long the enricher stops calling an upstream after it rate-limits (429) or its
transport fails — long enough to stop hammering a hurting service, short enough that
enrichment recovers on its own once the service does."""


def cell_key(lat: float, lon: float) -> str:
    """Map a position to its grid-cell key — the ``adsb-cells`` partition key."""
    return f"{math.floor(lat / CELL_SIZE_DEG)}:{math.floor(lon / CELL_SIZE_DEG)}"


def _airline_code(callsign: str) -> str | None:
    """The 3-letter ICAO airline designator from an airline-style callsign.

    An airline callsign is exactly three letters (the ICAO designator) immediately
    followed by a flight number that STARTS with a digit — ``BAW123``, ``EZY51JC``.
    Anything else has no operator to resolve and returns ``None``: general aviation
    flies under its registration (``N12345``), and a string with four-plus leading
    letters (``ABCD56``) is not "designator + flight number" — the digit-after-the-code
    check rejects both, where a looser "any digit somewhere" test would wrongly read
    ``ABCD56`` as ``ABC``.
    """
    if len(callsign) < 4:
        return None
    code = callsign[:3]
    return code.upper() if code.isalpha() and callsign[3].isdigit() else None


def _vertical_rate(altitude: int | None, prior: Record | None, polled_at: datetime) -> float | None:
    """Feet per minute between the prior altitude sample and this one.

    ``None`` unless we have both a current and a prior altitude with a positive
    time delta — the derivative the raw per-sample feed never carries. The prior
    carries ``alt_baro`` faithfully, so coerce it to feet with ``altitude_ft``.
    """
    if altitude is None or prior is None:
        return None
    prior_altitude, prior_at = altitude_ft(prior.get(SRC_ALTITUDE)), prior.get(POLLED_AT)
    if prior_altitude is None or prior_at is None:
        return None
    minutes = (polled_at - prior_at).total_seconds() / 60.0
    return (altitude - prior_altitude) / minutes if minutes > 0 else None


def _project_one(
    aircraft: Record,
    region: str,
    polled_at: datetime,
    prior: Record | None,
    airline_cache: dict[str, Record],
    type_cache: dict[str, Record],
    geo: Record | None,
) -> tuple[Event, Record]:
    """Spread one raw aircraft through, overlay our derived fields + enrichment.

    Every field the feed sent rides through untouched under its wire name — including
    ``alt_baro`` with its polymorphic value (a number or ``"ground"``), kept faithful;
    a new adsb.lol field appears downstream automatically. We only *add* our own fields
    (``requested_region``/``polled_at``/``is_deleted``/``emergency``/``vertical_rate`` +
    cached enrichment). Nested feed structure is left as-is — the ClickHouse ``JSON``
    sink stores it natively. The returned prior carries the faithful ``alt_baro`` too —
    ``altitude_ft`` coerces it to feet where the next poll's vertical rate and the
    conflict check need a number.
    """
    altitude = altitude_ft(aircraft.get(SRC_ALTITUDE))
    lat, lon = aircraft.get(SRC_LAT), aircraft.get(SRC_LON)
    lat = float(lat) if lat is not None else None
    lon = float(lon) if lon is not None else None
    squawk = aircraft.get(SQUAWK)
    vertical_rate = _vertical_rate(altitude, prior, polled_at)

    fields = Record.wrap(dict(aircraft.raw))  # spread the whole feed record, verbatim
    fields[REQUESTED_REGION] = region
    fields[POLLED_AT] = polled_at
    fields[IS_DELETED] = 0
    fields[EMERGENCY] = 1 if squawk in EMERGENCY_SQUAWKS else 0
    # alt_baro is NOT touched — the spread already carries its faithful wire value
    # ("ground" or a number) straight through to the sink.
    if vertical_rate is not None:
        fields[VERTICAL_RATE] = vertical_rate
    if (code := _airline_code((aircraft.get(SRC_CALLSIGN) or "").strip())) and (cached := airline_cache.get(code)):
        fields.update(cached)
    if (designator := aircraft.get(SRC_TYPE)) and (cached := type_cache.get(designator)):
        fields.update(cached)
    if geo:  # reverse-geocoded from this aircraft's exact position (no cell caching)
        fields.update(geo)
    event = Event.wrap(fields.raw)

    prior_record = Record({POLLED_AT: polled_at})
    if (raw_altitude := aircraft.get(SRC_ALTITUDE)) is not None:
        prior_record[SRC_ALTITUDE] = raw_altitude  # faithful; altitude_ft coerces at read
    if squawk is not None:
        prior_record[SQUAWK] = squawk
    if lat is not None:
        prior_record[SRC_LAT] = lat
    if lon is not None:
        prior_record[SRC_LON] = lon
    if vertical_rate is not None:
        prior_record[VERTICAL_RATE] = vertical_rate
    return event, prior_record


def _event(polled_at: datetime, event_type: str, icao: str, region: str, callsign: str | None,
           position: Record, detail: str) -> Event:
    """Build an ``adsb-events`` record — identity + position copied under wire keys."""
    event = Event({AT: polled_at, EVENT_TYPE: event_type, HEX: icao, REQUESTED_REGION: region, DETAIL: detail})
    if callsign is not None:
        event[SRC_CALLSIGN] = callsign
    for handle in (SRC_LAT, SRC_LON, SRC_ALTITUDE):
        if (value := position.get(handle)) is not None:
            event[handle] = value
    return event


def _derive_events(icao: str, region: str, callsign: str | None, current: Record,
                   previous: Record | None, polled_at: datetime) -> list[Event]:
    """Onset events for one aircraft: emergency squawk and rapid descent.

    Both fire *once*, at onset, by comparing the current prior against the previous
    one — a squawk that was already an emergency, or a descent already rapid, is not
    re-announced every poll.
    """
    events: list[Event] = []
    squawk, prior_squawk = current.get(SQUAWK), previous.get(SQUAWK) if previous else None
    if squawk in EMERGENCY_SQUAWKS and prior_squawk not in EMERGENCY_SQUAWKS:
        events.append(_event(polled_at, "emergency", icao, region, callsign, current,
                             f"squawk {squawk} ({EMERGENCY_SQUAWKS[squawk]})"))
    vertical_rate, prior_rate = current.get(VERTICAL_RATE), previous.get(VERTICAL_RATE) if previous else None
    if vertical_rate is not None and vertical_rate <= RAPID_DESCENT_FPM and (prior_rate is None or prior_rate > RAPID_DESCENT_FPM):
        events.append(_event(polled_at, "rapid_descent", icao, region, callsign, current, f"{int(vertical_rate)} ft/min"))
    return events


def _cell_message(icao: str, region: str, position: Record) -> Message:
    """Re-key a positioned aircraft onto ``adsb-cells`` by its grid cell (wire keys)."""
    lat, lon = position[SRC_LAT], position[SRC_LON]
    cell = cell_key(lat, lon)
    value = Event({CELL: cell, HEX: icao, REQUESTED_REGION: region, POLLED_AT: position[POLLED_AT],
                   SRC_LAT: lat, SRC_LON: lon})
    if (altitude := position.get(SRC_ALTITUDE)) is not None:
        value[SRC_ALTITUDE] = altitude
    return Message(key=cell, topic=CELLS_TOPIC, value=value)


async def project_page(
    region: str,
    state: State,
    aircraft: list[Record],
    polled_at: datetime,
    airline_cache: dict[str, Record],
    type_cache: dict[str, Record],
    geo: dict[str, Record],
    requested: set[str],
) -> AsyncIterator[Message | State]:
    """Turn one raw response into enriched aircraft, cell fan-out, and derived events.

    Per live aircraft: the spread+enriched ``adsb-aircraft`` message, a ``adsb-cells``
    message if it has a position, and any onset events; then, per departed ICAO, a
    tombstone (and a ``going_dark`` event if it was airborne); then a single
    ``State`` — the new roster/priors and the label caches — as the commit boundary.
    ``geo`` maps ICAO → this poll's reverse-geocode result (from the aircraft's exact
    position, not cached — a local ClickHouse batch is cheap and cell-free geocoding is
    more accurate). Pure and I/O-free (its inputs are already resolved), so the logic
    tier drives it directly.
    """
    previous: dict[str, Record] = state.get(TRACKED) or {}
    current: dict[str, Record] = {}
    for entry in aircraft:
        icao = entry[HEX]
        event, prior_record = _project_one(entry, region, polled_at, previous.get(icao),
                                           airline_cache, type_cache, geo.get(icao))
        current[icao] = prior_record
        yield Message(key=icao, topic=AIRCRAFT_TOPIC, value=event)
        if prior_record.get(SRC_LAT) is not None and prior_record.get(SRC_LON) is not None:
            yield _cell_message(icao, region, prior_record)
        for derived in _derive_events(icao, region, entry.get(SRC_CALLSIGN), prior_record, previous.get(icao), polled_at):
            yield Message(key=icao, topic=EVENTS_TOPIC, value=derived)

    for icao in previous.keys() - current.keys():
        prior = previous[icao]
        yield Message(key=icao, topic=AIRCRAFT_TOPIC,
                      value=Event({HEX: icao, REQUESTED_REGION: region, POLLED_AT: polled_at, IS_DELETED: 1}))
        if (altitude_ft(prior.get(SRC_ALTITUDE)) or 0) >= GOING_DARK_ALT_FT:
            going_dark = Event({AT: polled_at, EVENT_TYPE: "going_dark", HEX: icao, REQUESTED_REGION: region,
                                DETAIL: "lost contact while airborne"})
            for handle in (SRC_LAT, SRC_LON, SRC_ALTITUDE):
                if (value := prior.get(handle)) is not None:
                    going_dark[handle] = value
            yield Message(key=icao, topic=EVENTS_TOPIC, value=going_dark)

    yield State({TRACKED: current, AIRLINE_CACHE: airline_cache, TYPE_CACHE: type_cache,
                 COUNTRIES_REQUESTED: requested})


class Enricher(Protocol):
    """The narrow enrichment surface ``AdsbEnrich`` needs — real over HTTP, fake in tests.

    Each call resolves entities into a ``Record`` of enrichment attributes to merge onto
    the aircraft event (an empty ``Record`` when nothing matched), wrapped at this
    boundary so no naive dict travels into the stage. Raising signals a transient failure
    the caller treats as best-effort (see the module docstring). ``airline`` / ``type`` are
    per-entity (remote Wikidata, one at a time, budget-bounded); ``geocode`` takes *all*
    positions at once — it is a single local ClickHouse ``dictGet`` batch, returning one
    ``Record`` per input point in order, so it is fast and needs no per-item budget.
    """

    async def airline(self, icao: str) -> Record: ...
    async def aircraft_type(self, designator: str) -> Record: ...
    async def geocode(self, points: list[tuple[float, float]]) -> list[Record]: ...


def _labeled(name: str | None, wiki: str | None, name_attr: Attribute, wiki_attr: Attribute) -> Record:
    """A resolved ``(name, wiki)`` pair as a typed Record under the given attributes
    (each key set only when present) — the wrap-at-the-boundary for the label lookups."""
    record = Record()
    if name is not None:
        record[name_attr] = name
    if wiki is not None:
        record[wiki_attr] = wiki
    return record


class _Backoff(Exception):
    """Raised by the enricher when an upstream is in circuit-breaker cooldown: the call
    is skipped without touching the network. The stage treats it like a transient miss
    (``None``, don't cache, retry once the breaker closes) but *quietly* — no per-skip
    warning — so a sustained outage doesn't flood the log."""


class _CircuitBreaker:
    """Opens after a failure and fast-fails for a cooldown, so a rate-limited or dead
    upstream stops being called — and stops blocking the poll loop — until it may have
    recovered. ``now`` is injectable so tests can drive the clock without sleeping."""

    def __init__(self, cooldown: timedelta, now: Callable[[], float] = time.monotonic) -> None:
        self.cooldown = cooldown.total_seconds()
        self._now = now
        self._open_until = 0.0

    @property
    def is_open(self) -> bool:
        return self._now() < self._open_until

    def trip(self) -> None:
        self._open_until = self._now() + self.cooldown

    def reset(self) -> None:
        self._open_until = 0.0


class WikidataClickHouseEnricher:
    """Resolves airline/type names + Wikipedia links from Wikidata; places from ClickHouse.

    Airline/type labels come from Wikidata (a free, community service): this sends a
    proper ``User-Agent`` and — because the caller caches every result in state —
    issues at most one request per distinct entity for the whole run. Do not point it
    there for anything beyond a demo (same spirit as the adsb.lol note). Reverse
    geocoding is a local ClickHouse query over the world dict + the per-level region
    dictionaries (``boundaries.WORLD_DICT`` / :func:`boundaries.region_dict`) — no third
    party, no rate limit.

    Wikidata sits behind a :class:`_CircuitBreaker`: a 429 (rate limit) or a transport
    failure trips it, and while open its lookups fast-fail with ``_Backoff`` instead of
    issuing (slow, futile) requests — which is what keeps a Wikidata rate-limit from
    stalling the enrich stage inside its poll loop. The ClickHouse geocode is local and
    fast, so it needs no breaker; a transient failure is just a best-effort miss.
    """

    WIKIDATA_URL = "https://query.wikidata.org/sparql"

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 clickhouse_url: str = "http://localhost:8123",
                 clickhouse_auth: tuple[str, str] | None = None,
                 wikidata_url: str | None = None,
                 cooldown: timedelta = ENRICHER_COOLDOWN,
                 now: Callable[[], float] = time.monotonic) -> None:
        # `client`/`now` are injectable so tests drive the breaker over a MockTransport
        # and a fake clock; the live path builds the real, User-Agent'd client. The
        # ClickHouse endpoint/credentials are injectable so the integration tier points
        # the geocode at its testcontainer instead of the shared-stack ClickHouse.
        self._client = client or httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=httpx.Timeout(8.0),
        )
        self._clickhouse_url = clickhouse_url
        self._ch_auth = httpx.BasicAuth(*clickhouse_auth) if clickhouse_auth else None
        self.wikidata_url = wikidata_url or self.WIKIDATA_URL
        self._wikidata = _CircuitBreaker(cooldown, now)

    async def _guarded_get(self, breaker: _CircuitBreaker, url: str, params: dict) -> httpx.Response:
        """One GET behind a circuit breaker: skip when open (``_Backoff``); on a 429 or a
        transport failure trip it so we stop hammering a hurting upstream; else close it.
        Non-loop-blocking errors (a fast 5xx, a parse error) don't trip — they're rare
        and transient, and the best-effort caller just retries them next poll."""
        if breaker.is_open:
            raise _Backoff()
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            rate_limited = isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429
            if rate_limited or isinstance(exc, httpx.TransportError):
                if not breaker.is_open:
                    log.warning("%s unhealthy (%s) — pausing its lookups for %ds",
                                httpx.URL(url).host, type(exc).__name__, int(breaker.cooldown))
                breaker.trip()
            raise
        breaker.reset()
        return response

    async def _resolve(self, match: str, cls: str) -> tuple[str | None, str | None]:  # pragma: no cover — live path
        """Run one label-resolving SPARQL query and return ``(name, wiki)`` (each
        ``None`` if absent). ``match`` binds ``?item``, ``cls`` constrains its class,
        and we prefer an entity that is still operating and actually has an article,
        so a shared code resolves to the live one, not a defunct namesake. Kept
        private so the raw JSON never leaves this boundary as a dict."""
        query = (
            f"SELECT ?itemLabel ?article WHERE {{ {match} "
            f"?item wdt:P31/wdt:P279* wd:{cls} . "
            "OPTIONAL { ?item wdt:P576 ?dissolved . } "
            "OPTIONAL { ?article schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . } "
            'SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } } '
            "ORDER BY BOUND(?dissolved) DESC(BOUND(?article)) LIMIT 1"
        )
        response = await self._guarded_get(self._wikidata, self.wikidata_url, {"query": query, "format": "json"})
        rows = Record.wrap(response.json()).get(RESULTS, Record()).get(BINDINGS, [])
        if not rows:
            return None, None
        row = rows[0]
        name = row[ITEM_LABEL][VALUE] if ITEM_LABEL in row else None
        wiki = row[ARTICLE][VALUE] if ARTICLE in row else None
        return name, wiki

    async def airline(self, icao: str) -> Record:  # pragma: no cover — live path
        # P230 = ICAO airline designator; constrain to airlines (Q46970) so a shared
        # code resolves to the operating carrier, not a defunct namesake. `isalnum`
        # guards against SPARQL injection from the uncontrolled feed.
        if not icao.isalnum():
            return Record()
        name, wiki = await self._resolve(f'?item wdt:P230 "{icao}" .', "Q46970")
        return _labeled(name, wiki, AIRLINE, AIRLINE_WIKI)

    async def aircraft_type(self, designator: str) -> Record:  # pragma: no cover — live path
        # Wikidata has no ICAO *type*-designator property, so this best-effort match
        # treats the designator as an aircraft model's short name (P1813), constrained
        # to aircraft models (Q15056993). Coverage is sparse — most types resolve to
        # nothing, which the caller handles (the raw designator is still shown). A
        # bundled ICAO Doc 8643 table is the reliable alternative (see the README).
        if not designator.isalnum():
            return Record()
        name, wiki = await self._resolve(f'?item wdt:P1813 ?sn . FILTER(STR(?sn) = "{designator}") .', "Q15056993")
        return _labeled(name, wiki, AIRCRAFT_TYPE_NAME, TYPE_WIKI)

    async def geocode(self, points: list[tuple[float, float]]) -> list[Record]:
        """Reverse-geocode many positions in ONE ClickHouse query — a stack of ``dictGet``s.

        Per point: the **world** dictionary gives the country it is over (``over_country``
        name + ``iso3``, available once the world map is loaded); then each **per-level**
        region dictionary gives that level's admin area, and the non-empty hits are
        de-duplicated (``arrayDistinct`` — adjacent levels often share a ``shapeName``, e.g.
        ``"Kent; Kent; England"``) and concatenated finest→coarsest into ``nearest_place`` — a
        hierarchical label like ``"Le Bourget; Marne; Grand Est"`` (levels a country lacks
        contribute nothing). All
        points ride one round-trip (``arrayJoin`` over a literal array, JSONEachRow one row
        per point, in order). A ClickHouse error propagates and the best-effort caller
        swallows it (retry next poll). No breaker: the query is local.
        """
        if not points:
            return []
        array = ", ".join(f"(toFloat64({float(lon)}), toFloat64({float(lat)}))" for lat, lon in points)
        levels = ", ".join(f"dictGet('{region_dict(level)}', 'name', p)" for level in reversed(ADMIN_LEVELS))
        sql = (f"SELECT dictGet('{WORLD_DICT}', 'country', p) AS over_country, "
               f"dictGet('{WORLD_DICT}', 'iso3', p) AS iso3, "
               f"arrayStringConcat(arrayDistinct(arrayFilter(n -> n != '', [{levels}])), '; ') AS nearest_place "
               f"FROM (SELECT arrayJoin([{array}]) AS p) FORMAT JSONEachRow")
        kwargs: dict[str, Any] = {"content": sql.encode()}
        if self._ch_auth is not None:
            kwargs["auth"] = self._ch_auth
        response = await self._client.post(self._clickhouse_url, **kwargs)
        response.raise_for_status()
        records: list[Record] = []
        for line in response.text.splitlines():
            if not line.strip():
                continue
            row, record = Record.wrap(json.loads(line)), Record()
            if country := row.get(OVER_COUNTRY):
                record[OVER_COUNTRY] = country
            if iso3 := row.get(ISO3):
                record[ISO3] = iso3
            if place := row.get(NEAREST_PLACE):
                record[NEAREST_PLACE] = place
            records.append(record)
        return records

    async def aclose(self) -> None:  # pragma: no cover — live path
        await self._client.aclose()


class AdsbEnrich(Transformer):
    """Enriches ``adsb-raw`` into ``adsb-aircraft`` + ``adsb-events`` + ``adsb-cells``.

    Subclassing (rather than ``@transformer``) because it owns the ``Enricher``
    client: the real one is built in ``__aenter__`` and closed in ``__aexit__``;
    tests inject a fake before the stage is entered, so no network is touched off
    the live path.
    """

    input_topics = [RAW_TOPIC]

    def __init__(self, enricher: Enricher | None = None) -> None:
        super().__init__()
        self._enricher = enricher

    async def __aenter__(self) -> "AdsbEnrich":
        if self._enricher is None:
            self._enricher = WikidataClickHouseEnricher()  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if isinstance(self._enricher, WikidataClickHouseEnricher):
            await self._enricher.aclose()  # pragma: no cover — live path

    async def _lookup(self, coroutine) -> Record | None:
        """Await one best-effort lookup: success (including an empty not-found
        ``Record``) → cache it; a transient failure → ``None`` (don't cache, retry
        next poll). A ``_Backoff`` (upstream in circuit-breaker cooldown) is the same
        None-don't-cache outcome, but skipped quietly — no per-call warning."""
        try:
            return await coroutine
        except _Backoff:
            return None
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as exc:
            log.warning("enrichment lookup failed, proceeding un-enriched: %s", exc)
            return None

    async def _fill_caches(self, aircraft: list[Record], airline_cache: dict[str, Record],
                           type_cache: dict[str, Record]) -> None:
        """Fill airline/type cache misses via Wikidata — persistent, budget-bounded.

        A key already present (loaded from state) is never looked up again — that is the
        state-as-cache showcase, and why a restart re-issues no resolved lookups. These are
        slow, one-at-a-time *remote* calls, so at most ``LIVE_LOOKUPS_PER_POLL`` are
        attempted per poll: a cold cache could leave hundreds of misses, and doing them all
        here — synchronously, before any message is emitted — would block the consumer's
        poll loop long enough for Kafka to evict the stage. Overflow misses just retry next
        poll (and the enricher fast-fails a rate-limited upstream, so those cost ~nothing).
        Reverse geocoding is deliberately *not* cached — see :meth:`_geocode`.
        """
        assert self._enricher is not None, "enricher is created in __aenter__ or injected"
        budget = LIVE_LOOKUPS_PER_POLL
        for entry in aircraft:
            if budget <= 0:
                break
            callsign = (entry.get(SRC_CALLSIGN) or "").strip()
            if budget > 0 and (code := _airline_code(callsign)) and code not in airline_cache:
                budget -= 1
                if (result := await self._lookup(self._enricher.airline(code))) is not None:
                    airline_cache[code] = result
            if budget > 0 and (designator := entry.get(SRC_TYPE)) and designator not in type_cache:
                budget -= 1
                if (result := await self._lookup(self._enricher.aircraft_type(designator))) is not None:
                    type_cache[designator] = result

    async def _geocode(self, aircraft: list[Record]) -> tuple[dict[str, Record], set[str]]:
        """Reverse-geocode every positioned aircraft's *exact* position in one batch.

        Returns ``(geo, countries)``: ``geo`` maps ICAO → the enrichment overlay
        (``over_country`` name + fine ``nearest_place`` where the country's map is loaded);
        ``countries`` is the set of ISO-3 codes seen this poll — the countries whose fine
        maps the loader should fetch (``transform`` requests them via ``adsb-countries``).
        Deliberately **not cached**: a local batched ``dictGet`` is cheap, and geocoding each
        aircraft's exact position is more accurate than a shared grid cell — with no warmup or
        staleness to manage. One round-trip (one timeout), best-effort (a failure → no geocode
        this poll, retry next); empty overlays (no country/area) are dropped.
        """
        assert self._enricher is not None, "enricher is created in __aenter__ or injected"
        positioned = [(entry[HEX], float(entry[SRC_LAT]), float(entry[SRC_LON])) for entry in aircraft
                      if entry.get(SRC_LAT) is not None and entry.get(SRC_LON) is not None]
        if not positioned:
            return {}, set()
        results = await self._lookup_all(self._enricher.geocode([(lat, lon) for _, lat, lon in positioned]))
        geo: dict[str, Record] = {}
        countries: set[str] = set()
        for (icao, _, _), record in zip(positioned, results):
            if iso3 := record.get(ISO3):
                countries.add(iso3)
            overlay = Record()
            if country := record.get(OVER_COUNTRY):
                overlay[OVER_COUNTRY] = country
            if place := record.get(NEAREST_PLACE):
                overlay[NEAREST_PLACE] = place
            if overlay.raw:
                geo[icao] = overlay
        return geo, countries

    async def _lookup_all(self, coroutine) -> list[Record]:
        """Await one best-effort *batch* lookup: the list on success, ``[]`` on a transient
        failure (don't cache, retry next poll) — the list-valued twin of :meth:`_lookup`."""
        try:
            return await coroutine
        except _Backoff:
            return []
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as exc:
            log.warning("enrichment lookup failed, proceeding un-enriched: %s", exc)
            return []

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        response = msg.value[RESPONSE]
        region = msg.value[CONFIG][NAME]
        polled_at = datetime.fromtimestamp(float(response[NOW]) / 1000, tz=timezone.utc)
        aircraft = response[AC]
        airline_cache = dict(state.get(AIRLINE_CACHE) or {})
        type_cache = dict(state.get(TYPE_CACHE) or {})
        await self._fill_caches(aircraft, airline_cache, type_cache)
        geo, countries = await self._geocode(aircraft)
        # Request a fine map for each country with traffic we have not asked for yet — the
        # loader (boundaries.py) consumes adsb-countries and downloads it. Emit once per
        # country (tracked in state); compaction + the loader's dedup absorb any repeats.
        requested = set(state.get(COUNTRIES_REQUESTED) or set())
        for iso3 in sorted(countries - requested):
            yield Message(key=iso3, topic=COUNTRIES_TOPIC, value=Event({ISO3: iso3}))
            requested.add(iso3)
        async for item in project_page(region, state, aircraft, polled_at, airline_cache, type_cache, geo, requested):
            yield item


stage = AdsbEnrich()
"""The stage the dispatcher runs (``python -m examples.adsb_flight_tracker enrich``).

Reverse-geocodes against the shared-stack ClickHouse (``localhost:8123``) polygon
dictionary and resolves labels from public Wikidata by default. To point the geocode
elsewhere (e.g. a ClickHouse with credentials), pass an enricher:
``AdsbEnrich(enricher=WikidataClickHouseEnricher(clickhouse_url=..., clickhouse_auth=(user, pw)))``.
"""

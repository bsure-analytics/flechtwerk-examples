"""ADS-B enrich — a stateful ``Transformer`` that turns raw responses into events.

Stage 2 of the pipeline. It consumes ``adsb.raw`` (one wrapped response per region,
region-keyed), unrolls the ``ac[]`` array, and produces three streams:

- ``adsb.aircraft`` — one event per aircraft, re-keyed to ICAO hex: the **whole raw
  record spread through** (every feed field, verbatim, under its wire name — so a
  new adsb.lol field appears downstream with no code change) plus our own derived
  fields (``vertical_rate``, ``emergency``, ``region``/``polled_at``/``is_deleted``)
  and live-cached enrichment (airline / aircraft-type / geocode, with Wikipedia
  links), then ``flatten()``-ed to columns. Departures become tombstones
  (``is_deleted=1``).
- ``adsb.events`` — derived aviation events (``emergency`` squawk onset,
  ``rapid_descent`` onset, ``going_dark``) — the "stop viewing, start deriving"
  payoff a raw-feed viewer can't show.
- ``adsb.cells`` — positioned aircraft re-keyed by grid cell, so the conflict
  self-join (``conflict.py``) sees every aircraft in a cell on one partition.

Only the fields a stage computes with have an attribute; everything else rides
through by spreading + ``flatten()`` (see ``attributes.py``). Feed fields keep
their wire names; the dashboards alias them at query time.

**The state store is a live enrichment cache.** Airline names + Wikipedia links
resolve from Wikidata and positions reverse-geocode via Nominatim (aircraft types
are attempted the same way but best-effort — Wikidata has no ICAO type-designator
property, so most types don't resolve; the raw designator ``t`` is always present).
Each entity is looked up *once* and cached in the region's ``State`` (``AIRLINE_CACHE``
/ ``TYPE_CACHE`` / ``GEO_CACHE``). Because that state is changelog-backed, the
cache **survives a restart**: a re-launched enrich stage re-issues zero lookups
for entities it has already resolved. This is the headline showcase — look up
once, remember forever, restore from the log.

**Enrichment is best-effort — a deliberate exception to "let it crash".** A
Wikidata/Nominatim timeout or 5xx is swallowed: the aircraft is emitted
un-enriched and the miss is *not* cached (so it retries next poll). Enrichment is
a decoration; a flaky third-party service must never stall live telemetry. The
position/roster/event logic keeps the framework's strict let-it-crash behaviour —
only the decorative lookups are softened. Two guards stop "best-effort" from
degrading into "silently stalled" when an upstream fails *persistently* (a Nominatim
429 storm): the enricher trips a per-upstream **circuit breaker** and fast-fails
while it is open, and the stage **bounds live lookups per poll**
(``LIVE_LOOKUPS_PER_POLL``). Both matter because the lookups run *inside the
consumer's poll loop* before any message is emitted — unbounded, they would block it
past ``max.poll.interval.ms`` and get the stage evicted from its group, stalling the
pipeline. With the guards, the loop stays responsive and un-enriched telemetry keeps
flowing.

The interesting logic — spread+enrich projection, the roster diff, vertical-rate,
squawk-onset, going-dark, and the cell fan-out — lives in the pure async generator
:func:`project_page`, which touches no framework machinery, no I/O, and no live
enricher (it reads an already-filled cache). The :class:`AdsbEnrich` stage is a
thin shell that fills the cache (the only I/O) and delegates. That split is what
makes the pure-logic test tier possible.
"""
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
    ADDRESS,
    AIRCRAFT_TYPE_NAME,
    AIRLINE,
    AIRLINE_CACHE,
    AIRLINE_WIKI,
    altitude_ft,
    ARTICLE,
    AT,
    BINDINGS,
    CELL,
    CITY,
    CONFIG,
    COUNTRY,
    DETAIL,
    EMERGENCY,
    EVENT_TYPE,
    GEO_CACHE,
    HEX,
    IS_DELETED,
    ITEM_LABEL,
    NAME,
    NEAREST_PLACE,
    NOW,
    OVER_COUNTRY,
    POLLED_AT,
    REGION,
    RESPONSE,
    RESULTS,
    SQUAWK,
    SRC_ALTITUDE,
    SRC_CALLSIGN,
    SRC_LAT,
    SRC_LON,
    SRC_TYPE,
    STATE,
    TOWN,
    TRACKED,
    TYPE_CACHE,
    TYPE_WIKI,
    VALUE,
    VERTICAL_RATE,
)
from .geocoding import NOMINATIM_BASE_URL, USER_AGENT
from .ingest import RAW_TOPIC

log = logging.getLogger(__name__)

AIRCRAFT_TOPIC = "adsb.aircraft"
EVENTS_TOPIC = "adsb.events"
CELLS_TOPIC = "adsb.cells"
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
"""Cap on live enrichment lookups (Wikidata + Nominatim) attempted per poll. Each real
lookup can take seconds and they run inside the consumer's poll loop *before* any message
is emitted, so an unbounded backlog of cache misses — e.g. every position needing a
geocode when the cache is cold or the geocoder is failing — would block the loop past
``max.poll.interval.ms``, get the stage evicted from its group, and silently stall it.
Bounding the work per poll keeps the loop responsive; excess misses just retry next poll.
The enricher's circuit breaker handles a *sustained* outage by fast-failing, so under
normal operation this budget is barely touched (a warm cache issues almost no lookups)."""
ENRICHER_COOLDOWN = timedelta(seconds=60)
"""How long the enricher stops calling an upstream after it rate-limits (429) or its
transport fails — long enough to stop hammering a hurting service, short enough that
enrichment recovers on its own once the service does."""


def flatten(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Recursively flatten a wire-form dict into ``underscore_joined`` keys.

    Dicts contribute path segments; a primitive-only list flattens by index
    (``mlat: [a, b]`` → ``mlat_0``, ``mlat_1``); a list containing structure stays
    one opaque column. This is what lets us *spread* the whole adsb.lol record
    through and still land every leaf as a ClickHouse column — with no attribute
    per field. (The same helper the fret xovis transformer uses.)
    """
    return dict(
        item
        for key, value in record.items()
        for item in (
            flatten(value, prefix + key + "_").items()
            if isinstance(value, dict)
            else flatten({str(i): element for i, element in enumerate(value)}, prefix + key + "_").items()
            if isinstance(value, list) and not any(isinstance(element, (dict, list)) for element in value)
            else [(prefix + key, value)]
        )
    )


def cell_key(lat: float, lon: float) -> str:
    """Map a position to its grid-cell key — the ``adsb.cells`` partition key."""
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
    geo_cache: dict[str, Record],
) -> tuple[Event, Record]:
    """Spread one raw aircraft through, overlay our derived fields + enrichment, flatten.

    Every field the feed sent rides through untouched under its wire name — including
    ``alt_baro`` with its polymorphic value (a number or ``"ground"``), kept faithful;
    a new adsb.lol field appears downstream automatically. We only *add* our own fields
    (``region``/``polled_at``/``is_deleted``/``emergency``/``vertical_rate`` + cached
    enrichment). ``flatten`` collapses any nested feed structure to columns. The
    returned prior carries the faithful ``alt_baro`` too — ``altitude_ft`` coerces it
    to feet where the next poll's vertical rate and the conflict check need a number.
    """
    altitude = altitude_ft(aircraft.get(SRC_ALTITUDE))
    lat, lon = aircraft.get(SRC_LAT), aircraft.get(SRC_LON)
    lat = float(lat) if lat is not None else None
    lon = float(lon) if lon is not None else None
    squawk = aircraft.get(SQUAWK)
    vertical_rate = _vertical_rate(altitude, prior, polled_at)

    fields = Record.wrap(dict(aircraft.raw))  # spread the whole feed record, verbatim
    fields[REGION] = region
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
    if lat is not None and lon is not None and (cached := geo_cache.get(cell_key(lat, lon))):
        fields.update(cached)
    event = Event.wrap(flatten(fields.raw))

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
    """Build an ``adsb.events`` record — identity + position copied under wire keys."""
    event = Event({AT: polled_at, EVENT_TYPE: event_type, HEX: icao, REGION: region, DETAIL: detail})
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
    """Re-key a positioned aircraft onto ``adsb.cells`` by its grid cell (wire keys)."""
    lat, lon = position[SRC_LAT], position[SRC_LON]
    cell = cell_key(lat, lon)
    value = Event({CELL: cell, HEX: icao, REGION: region, POLLED_AT: position[POLLED_AT],
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
    geo_cache: dict[str, Record],
) -> AsyncIterator[Message | State]:
    """Turn one raw response into enriched aircraft, cell fan-out, and derived events.

    Per live aircraft: the spread+enriched ``adsb.aircraft`` message, a ``adsb.cells``
    message if it has a position, and any onset events; then, per departed ICAO, a
    tombstone (and a ``going_dark`` event if it was airborne); then a single
    ``State`` — the new roster/priors and the caches — as the commit boundary. Pure
    and I/O-free (the caches are already filled), so the logic tier drives it directly.
    """
    previous: dict[str, Record] = state.get(TRACKED) or {}
    current: dict[str, Record] = {}
    for entry in aircraft:
        icao = entry[HEX]
        event, prior_record = _project_one(entry, region, polled_at, previous.get(icao),
                                           airline_cache, type_cache, geo_cache)
        current[icao] = prior_record
        yield Message(key=icao, topic=AIRCRAFT_TOPIC, value=event)
        if prior_record.get(SRC_LAT) is not None and prior_record.get(SRC_LON) is not None:
            yield _cell_message(icao, region, prior_record)
        for derived in _derive_events(icao, region, entry.get(SRC_CALLSIGN), prior_record, previous.get(icao), polled_at):
            yield Message(key=icao, topic=EVENTS_TOPIC, value=derived)

    for icao in previous.keys() - current.keys():
        prior = previous[icao]
        yield Message(key=icao, topic=AIRCRAFT_TOPIC,
                      value=Event({HEX: icao, REGION: region, POLLED_AT: polled_at, IS_DELETED: 1}))
        if (altitude_ft(prior.get(SRC_ALTITUDE)) or 0) >= GOING_DARK_ALT_FT:
            going_dark = Event({AT: polled_at, EVENT_TYPE: "going_dark", HEX: icao, REGION: region,
                                DETAIL: "lost contact while airborne"})
            for handle in (SRC_LAT, SRC_LON, SRC_ALTITUDE):
                if (value := prior.get(handle)) is not None:
                    going_dark[handle] = value
            yield Message(key=icao, topic=EVENTS_TOPIC, value=going_dark)

    yield State({TRACKED: current, AIRLINE_CACHE: airline_cache, TYPE_CACHE: type_cache, GEO_CACHE: geo_cache})


class Enricher(Protocol):
    """The narrow enrichment surface ``AdsbEnrich`` needs — real over HTTP, fake in tests.

    Each call resolves one entity into a ``Record`` of enrichment attributes to
    merge onto the aircraft event (an empty ``Record`` when nothing matched),
    wrapped at this boundary so no naive dict travels into the stage. Raising
    signals a transient failure the caller treats as best-effort (see the module
    docstring).
    """

    async def airline(self, icao: str) -> Record: ...
    async def aircraft_type(self, designator: str) -> Record: ...
    async def geocode(self, lat: float, lon: float) -> Record: ...


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


class WikidataNominatimEnricher:
    """Resolves airline/type names + Wikipedia links from Wikidata, places from Nominatim.

    Both are free, community services with usage policies: this sends a proper
    ``User-Agent`` and — because the caller caches every result in state — issues
    at most one request per distinct entity for the whole run. Do not point this at
    them for anything beyond a demo (same spirit as the adsb.lol note).

    Each upstream sits behind a :class:`_CircuitBreaker`: a 429 (rate limit) or a
    transport failure trips it, and while open the affected lookups fast-fail with
    ``_Backoff`` instead of issuing (slow, futile) requests — which is what keeps a
    Nominatim rate-limit from stalling the whole enrich stage (it kept hammering the
    geocoder inside the poll loop until Kafka evicted the consumer).
    """

    WIKIDATA_URL = "https://query.wikidata.org/sparql"
    NOMINATIM_URL = NOMINATIM_BASE_URL + "/reverse"

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 nominatim_url: str | None = None, wikidata_url: str | None = None,
                 cooldown: timedelta = ENRICHER_COOLDOWN,
                 now: Callable[[], float] = time.monotonic) -> None:
        # `client`/`now` are injectable so tests drive the breaker over a MockTransport
        # and a fake clock; the live path builds the real, User-Agent'd client. The URLs
        # are injectable so a caller can point geocoding at a self-hosted Nominatim (the
        # `geocoder` compose profile) instead of the public, rate-limited one.
        self._client = client or httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=httpx.Timeout(8.0),
        )
        self.nominatim_url = nominatim_url or self.NOMINATIM_URL
        self.wikidata_url = wikidata_url or self.WIKIDATA_URL
        self._wikidata = _CircuitBreaker(cooldown, now)
        self._nominatim = _CircuitBreaker(cooldown, now)

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

    async def geocode(self, lat: float, lon: float) -> Record:
        response = await self._guarded_get(
            self._nominatim, self.nominatim_url, {"format": "jsonv2", "lat": lat, "lon": lon, "zoom": 8})
        data = Record.wrap(response.json())
        address, record = data.get(ADDRESS, Record()), Record()
        if COUNTRY in address:
            record[OVER_COUNTRY] = address[COUNTRY]
        if place := (data.get(NAME) or address.coalesce(CITY, TOWN, STATE)):
            record[NEAREST_PLACE] = place
        return record

    async def aclose(self) -> None:  # pragma: no cover — live path
        await self._client.aclose()


class AdsbEnrich(Transformer):
    """Enriches ``adsb.raw`` into ``adsb.aircraft`` + ``adsb.events`` + ``adsb.cells``.

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
            self._enricher = WikidataNominatimEnricher()  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if isinstance(self._enricher, WikidataNominatimEnricher):
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
                           type_cache: dict[str, Record], geo_cache: dict[str, Record]) -> None:
        """Fill cache misses via the live enricher — the only I/O the stage does.

        A key already present (loaded from state) is never looked up again — that is
        the state-as-cache showcase, and why a restart re-issues no resolved lookups.
        At most ``LIVE_LOOKUPS_PER_POLL`` lookups are attempted per poll: a cold cache
        or a failing upstream can leave hundreds of misses, and doing them all here —
        synchronously, before any message is emitted — would block the consumer's poll
        loop long enough for Kafka to evict the stage. Overflow misses just retry next
        poll (and the enricher fast-fails a rate-limited upstream, so those cost ~nothing).
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
            lat, lon = entry.get(SRC_LAT), entry.get(SRC_LON)
            if budget > 0 and lat is not None and lon is not None and (cell := cell_key(float(lat), float(lon))) not in geo_cache:
                budget -= 1
                if (result := await self._lookup(self._enricher.geocode(float(lat), float(lon)))) is not None:
                    geo_cache[cell] = result

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        response = msg.value[RESPONSE]
        region = msg.value[CONFIG][NAME]
        polled_at = datetime.fromtimestamp(float(response[NOW]) / 1000, tz=timezone.utc)
        aircraft = response[AC]
        airline_cache = dict(state.get(AIRLINE_CACHE) or {})
        type_cache = dict(state.get(TYPE_CACHE) or {})
        geo_cache = dict(state.get(GEO_CACHE) or {})
        await self._fill_caches(aircraft, airline_cache, type_cache, geo_cache)
        async for item in project_page(region, state, aircraft, polled_at, airline_cache, type_cache, geo_cache):
            yield item


stage = AdsbEnrich()
"""The stage the dispatcher runs (``python -m examples.adsb_flight_tracker enrich``).

Uses the public Wikidata/Nominatim by default. The dispatcher (``__main__.py``) builds
a repointed stage automatically when the self-hosted ``geocoder`` compose profile is up;
to do it by hand, pass a repointed enricher:
``AdsbEnrich(enricher=WikidataNominatimEnricher(nominatim_url="http://localhost:8091/reverse"))``.
"""

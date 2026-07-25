"""FIRMS ingest — an ``Extractor`` that polls two satellites per watch region.

Stage 1. A ``wildfire-regions`` config record names a place; :meth:`FirmsIngest.enrich_config`
geocodes it once to a bounding box. Each poll then does one GET per source (NOAA-20 and
NOAA-21) for the rolling ``DAY_RANGE`` window, parses the 14-column CSV, drops every row it has
already emitted, and yields the new detections **sorted**, then one ``sweep`` marker, then the
updated seen-set — one transaction per poll per region.

**The third point on the cursor spectrum — why the state looks like this.**
ADS-B and Odds are *snapshot* sources: every poll re-derives the present, so they keep no state
at all. GDELT and SMARD are *monotonic feeds*: a timestamp or a window is a genuine resume
cursor. FIRMS is neither. The area API returns a **rolling day-window snapshot** into which late
detections keep arriving — NRT delivery runs up to ~3 h behind acquisition, so re-polling the
same window keeps yielding genuinely *new* rows for *old* times — and it ships **no unique row
id and nothing monotonic to resume from**. So "what is new?" is answerable only by remembering
what was already emitted, and the honest cursor is a **bounded, event-time-pruned seen-set** of
derived identity hashes, bucketed by acquisition date so that pruning is a whole-bucket drop
(see :data:`SEEN` and :func:`prune_seen`).

**Why the sweep marker is emitted even when nothing is new.** The framework has no timers, so a
transformer only ever runs when a record arrives. The tracker's extinction timeout and its
status heartbeats therefore have to ride on input — so every poll emits a sweep, and a quiet
poll emits a sweep saying ``new_detections = 0``. Skipping it on quiet polls would freeze the
lifecycle at exactly the moment fires are dying, which is when it matters most.

**The map key is a constructor argument, never an environment read.** ``__main__.py`` (the ops
caller) reads ``FIRMS_MAP_KEY`` and injects it, so this module keeps the framework's no-env-magic
rule intact. That is also why there is no module-level ``stage`` singleton here as the other
extractors have — a credential cannot be baked in at import time.

**Let it crash.** HTTP errors, an invalid key (FIRMS answers 400 ``Invalid MAP_KEY.``), an
over-quota response, or a body that isn't CSV all raise. There is one upstream and no remedy a
retry loop could apply, so the 5-minute cadence plus the supervisor's restart *is* the recovery.

Data courtesy of NASA FIRMS — see the README's attribution.
"""
import csv
import hashlib
import io
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from .attributes import (
    ACQUIRED_AT,
    BRIGHT_TI4,
    BRIGHT_TI5,
    CONFIDENCE,
    DAYNIGHT,
    DETECTION_ID,
    DETECTIONS_TOPIC,
    EAST,
    FETCHED_AT,
    FRP,
    INSTRUMENT,
    KIND,
    LAT,
    LON,
    NAME,
    NEW_DETECTIONS,
    NORTH,
    REGION,
    REGIONS_TOPIC,
    SATELLITE,
    SCAN,
    SEEN,
    SOUTH,
    SWEEP_AT,
    TRACK,
    WEST,
)
from .geocoding import Geocoder, NominatimGeocoder

log = logging.getLogger(__name__)

FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov"
"""NASA FIRMS host. The demo constant; injectable for tests via ``FirmsIngest(base_url=…)``."""

SOURCES = ("VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT")
"""The two NRT sources polled per region, one GET each.

Both NOAA VIIRS birds at 375 m. Two satellites roughly halve the revisit gap, which is what
makes a 12 h extinction timeout tolerable, and a fire confirmed by both is meaningfully stronger
evidence than one seen by a single pass. **Suomi NPP (``VIIRS_SNPP_NRT``) is deliberately
excluded** despite currently serving data, because of the data anomaly NASA has flagged since
2026-03-09. ``MODIS_NRT`` and ``LANDSAT_NRT`` exist too but use different column semantics
(MODIS reports confidence as 0–100, not ``l``/``n``/``h``) and would add mapping noise without
teaching a new framework lesson."""

DAY_RANGE = 2
"""How many days of the rolling window to request (FIRMS accepts 1..5).

Today's UTC day counts as day 1, so ``2`` means "today and yesterday" — comfortably more than
the ~3 h NRT latency needs, so no detection can slip past between polls, while keeping each
response small. Widening it costs quota and grows the seen-set for no extra coverage."""

PAD_DEG = 0.1
"""Degrees of slack added to each side of a geocoded bounding box (~11 km).

A place's administrative boundary is not its fire boundary: blazes routinely start just outside
the line and burn in. The pad is generous enough to catch that and small enough not to swallow
a neighbouring region."""

SEEN_HARD_CAP = 20_000
"""Maximum detection ids kept in the seen-set before whole oldest date buckets are dropped.

Each ``State`` is ONE changelog record and must stay well under the broker's ~1 MB
``max.message.bytes`` — the lesson GDELT's clustering bucket taught the hard way, promoted here
to a first-class part of the design. The arithmetic: 20 000 ids × ~15 bytes of JSON ≈ 300 KB,
roughly 3× headroom. For scale, a violent fire day over a large region runs 2–5 k detections/day,
so the cap is ~4× what the ``DAY_RANGE`` window should ever hold. Overflow degrades to
*duplicate emission* (a re-emitted detection re-joins its fire and bumps its count — untidy but
harmless downstream), never to a wedged or crashing stage."""

CSV_HEADER_PREFIX = "latitude,longitude"
"""What the first line of a valid area-CSV response starts with.

FIRMS signals some failures with a **non-CSV body**, and not always with a non-2xx status, so
the header is checked explicitly and a mismatch raises loudly with a snippet of what did arrive.
A bad key is in fact a 400 (``Invalid MAP_KEY.``) that ``raise_for_status`` already catches; this
guard is what stops a maintenance page or a quota notice served as 200 from being parsed into
zero detections and silently reported as "no fires"."""

_ID_SEPARATOR = "\x1f"
"""ASCII unit separator, joining the identity fields before hashing so that no combination of
field values can collide by concatenation (a coordinate can't run into a date)."""


def parse_area_csv(text: str) -> list[dict[str, str]]:
    """The area API's CSV body as a list of raw string rows — pure.

    Values are left as **strings**, exactly as delivered: :func:`detection_id` hashes them
    verbatim (so identity never depends on float formatting) and :func:`normalize_detection`
    does the typing. A header-only body is a perfectly normal quiet region → ``[]``. A body that
    doesn't start with the expected header raises ``RuntimeError`` with a snippet, so an
    unexpected non-CSV response is loud instead of looking like "no fires".
    """
    body = text.lstrip("﻿")
    first_line = body.splitlines()[0] if body.strip() else ""
    if not first_line.startswith(CSV_HEADER_PREFIX):
        raise RuntimeError(
            f"FIRMS did not return area CSV (expected a {CSV_HEADER_PREFIX!r} header, "
            f"got {text[:200]!r})")
    return list(csv.DictReader(io.StringIO(body)))


def detection_id(row: dict[str, str]) -> str:
    """A stable 12-hex identity for one hotspot pixel — pure.

    FIRMS ships **no unique row id**, so identity is derived from the fields that together pin a
    detection down: position, acquisition date and time, and which satellite saw it. The **raw
    CSV strings** are hashed, never reparsed floats — ``"37.54301"`` and a float that formats
    back as ``37.543010000000004`` would otherwise be different detections, and the seen-set
    would leak duplicates on every restart. 12 hex digits ≈ 4.8 × 10¹⁴ values: collision-free at
    the tens-of-thousands scale the seen-set holds, at a fifth of a full digest's JSON size.
    """
    material = _ID_SEPARATOR.join((
        row["latitude"], row["longitude"], row["acq_date"], row["acq_time"], row["satellite"]))
    return hashlib.sha256(material.encode()).hexdigest()[:12]


def acquired_at(row: dict[str, str]) -> datetime:
    """The satellite's acquisition instant as aware UTC — pure.

    FIRMS splits it across ``acq_date`` (``YYYY-MM-DD``) and ``acq_time``, an **unpadded integer
    HHMM**: ``230`` is 02:30 and ``48`` would be 00:48, so string slicing would be wrong and
    integer arithmetic is right. Always UTC.
    """
    hhmm = int(row["acq_time"])
    return datetime.combine(
        date.fromisoformat(row["acq_date"]),
        time(hour=hhmm // 100, minute=hhmm % 100),
        tzinfo=timezone.utc,
    )


def normalize_detection(row: dict[str, str], region: str, fetched_at: datetime) -> Event:
    """Project one raw CSV row into a typed ``detection`` record — pure.

    Numbers are wrapped in ``float()`` because the ``FLOAT`` codec rejects ``int`` and FIRMS
    delivers saturated brightness values as a bare ``367``. The three optional measurements are
    included only when the field is **non-empty**: emptiness is tested on the string, never by
    falsiness, because a genuine ``frp`` of ``0.0`` occurs in real data and must survive as 0.0
    rather than vanishing (the mirror of the house rule that an absent value must never become a
    fabricated 0).
    """
    detection = Event({
        KIND: "detection",
        REGION: region,
        DETECTION_ID: detection_id(row),
        LAT: float(row["latitude"]),
        LON: float(row["longitude"]),
        ACQUIRED_AT: acquired_at(row),
        SATELLITE: row["satellite"],
        INSTRUMENT: row["instrument"],
        CONFIDENCE: row["confidence"],
        SCAN: float(row["scan"]),
        TRACK: float(row["track"]),
        DAYNIGHT: row["daynight"],
        FETCHED_AT: fetched_at,
    })
    for attribute, column in ((FRP, "frp"), (BRIGHT_TI4, "bright_ti4"), (BRIGHT_TI5, "bright_ti5")):
        if (raw := (row.get(column) or "").strip()):
            detection[attribute] = float(raw)
    return detection


def prune_seen(seen: dict[str, list[str]], max_date: date, *,
               day_range: int = DAY_RANGE, hard_cap: int = SEEN_HARD_CAP) -> dict[str, list[str]]:
    """The seen-set trimmed to the live window and the changelog budget — pure.

    Two independent bounds:

    * **Event-time window.** Dates older than ``max_date - day_range`` days are dropped whole.
      Once a date falls out of the requested window the API can never mention it again, so its
      ids can never be needed. The window is kept one day wider than ``day_range`` as grace, so
      a detection sitting right at the UTC midnight rollover isn't re-emitted.
    * **Hard cap.** If the surviving ids still exceed ``hard_cap``, whole oldest date buckets go
      until the total fits, with a WARNING naming what was dropped. This is the guard that keeps
      each State under the broker's record limit no matter how violent a fire day gets; the cost
      of hitting it is re-emitting some older detections, not a stuck stage.
    """
    cutoff = (max_date - timedelta(days=day_range)).isoformat()
    pruned = {day: ids for day, ids in seen.items() if day >= cutoff}
    total = sum(len(ids) for ids in pruned.values())
    dropped: list[str] = []
    while total > hard_cap and len(pruned) > 1:
        oldest = min(pruned)
        total -= len(pruned.pop(oldest))
        dropped.append(oldest)
    if dropped:
        log.warning(
            "seen-set over the %d-id cap: dropped date bucket(s) %s; %d ids kept. Detections "
            "from the dropped day(s) may be re-emitted (harmless: a duplicate re-joins its fire)",
            hard_cap, ", ".join(dropped), total)
    return pruned


class FirmsIngest(Extractor):
    """Polls both VIIRS satellites for each watch region → ``wildfire-detections``.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed in
    ``__aexit__``) and the geocoder; tests inject a ``MockTransport`` client and a fake geocoder,
    so no network is touched off the live path. ``map_key`` is required and supplied by the ops
    caller — see the module docstring.
    """

    config_topics = [REGIONS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, map_key: str,
                 base_url: str = FIRMS_BASE_URL, detections_topic: str = DETECTIONS_TOPIC,
                 geocoder: Geocoder | None = None) -> None:
        super().__init__()
        self._client = client
        self._map_key = map_key
        self._base_url = base_url.rstrip("/")
        self._topic = detections_topic
        self._geocoder = geocoder

    async def __aenter__(self) -> "FirmsIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(60.0), follow_redirects=True,
            )
        if self._geocoder is None:
            self._geocoder = NominatimGeocoder()  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path
        if isinstance(self._geocoder, NominatimGeocoder):
            await self._geocoder.aclose()  # pragma: no cover — live path

    async def enrich_config(self, config: Config) -> Config:
        """Fill in a region's bounding box from its name, once per config record — the fallback.

        The framework calls this exactly once when a config arrives (not per poll), so the
        Nominatim lookup costs one request per region for the lifetime of the config.

        In practice this is the **uncommon** path: ``request.py`` resolves the box at request time
        and caches all four edges in the record, and such a config is returned untouched. What
        this hook exists for is a **name-only record** — someone producing
        ``{"region": …, "name": "Attica, Greece"}`` straight to the topic from Kafbat — which stays
        a first-class way to ask for a region precisely because the stage can still resolve it.
        Spreading (``{**config, …}``) enriches without mutating.

        The box is padded by :data:`PAD_DEG`. A name that resolves to nothing raises
        ``LookupError`` out of the geocoder: the config is unusable, so let it crash rather than
        poll a meaningless box.
        """
        edges = (WEST, SOUTH, EAST, NORTH)
        if all(config.get(edge) is not None for edge in edges):
            return config
        assert self._geocoder is not None, "geocoder is opened in __aenter__ or injected"
        south, north, west, east = await self._geocoder.bbox(config[NAME])
        log.info("region %s: geocoded %r to bbox %s,%s,%s,%s (+%.2f° pad)",
                 config[REGION], config[NAME], west, south, east, north, PAD_DEG)
        return Config({**config,
                       WEST: west - PAD_DEG, SOUTH: south - PAD_DEG,
                       EAST: east + PAD_DEG, NORTH: north + PAD_DEG})

    def _area_url(self, config: Config, source: str) -> str:
        """The area-API URL for one source and region — ``west,south,east,north`` order."""
        bbox = f"{config[WEST]},{config[SOUTH]},{config[EAST]},{config[NORTH]}"
        return f"{self._base_url}/api/area/csv/{self._map_key}/{source}/{bbox}/{DAY_RANGE}"

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit this region's new detections, then its sweep marker, then the seen-set.

        The order is the two-yield contract: messages first, the ``State`` that accounts for them
        last, so the whole poll is one transaction. On a crash the seen-set is unadvanced and the
        re-poll re-derives exactly the same news — nothing is double-emitted, because the ids are
        derived deterministically from the rows themselves.

        Detections are sorted by ``(acq_date, acq_time, satellite, detection_id)`` so a replay
        produces them in the same order, which matters because the tracker's clustering result
        depends on arrival order (which detection founds a fire and which joins it).
        """
        assert self._client is not None, "client is opened in __aenter__ or injected"
        region = config[REGION]

        seen = {day: list(ids) for day, ids in (state.get(SEEN) or {}).items()}
        known = {identity for ids in seen.values() for identity in ids}

        rows: list[dict[str, str]] = []
        fetched_ats: list[datetime] = []
        for source in SOURCES:
            response = await self._client.get(self._area_url(config, source))
            response.raise_for_status()
            rows += parse_area_csv(response.text)
            fetched_ats.append(self._fetched_at(response))
        # The later of the two responses: the sweep's event time must not claim to know more
        # than the freshest data actually fetched.
        fetched_at = max(fetched_ats)

        fresh: list[dict[str, str]] = []
        for row in rows:
            identity = detection_id(row)
            if identity in known:
                continue
            known.add(identity)  # also dedupes within the poll — the two sources' windows overlap
            seen.setdefault(row["acq_date"], []).append(identity)
            fresh.append(row)
        fresh.sort(key=lambda row: (
            row["acq_date"], int(row["acq_time"]), row["satellite"], detection_id(row)))

        for row in fresh:
            yield Message(key=region, topic=self._topic,
                          value=normalize_detection(row, region, fetched_at))
        yield Message(key=region, topic=self._topic, value=Event({
            KIND: "sweep", REGION: region, SWEEP_AT: fetched_at, NEW_DETECTIONS: len(fresh),
        }))
        log.info("%s: %d new detection(s) of %d row(s) across %d source(s) (sweep at %s)",
                 region, len(fresh), len(rows), len(SOURCES), fetched_at.isoformat())
        # Prune against the newest acquisition date the set knows about, not against the poll's
        # own clock: the window that matters is the one the API will keep serving.
        pruned = prune_seen(seen, date.fromisoformat(max(seen))) if seen else {}
        yield State({SEEN: pruned})

    @staticmethod
    def _fetched_at(response: httpx.Response) -> datetime:
        """The server ``Date`` as aware UTC — the event-time clock. Server-controlled, so tests
        pin it via a ``Date`` response header; falls back to now if it is absent."""
        raw = response.headers.get("Date")
        if raw:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        return datetime.now(timezone.utc)  # pragma: no cover — live feed always sends Date

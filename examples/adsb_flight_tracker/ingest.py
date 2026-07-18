"""ADS-B ingest — an ``Extractor`` that wraps the raw adsb.lol feed, untouched.

Stage 1 of the pipeline. It polls adsb.lol's radius endpoint once per region and
emits **one** ``adsb.raw`` message per poll, structured as three nested Records:
the whole response verbatim (the entire ``ac[]`` array intact), the ``config``
that produced it, and ``metadata`` provenance (``fetched_at`` and a
``fetch_duration`` timedelta). Nesting keeps the *uncontrolled* feed schema in its
own namespace, so a feed key can never collide with our fields.

Why wrap-and-forward instead of unrolling here: the adsb.lol feed is live and
un-replayable — there is no "poll yesterday". Capturing the raw response is the
only way to keep history, so a later change to the enrichment (``enrich.py``) can
reprocess ``adsb.raw`` from the changelog rather than re-poll a feed that has
already moved on. Ingestion also runs on adsb.lol's polite, rate-limited cadence;
decoupling it from the enrichment lets each scale and fail independently.

The wrapping logic lives in the plain function :func:`wrap_response` — no
framework machinery, no I/O — so the logic tier drives it directly. The
:class:`AdsbIngest` stage is a thin shell that fetches and delegates; the
enrichment and roster logic it used to fuse in now lives downstream in
``enrich.py``.

"Let it crash": a feed timeout or 5xx propagates and crashes the poll; the
orchestrator restarts and the cursor restores from the changelog — no in-process
retry.
"""
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Record

from .attributes import (
    CONFIG,
    FETCH_DURATION,
    FETCHED_AT,
    LAT,
    LON,
    METADATA,
    NAME,
    RADIUS,
    RESPONSE,
)
from .geocoding import USER_AGENT, Geocoder, NominatimGeocoder

ADSB_BASE_URL = "https://api.adsb.lol"
CONFIG_TOPIC = "adsb.regions"
RAW_TOPIC = "adsb.raw"
DEFAULT_RADIUS = 100
MAX_RADIUS = 250
"""adsb.lol rejects a larger radius; enrich_config clamps to it."""


def wrap_response(config: Config, response: Record, fetched_at: datetime, duration: timedelta) -> Event:
    """Assemble one ``adsb.raw`` poll record from its three nested parts.

    ``fetch`` ``Record.wrap``-s the response at the HTTP boundary, so nothing naive
    travels past the edge; here it becomes the message value — the response, the
    config that produced it, and metadata, each nested under its own attribute. The
    *uncontrolled* feed schema therefore keeps its own namespace and can never
    collide with our fields (a flat spread would let a feed key clash). Pure and
    I/O-free, so the logic tier drives it directly.
    """
    return Event({
        CONFIG: config,
        METADATA: Record({FETCHED_AT: fetched_at, FETCH_DURATION: duration}),
        RESPONSE: response,
    })


class AdsbIngest(Extractor):
    """Polls adsb.lol's radius endpoint, one region per config record, → ``adsb.raw``.

    Subclassing (rather than ``@extractor``) is what the framework recommends for a
    stage that owns a resource: the ``httpx`` client is opened in ``__aenter__`` and
    closed in ``__aexit__``. Tests inject their own client (a stubbed transport)
    before the stage is entered, so no network is touched off the live path.
    """

    config_topics = [CONFIG_TOPIC]
    client: httpx.AsyncClient | None = None
    geocoder: Geocoder | None = None

    async def __aenter__(self) -> "AdsbIngest":
        if self.client is None:
            self.client = httpx.AsyncClient(
                base_url=ADSB_BASE_URL,
                headers={"User-Agent": USER_AGENT},
                timeout=httpx.Timeout(10.0),
            )
        if self.geocoder is None:
            self.geocoder = NominatimGeocoder()  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self.client is not None:
            await self.client.aclose()
        if isinstance(self.geocoder, NominatimGeocoder):
            await self.geocoder.aclose()  # pragma: no cover — live path

    async def enrich_config(self, config: Config) -> Config:
        """Normalise a region config once, when it arrives.

        Two normalisations, applied by the framework exactly once per config record
        (not per poll); spreading (``{**config, ...}``) enriches without mutating the
        original:

        - **Geocode a name-only region.** A config may give just a name
          (``{"name": "London"}``) and omit ``lat``/``lon``; when either is
          absent this forward-geocodes the name to its centre (see :mod:`.geocoding`),
          so the poll always has coordinates to search around. That lookup is
          *essential* — without it the region can't be polled — so it keeps the
          framework's "let it crash" behaviour (a timeout / 5xx propagates and the
          orchestrator restarts; a name matching nothing raises), unlike the enrich
          stage's decorative, best-effort reverse lookups.
        - **Default and clamp the radius** to adsb.lol's maximum, so every poll works
          from a valid, bounded value.
        """
        if config.get(LAT) is None or config.get(LON) is None:
            assert self.geocoder is not None, "geocoder is opened in __aenter__"
            lat, lon = await self.geocoder.locate(config[NAME])
            config = Config({**config, LAT: lat, LON: lon})
        return Config({**config, RADIUS: min(config.get(RADIUS) or DEFAULT_RADIUS, MAX_RADIUS)})

    async def fetch(self, config: Config) -> tuple[datetime, timedelta, Record]:
        """Fetch the region's aircraft, wrapping the response at the JSON boundary.

        The raw JSON is ``Record.wrap``-ed here, at the edge, so nothing downstream
        ever handles a naive dict (it becomes the ``adsb.raw`` ``Event`` in
        ``wrap_response``). Errors propagate — a timeout or a 5xx crashes the poll
        ("let it crash"): the orchestrator restarts and the cursor restores from
        the changelog, no in-process retry.
        """
        assert self.client is not None, "client is opened in __aenter__"
        start = time.monotonic()
        response = await self.client.get(f"/v2/point/{config[LAT]}/{config[LON]}/{config[RADIUS]}")
        response.raise_for_status()
        duration = timedelta(seconds=time.monotonic() - start)
        return datetime.now(timezone.utc), duration, Record.wrap(response.json())

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit one raw record per poll — no cursor.

        The raw layer is stateless replay: every poll re-reads the live feed from
        scratch, so there is nothing to resume from and no ``State`` to yield. The
        lone ``Message`` still commits durably — the framework closes the *trailing
        page* when the generator completes, committing the message in its own
        transaction with no ``State`` required (see ``Extractor.poll``). Yielding a
        dummy cursor here would only write a changelog record on every poll that
        nothing ever reads.
        """
        fetched_at, duration, response = await self.fetch(config)
        yield Message(key=config[NAME], topic=RAW_TOPIC, value=wrap_response(config, response, fetched_at, duration))


stage = AdsbIngest()
"""The stage the dispatcher runs (``python -m examples.adsb_flight_tracker ingest``).

Forward-geocodes a name-only region config (see ``enrich_config``) against the public
Nominatim by default. The dispatcher (``__main__.py``) repoints ``geocoder`` at the
self-hosted Nominatim automatically when the ``geocoder`` compose profile is up; to do
it by hand, assign one before running:
``stage.geocoder = NominatimGeocoder(search_url="http://localhost:8091/search")``.
"""

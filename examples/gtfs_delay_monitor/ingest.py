"""GTFS-Realtime ingest — an ``Extractor`` that turns the protobuf feed into per-trip
delay updates.

Stage 1. A ``gtfs-rt-feeds`` config record names a GTFS-Realtime endpoint; each poll
downloads the (~52 MB, national) protobuf snapshot, **decodes it at the edge**
(protobuf → plain dict via :func:`decode_feed`), and emits one message per
``TripUpdate`` to ``gtfs-trip-updates``, keyed by ``trip_id``. ServiceAlerts are
ignored in v1 (their text was empty in the live feed — an extension point).

**Protobuf never crosses the Kafka boundary.** The framework stays JSON-only:
:func:`decode_feed` runs ``MessageToDict`` inside the stage and the messages it yields
are ordinary ``Event``s (JSON-native dicts). This is the same "decode the source in
the extractor, emit typed records" shape as GDELT (tab-delimited files) and ADS-B (an
HTTP JSON API) — a binary source is no different.

**Event time is the feed header timestamp.** ``header.timestamp`` (POSIX epoch)
becomes ``FEED_TS`` on every message *and* the resume cursor: the feed refreshes every
10 s but a poll whose snapshot is not newer than the cursor yields nothing (the poll
interval is a respectful 60 s, and an ``ETag`` gives a cheap 304 when unchanged). All
rows in one snapshot share the timestamp, so downstream delay windows are exact —
never wall-clock.

The decode is the pure function :func:`decode_feed` (bytes in, ``(feed_ts, messages)``
out), so the logic tier drives it straight off the committed ``.pb`` fixture.
:class:`GtfsRtIngest` is the thin shell that fetches, gates on the cursor, and delegates.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from .attributes import ETAG, FEED_TS, TRIP, TRIP_ID, URL

log = logging.getLogger(__name__)

RT_FEEDS_CONFIG_TOPIC = "gtfs-rt-feeds"
"""Compacted config topic, one record per GTFS-Realtime feed to poll (keyed by a name
like ``germany-free``), seeded by ``setup.py``. Each entry drives one poll target."""

UPDATES_TOPIC = "gtfs-trip-updates"
"""Partitioned output: one ``TripUpdate`` per message, keyed by ``trip_id`` and
co-partitioned with ``gtfs-trip-profiles`` so the delay join meets on one task."""

DEFAULT_FEED_URL = "https://realtime.gtfs.de/realtime-free.pb"
"""The free national GTFS-Realtime feed (TripUpdates + ServiceAlerts, ~10 s cadence).
The demo constant; the real value is injected via the config record (``setup.py``)."""


def decode_feed(pb_bytes: bytes) -> tuple[datetime, list[tuple[str, Event]]]:
    """Decode a GTFS-Realtime ``FeedMessage`` into ``(feed_ts, [(trip_id, update)])``.

    Pure and I/O-free. ``feed_ts`` is the header timestamp as an aware UTC datetime.
    Each ``TripUpdate`` entity becomes one ``Event``: the ``MessageToDict`` projection
    spread through verbatim under its proto field names (``trip``, ``stop_time_update``,
    …) with ``FEED_TS`` stamped on top — so a field we don't read today (``vehicle``,
    per-stop ``time``, …) still rides to ClickHouse untouched. Entities without a
    ``trip_id`` are skipped (nothing to key on); ServiceAlerts are ignored."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_bytes)
    feed_ts = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    updates: list[tuple[str, Event]] = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        raw = MessageToDict(entity.trip_update, preserving_proto_field_name=True)
        trip_id = raw.get("trip", {}).get("trip_id")
        if not trip_id:
            continue
        # wrap() encodes verbatim (JSON-native passes through); FEED_TS's datetime
        # renders ISO-8601 via the ANY codec so the whole value stays wire-ready.
        updates.append((trip_id, Event.wrap({**raw, FEED_TS.name: feed_ts})))
    return feed_ts, updates


class GtfsRtIngest(Extractor):
    """Polls a GTFS-Realtime feed and emits each snapshot's TripUpdates, once.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``,
    closed in ``__aexit__``); tests inject a stub transport serving the ``.pb`` fixture.
    ``httpx`` negotiates gzip transparently, easing the ~52 MB national payload."""

    config_topics = [RT_FEEDS_CONFIG_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, updates_topic: str = UPDATES_TOPIC) -> None:
        super().__init__()
        self._client = client
        self._topic = updates_topic

    async def __aenter__(self) -> "GtfsRtIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(120.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit one new snapshot's TripUpdates, then advance the ``feed_ts`` cursor.

        A 304 (``If-None-Match`` on the stored ``ETag``) or a snapshot whose
        ``feed_ts`` is not newer than the cursor is a no-op — the feed republishes the
        full snapshot every 10 s, so nothing is lost by skipping a duplicate. Otherwise
        every TripUpdate is emitted (messages first) and the cursor advances last, so
        the snapshot and its cursor commit in one transaction."""
        assert self._client is not None, "client is opened in __aenter__ or injected"
        etag = state.get(ETAG)
        response = await self._client.get(config[URL], headers={"If-None-Match": etag} if etag else {})
        if response.status_code == 304:
            return
        response.raise_for_status()

        feed_ts, updates = decode_feed(response.content)
        cursor = state.get(FEED_TS)
        if cursor is not None and feed_ts <= cursor:
            return  # this snapshot (or an older/repeated one) was already emitted

        for trip_id, update in updates:
            yield Message(key=trip_id, topic=self._topic, value=update)
        log.info("Emitted %d trip updates (feed_ts %s)", len(updates), feed_ts.isoformat())
        yield State({FEED_TS: feed_ts, ETAG: response.headers.get("ETag")})


stage = GtfsRtIngest()
"""The stage the dispatcher runs (``python -m examples.gtfs_delay_monitor ingest``)."""

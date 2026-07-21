"""GDELT ingest — an ``Extractor`` that turns the 15-minute file feed into rows.

Stage 1. One extractor handles every feed: a ``gdelt-feeds`` config record names a feed
(``english`` / ``translation``), and each poll fetches that feed's pointer file, and — when
it names a **new** 15-minute slice — downloads the three announced zips, verifies each one's
size + MD5, unzips it in memory, and emits every row of the three tables:

- Events → ``gdelt-events-raw``, keyed by ``GlobalEventID``
- Mentions → ``gdelt-mentions-raw``, keyed by ``GlobalEventID`` (co-partitioned with events)
- GKG → ``gdelt-gkg-raw``, keyed by the article URL (a single-partition topic; see ``stories.py``)

**The cursor is the resume mechanism, and it is the file timestamp.** ``State`` holds the last
fully-processed slice timestamp *per feed* (the state key is the feed name). A poll whose
pointer names a timestamp ``<=`` the cursor is a no-op — that absorbs an empty/late slot and
the pointer briefly repeating the previous timestamp. This is the documented Extractor
pattern: yield the page's messages FIRST, then the ``State`` cursor LAST, so the whole slice
and its cursor commit in one transaction. Crash mid-slice ⇒ the cursor never advanced ⇒ the
re-poll re-emits the slice: **at-least-once into Kafka**, with downstream dedup where it
matters (the stories stage dedups by URL). "Let it crash": a download timeout, a 5xx, or a
size/MD5 mismatch propagates and the orchestrator restarts — no in-process retry.

**Event time is the file timestamp, never the row's ``SQLDATE``** (machine-coded, observed a
year stale). Every row in one file shares that timestamp, so it rides in ``metadata.file_ts``
and downstream windows are exact.

The projection lives in the pure function :func:`build_page` — no framework machinery, no
I/O — so the logic tier drives it straight off the fixture bytes. :class:`GdeltIngest` is the
thin shell that fetches, verifies, and delegates.
"""
import hashlib
import logging
import zipfile
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from io import BytesIO

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Record

from .parsers import parse_table
from .schema import (
    COLUMNS_BY_TABLE,
    DOCUMENT_IDENTIFIER,
    FEED,
    FEED_NAME,
    FETCHED_AT,
    FILE_TS,
    GLOBAL_EVENT_ID,
    METADATA,
    ROW,
    ROW_NUMBER,
    TABLE,
    parse_gdelt_datetime,
)

log = logging.getLogger(__name__)

GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"
"""The GDELT 2.0 feed root — plain HTTP GET, no auth/key/quota. Injectable so tests point
the stage at a stub and the download URLs are re-based onto it (see :meth:`GdeltIngest.poll`)."""

FEEDS_CONFIG_TOPIC = "gdelt-feeds"
"""Compacted config topic, one record per feed to poll (``english`` / ``translation``),
seeded by ``setup.py``. Each entry drives one poll target; the cursor is per feed."""

EVENTS_RAW_TOPIC = "gdelt-events-raw"
MENTIONS_RAW_TOPIC = "gdelt-mentions-raw"
GKG_RAW_TOPIC = "gdelt-gkg-raw"

POINTER_BY_FEED = {"english": "lastupdate.txt", "translation": "lastupdate-translation.txt"}
"""Which pointer file each feed publishes. The parallel ``-translation`` feed is the
machine-translated non-English world press — European coverage is a stated motivation."""

DEFAULT_RAW_TOPICS = {"events": EVENTS_RAW_TOPIC, "mentions": MENTIONS_RAW_TOPIC, "gkg": GKG_RAW_TOPIC}
"""table → raw topic. Injectable on the stage so the integration tier can run against
per-test topics on the shared broker without cross-contamination."""
TABLE_KEY = {"events": GLOBAL_EVENT_ID, "mentions": GLOBAL_EVENT_ID, "gkg": DOCUMENT_IDENTIFIER}
"""table → the column whose value keys the raw message (co-partitioning events + mentions
on ``GlobalEventID``; keying GKG by article URL)."""
TABLE_INFIX = {"events": ".export.", "mentions": ".mentions.", "gkg": ".gkg."}
"""table → the filename infix that identifies its file in a pointer line."""


def parse_pointer(text: str) -> list[tuple[int, str, str]]:
    """Parse a ``lastupdate.txt`` body into ``[(size, md5, filename)]``.

    Each line is ``<size_bytes> <md5> <url>`` (whitespace-separated); we keep the URL's
    basename, since the download is re-based onto the injected ``base_url`` (so a stub or a
    local server can serve it without the absolute GDELT host). Blank lines are ignored.
    """
    entries: list[tuple[int, str, str]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            entries.append((int(parts[0]), parts[1], parts[2].rsplit("/", 1)[-1]))
    return entries


def table_of(filename: str) -> str | None:
    """Which GDELT table a file's name identifies (``events`` / ``mentions`` / ``gkg``)."""
    for table, infix in TABLE_INFIX.items():
        if infix in filename:
            return table
    return None


def unzip_single(raw: bytes) -> bytes:
    """Return the sole member of a GDELT ``.zip`` (they each hold one file)."""
    with zipfile.ZipFile(BytesIO(raw)) as zf:
        return zf.read(zf.namelist()[0])


def _row_message(table: str, topic: str, row: dict[str, str], file_ts: datetime, feed: str,
                 row_number: int, fetched_at: datetime) -> Message | None:
    """Wrap one parsed row as a raw-capture message, or ``None`` if it has no key.

    The row rides nested under ``ROW`` in its own namespace (ADS-B's spread-through), with
    ``METADATA`` provenance alongside — so an uncontrolled GDELT column can never collide
    with our derived fields. A row missing its key column is malformed data (not a transient
    fault), so it is skipped defensively rather than crashing the whole slice.
    """
    key = row.get(TABLE_KEY[table].name)
    if not key:
        return None
    value = Event({
        ROW: Record.wrap(dict(row)),
        METADATA: Record({FILE_TS: file_ts, FEED: feed, TABLE: table,
                          ROW_NUMBER: row_number, FETCHED_AT: fetched_at}),
    })
    return Message(key=key, topic=topic, value=value)


def build_page(tables: dict[str, bytes], *, feed: str, file_ts: datetime, fetched_at: datetime,
               topics: dict[str, str] = DEFAULT_RAW_TOPICS) -> Iterator[Message]:
    """Project one 15-minute slice's three unzipped files into raw-capture messages.

    Pure and I/O-free (bytes in, ``Message``s out) so the logic tier drives it off the
    committed fixtures. Emits events, then mentions, then GKG; keyless rows are skipped.
    ``topics`` maps each table to its raw topic (overridable for per-test isolation).
    """
    for table, raw in tables.items():
        for i, row in enumerate(parse_table(raw, COLUMNS_BY_TABLE[table]), start=1):
            message = _row_message(table, topics[table], row, file_ts, feed, i, fetched_at)
            if message is not None:
                yield message


class GdeltIngest(Extractor):
    """Polls each feed's pointer file and emits a new 15-minute slice's rows, once.

    Subclassing (rather than ``@extractor``) because it owns the ``httpx`` client: the live
    one is built in ``__aenter__`` and closed in ``__aexit__``; tests inject a stubbed
    transport before the stage is entered, so no network is touched off the live path.
    """

    config_topics = [FEEDS_CONFIG_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, base_url: str = GDELT_BASE_URL,
                 raw_topics: dict[str, str] | None = None) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._topics = raw_topics or DEFAULT_RAW_TOPICS

    async def __aenter__(self) -> "GdeltIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(120.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def _get(self, path: str) -> httpx.Response:
        assert self._client is not None, "client is opened in __aenter__ or injected"
        response = await self._client.get(f"{self._base_url}/{path}")
        response.raise_for_status()
        return response

    async def _download_verified(self, size: int, md5: str, filename: str) -> bytes:
        """Fetch one announced file and verify its size, then its MD5, against the pointer.

        Size first: it is free (no hashing) and catches a truncated or wrong download before
        we spend anything computing the digest of a file we already know is bad. A mismatch of
        either is a real fault — raise it ("let it crash"): the slice never commits, so the
        re-poll re-fetches it. Re-based onto ``base_url`` so a stub/local server serves it.
        """
        raw = (await self._get(filename)).content
        if len(raw) != size:
            raise ValueError(f"{filename}: expected {size} bytes, got {len(raw)}")
        digest = hashlib.md5(raw).hexdigest()
        if digest.lower() != md5.lower():
            raise ValueError(f"{filename}: expected md5 {md5}, got {digest}")
        return raw

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit one new 15-minute slice's rows, then advance the per-feed cursor.

        The pointer names the slice; if its timestamp is not newer than the cursor (an empty
        or late slot, or the pointer repeating itself) the poll is a no-op and yields nothing,
        so the next poll re-enters with the same state. Otherwise every announced file is
        downloaded, verified, and unzipped, all rows are emitted, and the cursor advances to
        this slice — messages first, ``State`` last, one transaction per slice.
        """
        feed = config[FEED_NAME]
        pointer = POINTER_BY_FEED.get(feed)
        if pointer is None:
            raise ValueError(f"unknown feed {feed!r} — expected one of {sorted(POINTER_BY_FEED)}")

        entries = parse_pointer((await self._get(pointer)).text)
        if not entries:
            return  # feed briefly published an empty pointer — nothing to do
        file_ts = parse_gdelt_datetime(entries[0][2][:14])
        if file_ts is None:
            raise ValueError(f"{feed}: pointer names an unparseable slice {entries[0][2]!r}")
        cursor = state.get(FILE_TS)
        if cursor is not None and file_ts <= cursor:
            return  # already processed this slice (or an older/repeated one) — skip

        try:
            tables: dict[str, bytes] = {}
            for size, md5, filename in entries:
                table = table_of(filename)
                if table is not None:
                    tables[table] = unzip_single(await self._download_verified(size, md5, filename))
        except httpx.HTTPStatusError as exc:
            # A 404 means GDELT has *announced* the slice in the pointer but not yet published
            # all three files (the translation feed lags most). That is not a fault to let
            # crash — it is a not-ready-yet condition we can remedy by skipping this poll and
            # retrying: the cursor doesn't advance, so the next poll re-reads the pointer and
            # picks the slice up once it's complete (or moves on when the pointer advances).
            if exc.response.status_code == 404:
                log.info("%s slice %s not fully published yet (404) — retrying next poll", feed, file_ts.isoformat())
                return
            raise  # any other HTTP error (5xx, etc.) still crashes — let it crash
        fetched_at = datetime.now(timezone.utc)

        count = 0
        for message in build_page(tables, feed=feed, file_ts=file_ts, fetched_at=fetched_at, topics=self._topics):
            count += 1
            yield message
        log.info("%s slice %s: emitted %d rows across %s", feed, file_ts.isoformat(), count, sorted(tables))
        yield State({FILE_TS: file_ts})


stage = GdeltIngest()
"""The stage the dispatcher runs (``python -m examples.gdelt_news_stories ingest``).

Polls the live GDELT feed root by default; inject a client to point it at a stub.
"""

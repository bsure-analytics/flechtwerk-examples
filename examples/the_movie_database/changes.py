"""TMDB changes — an ``Extractor`` that backs up each feed's daily change feed, page by page.

Stage 1. A ``tmdb-feeds`` config record per feed (``movie`` | ``tv`` | ``person``) names what to
list and carries the TMDB access token **encrypted**. Each poll lists every calendar day from the
feed's cursor up to today — one TMDB window per day, paged to the end — and emits every page
**exactly as TMDB delivered it** to ``tmdb-changes-raw``, nested under ``RESPONSE`` beside a
``METADATA`` record of what was asked and when (feed, day, page, open/closed, the server
``Date``). Keyed by the feed name, so one feed's pages stay in order.

**An extractor is a backup, not a parser.** This stage never interprets a page: no ids are read,
nothing is filtered, deduped or reshaped, and no ``Attribute`` describes the body. Splitting a
page into per-entity change records, dropping ``adult`` ids and deduping the open day are
*transformations*, so they live in the ``split`` stage, where a replay of this topic can redo them
under new rules. The one field read out of a body is the envelope's ``total_pages`` — paging is
impossible without it, and it is about the *listing*, not the data.

**Why one calendar day per window.** TMDB caps a change window at 14 days and ``page`` at 500
(100 ids per page). Measured: one day of people is ~60 pages, but a 14-day people window is ~600 —
over the cap. A one-day window can never reach it, and it makes the day the natural unit of
progress.

**The cursor is the last closed day.** Days before today are *closed*: each is listed exactly
once, its pages marked ``closed``, and the cursor moves past it. Today is still accruing, so it is
*open*: every poll re-lists it in full and emits its pages marked ``open`` (``split`` emits only the
ids it hasn't seen). When midnight UTC passes, the first poll lists yesterday once more, now
``closed`` — its one closed listing. (TMDB keeps amending a past day's list for hours after
midnight, so an id it adds later is caught only at its next change — see the README's caveats.)
A backfill is all closed days, so each is listed exactly once.

**One ``State`` per day.** Every ``State`` yield is a commit boundary: a 90-day backfill is 90
transactions, a crash resumes at the last closed day, and no transaction outlives the framework's
10-minute limit (one day is at most 500 pages, a couple of minutes). The cursor is **monotonic**:
``since`` seeds it once; editing ``since`` later does not rewind it — re-listing means a fresh
``application_id`` or a deleted changelog.

**The wall clock decides what *today* is — and nothing else.** ``clock`` is injected (the tests
pin it); it is operational time, not event time: the day listed is TMDB's own UTC day, which was
measured to be exactly the UTC calendar day (per-entity change stamps never leave it).

**Let it crash.** A 5xx, a timeout or a 429 propagates; the supervisor restarts the stage and the
cursor resumes. The one named failure is a 401, whose remedy is the operator's
(``request-tmdb token``), so the message says so.

TMDB data — see the README's attribution.
"""
import logging
from collections.abc import AsyncIterator, Callable
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Record

from .attributes import (
    ACCESS_TOKEN,
    CHANGED_ON,
    CHANGES_RAW_TOPIC,
    CURSOR,
    DISCOVERED_AT,
    FEED,
    FEEDS_TOPIC,
    LISTING,
    METADATA,
    PAGE,
    RESPONSE,
    SINCE,
)
from .projection import DEFAULT_LOOKBACK_DAYS, MAX_PAGES, TMDB_BASE_URL, changes_path, day_windows

log = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def response_date(response: httpx.Response, fallback: Callable[[], datetime] = utc_now) -> datetime:
    """The server ``Date`` as aware UTC — the event-time clock. Server-controlled, so tests pin it
    with a ``Date`` response header; falls back to ``fallback()`` if it is absent."""
    raw = response.headers.get("Date")
    if raw:
        return parsedate_to_datetime(raw).astimezone(timezone.utc)
    return fallback()  # pragma: no cover — the live API always sends Date


def raise_for_status(response: httpx.Response) -> None:
    """``raise_for_status`` with the one failure an operator can fix made legible: a 401 says
    which command re-stores the token. Shared by the two stages that call TMDB (``changes``, ``catalog``)."""
    if response.status_code == 401:
        try:
            message = response.json().get("status_message", response.text)
        except ValueError:
            message = response.text
        raise RuntimeError(
            f"TMDB rejected the access token (401: {message}); "
            f"re-run: uv run poe request-tmdb token < <token file>")
    response.raise_for_status()


class TmdbChanges(Extractor):
    """Backs up each feed's daily change feed, page by page → ``tmdb-changes-raw``.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed in
    ``__aexit__``); tests inject a ``MockTransport`` client and a pinned clock. The credential is
    on the config record, so unlike ``wildfire_watch``'s ingest a module-level ``stage`` singleton
    is fine.
    """

    config_topics = [FEEDS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, base_url: str = TMDB_BASE_URL,
                 raw_topic: str = CHANGES_RAW_TOPIC, max_pages: int = MAX_PAGES,
                 clock: Callable[[], datetime] | None = None) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._topic = raw_topic
        self._max_pages = max_pages
        self._clock = clock or utc_now

    async def __aenter__(self) -> "TmdbChanges":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(60.0),
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def _page(self, token: str, feed: str, day: date, page: int) -> httpx.Response:
        assert self._client is not None, "client is opened in __aenter__ or injected"
        response = await self._client.get(
            self._base_url + changes_path(feed, day, page),
            headers={"Authorization": f"Bearer {token}"})
        raise_for_status(response)
        return response

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """List every day from the cursor to today; one ``State`` (one transaction) per day.

        Each page is yielded raw as soon as it arrives; the day's ``State`` follows its last page —
        the two-yield contract — so a crash replays only the uncommitted day, whose pages were
        aborted and are invisible downstream."""
        feed = config[FEED]
        token = config[ACCESS_TOKEN]  # decrypted by the codec on read
        today = self._clock().date()
        since = config.get(SINCE) or today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        cursor = state.get(CURSOR)
        first = cursor + timedelta(days=1) if cursor else since

        for day in day_windows(first, today):
            listing = "closed" if day < today else "open"
            page = 1
            while True:
                response = await self._page(token, feed, day, page)
                body = response.json()
                yield Message(key=feed, topic=self._topic, value=Event({
                    METADATA: Record({FEED: feed, CHANGED_ON: day, PAGE: page, LISTING: listing,
                                      DISCOVERED_AT: response_date(response, self._clock)}),
                    RESPONSE: Record.wrap(body),
                }))
                # The envelope's page count is the one thing read from a body: paging needs it.
                total_pages = int(body.get("total_pages") or 0)
                if page == 1 and total_pages > self._max_pages:
                    log.warning(
                        "%s %s: TMDB lists %d pages, capped at %d — ids beyond the cap are "
                        "caught on their next change", feed, day, total_pages, self._max_pages)
                if page >= min(total_pages, self._max_pages):
                    break
                page += 1
            log.info("%s %s: %d page(s) backed up (%s)", feed, day, page, listing)
            if listing == "closed":
                cursor = day
            yield State({CURSOR: cursor}) if cursor is not None else State()


stage = TmdbChanges()

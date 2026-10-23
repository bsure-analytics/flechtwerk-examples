"""TMDB changes — an ``Extractor`` that lists each feed's daily change feed.

Stage 1. A ``tmdb-feeds`` config record per feed (``movie`` | ``tv`` | ``person``) names what to
list and carries the TMDB access token **encrypted**. Each poll lists every calendar day from the
feed's cursor up to today — one TMDB window per day, paged to the end — and emits one
``tmdb-changes`` record per changed entity, keyed ``<feed>:<id>``. The catalog stage fetches the
details; this stage only answers "what changed, and on which day?".

**Why one calendar day per window.** TMDB caps a change window at 14 days and ``page`` at 500
(100 ids per page). Measured: one day of people is ~60 pages, but a 14-day people window is ~600 —
over the cap. A one-day window can never reach it, and it makes the day the natural unit of
progress.

**The cursor spectrum, fourth point.** The change feed is a *monotonic feed* — days resume — but
today's day is still accruing, so the honest state is two things: a ``CURSOR`` (the last **closed**
day fully listed) and a ``SEEN`` set holding the ids already emitted for the **open** day, so the
hourly re-list of today emits only the news. When midnight UTC passes, the day closes and gets
**one full re-list**: every id of the day is emitted again as ``closed``. That is deliberate and
cheap (an entity edited again after its hourly fetch is fetched once more, so the catalog's copy
of any day is final within one poll of midnight), and the catalog's content-hash gate turns a
no-op second fetch into nothing. A backfill is all closed days, so each is listed exactly once.

**One ``State`` per day.** Every ``State`` yield is a commit boundary: a 90-day backfill is 90
transactions, a crash resumes at the last closed day, and no transaction outlives the framework's
10-minute limit (one day is at most 500 pages, a couple of minutes). The cursor is **monotonic**:
``since`` seeds it once; editing ``since`` later does not rewind it — re-listing means a fresh
``application_id`` or a deleted changelog.

**The wall clock decides what *today* is — and nothing else.** ``clock`` is injected (the tests
pin it); it is operational time, not event time: ``changed_on`` is TMDB's own UTC day, which was
measured to be exactly the UTC calendar day (per-entity change stamps never leave it).

**Let it crash.** A 5xx, a timeout or a 429 propagates; the supervisor restarts the stage and the
cursor resumes. The one named failure is a 401, whose remedy is the operator's
(``request-tmdb token``), so the message says so. ``adult`` ids are dropped here and never reach
the catalog.

TMDB data — see the README's attribution.
"""
import logging
from collections.abc import AsyncIterator, Callable
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from .attributes import (
    ACCESS_TOKEN,
    CHANGED_ON,
    CHANGES_TOPIC,
    CURSOR,
    DISCOVERED_AT,
    FEED,
    FEEDS_TOPIC,
    LISTING,
    SEEN,
    SINCE,
    TMDB_ID,
)
from .projection import (
    DEFAULT_LOOKBACK_DAYS,
    MAX_PAGES,
    TMDB_BASE_URL,
    changes_path,
    day_windows,
    parse_changes_page,
    prune_seen,
)

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
    which command re-stores the token. Shared by both stages."""
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
    """Lists each feed's daily change feed → ``tmdb-changes``.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed in
    ``__aexit__``); tests inject a ``MockTransport`` client and a pinned clock. The credential is
    on the config record, so unlike ``wildfire_watch``'s ingest a module-level ``stage`` singleton
    is fine.
    """

    config_topics = [FEEDS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, base_url: str = TMDB_BASE_URL,
                 changes_topic: str = CHANGES_TOPIC, max_pages: int = MAX_PAGES,
                 clock: Callable[[], datetime] | None = None) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._topic = changes_topic
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

    async def _list_day(self, token: str, feed: str, day: date) -> tuple[list[int], int, datetime]:
        """Every (non-adult) id TMDB lists for one day, in order, plus the pages read and the
        server time of the first page."""
        ids: list[int] = []
        discovered_at: datetime | None = None
        page = 1
        while True:
            response = await self._page(token, feed, day, page)
            page_ids, total_pages = parse_changes_page(response.json())
            if discovered_at is None:
                discovered_at = response_date(response, self._clock)
                if total_pages > self._max_pages:
                    log.warning(
                        "%s %s: TMDB lists %d pages, capped at %d — ids beyond the cap are "
                        "caught on their next change", feed, day, total_pages, self._max_pages)
            ids += page_ids
            if page >= min(total_pages, self._max_pages):
                break
            page += 1
        return list(dict.fromkeys(ids)), page, discovered_at

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """List every day from the cursor to today; one ``State`` (one transaction) per day.

        Closed days are listed once in full; the open day emits only ids not already in ``SEEN``.
        Messages first, the State that accounts for them last — the two-yield contract — so a
        crash replays only the uncommitted day, and the replay is identical."""
        feed = config[FEED]
        token = config[ACCESS_TOKEN]  # decrypted by the codec on read
        today = self._clock().date()
        since = config.get(SINCE) or today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        cursor = state.get(CURSOR)
        first = cursor + timedelta(days=1) if cursor else since
        seen = {day: list(ids) for day, ids in (state.get(SEEN) or {}).items()}

        for day in day_windows(first, today):
            ids, pages, discovered_at = await self._list_day(token, feed, day)
            if day < today:
                listing, fresh = "closed", ids
                cursor = day
                seen.pop(day.isoformat(), None)
            else:
                listing = "open"
                known = set(seen.get(day.isoformat(), []))
                fresh = [tmdb_id for tmdb_id in ids if tmdb_id not in known]
                seen[day.isoformat()] = sorted(known | set(ids))
            for tmdb_id in fresh:
                yield Message(key=f"{feed}:{tmdb_id}", topic=self._topic, value=Event({
                    FEED: feed, TMDB_ID: tmdb_id, CHANGED_ON: day, LISTING: listing,
                    DISCOVERED_AT: discovered_at,
                }))
            seen = prune_seen(seen, keep_from=today)
            log.info("%s %s: %d page(s), %d id(s) listed, %d emitted (%s)",
                     feed, day, pages, len(ids), len(fresh), listing)
            new_state = State({SEEN: seen})
            if cursor is not None:
                new_state[CURSOR] = cursor
            yield new_state


stage = TmdbChanges()

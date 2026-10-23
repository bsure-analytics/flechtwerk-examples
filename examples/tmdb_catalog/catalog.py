"""TMDB catalog — a ``Transformer`` that fetches, projects and versions each changed entity.

Stage 3. Each ``tmdb-changes`` record names an entity that changed. The stage fetches its details
(with credits, keywords, ratings and external ids appended — one request), projects the response
to a flat row, and hashes it. If the hash equals the one in the entity's state, nothing is emitted
and nothing is written: the change was a no-op for us. Otherwise the entity gets its next
**version** — one flat row on the feed's topic and one row per credit on ``tmdb-credits`` — and
the state remembers the hash.

**Slowly changing dimensions behind a content-hash gate.** TMDB lists an id as changed for a
hundred reasons that do not alter what we keep (a translation, an image), and the open-day
re-listing plus the closing re-list make a repeat fetch routine. The hash gate is what makes both
free: versions are emitted when the *content* moved, never when the *listing* did.
``popularity`` is inside the hash on purpose — TMDB recomputes it daily, and one new version per
move is the popularity history.

**The state key is the wire key** (``movie:550``), so an entity's change records, its versions and
its credits all co-partition. The state itself is tiny: hash, version counter, last version time.

**Bounded fan-out.** The runner runs one ``transform`` per state key of a batch concurrently, so a
500-record batch would open 500 requests against a service with a ~40 requests-per-second ceiling.
A semaphore of ``concurrency`` (8) bounds the in-flight detail fetches; at ~0.25 s per fetch a
batch drains in ~16 s, far inside ``max.poll.interval``. (The bound is per catalog process, not per
feed.)

**Let it crash — with two remedied conditions.** A 5xx, a timeout or a 429 propagates; the
supervisor restarts the stage and the batch replays (exactly-once: the aborted outputs never
existed). Remedied: a **404** — the entity is gone, so its state is tombstoned and nothing is
emitted — and a change for a feed with **no config record any more**, which is warned about and
skipped (the remedy is ``request-tmdb token``). The token is read from the feed's config record,
decrypted by the codec on read.

TMDB data — see the README's attribution.
"""
import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import httpx
from flechtwerk import Event, IncomingMessage, Message, State, Transformer
from flechtwerk.attribute import Record

from .attributes import (
    ACCESS_TOKEN,
    CHANGED_ON,
    CHANGES_TOPIC,
    CONTENT_HASH,
    CREDITS_TOPIC,
    FEED,
    FEEDS_TOPIC,
    MEDIA_ID,
    MEDIA_TYPE,
    MOVIES_TOPIC,
    PEOPLE_TOPIC,
    TMDB_ID,
    TV_TOPIC,
    VERSION,
    VERSION_AT,
)
from .changes import raise_for_status, response_date
from .projection import PROJECTORS, TMDB_BASE_URL, content_hash, details_path

log = logging.getLogger(__name__)

CONCURRENCY = 8
"""In-flight detail fetches per catalog process — see the module docstring."""


class TmdbCatalog(Transformer):
    """Fetches each changed entity, projects it, and emits a new version when the content moved.

    Subclasses ``Transformer`` to own the ``httpx`` client and the concurrency semaphore; tests
    inject a ``MockTransport`` client. ``topics`` maps a feed to its output topic."""

    input_topics = [CHANGES_TOPIC]
    config_topics = [FEEDS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, base_url: str = TMDB_BASE_URL,
                 concurrency: int = CONCURRENCY, topics: dict[str, str] | None = None,
                 credits_topic: str = CREDITS_TOPIC) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._concurrency = concurrency
        self._topics = topics or {"movie": MOVIES_TOPIC, "tv": TV_TOPIC, "person": PEOPLE_TOPIC}
        self._credits_topic = credits_topic
        self._semaphore = asyncio.Semaphore(concurrency)

    async def __aenter__(self) -> "TmdbCatalog":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(60.0),
                limits=httpx.Limits(max_connections=self._concurrency),
            )
        self._semaphore = asyncio.Semaphore(self._concurrency)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def _details(self, token: str, feed: str, tmdb_id: int) -> httpx.Response:
        assert self._client is not None, "client is opened in __aenter__ or injected"
        async with self._semaphore:
            return await self._client.get(
                self._base_url + details_path(feed, tmdb_id),
                headers={"Authorization": f"Bearer {token}"})

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        if msg.value is None:
            return
        feed, tmdb_id, changed_on = msg.value[FEED], msg.value[TMDB_ID], msg.value[CHANGED_ON]
        feed_config = self.configs.get(feed)
        if feed_config is None:
            log.warning("%s: no config record for feed %r — skipped; re-request it with "
                        "`uv run poe request-tmdb token < <token file>`", msg.key, feed)
            return

        response = await self._details(feed_config[ACCESS_TOKEN], feed, tmdb_id)
        if response.status_code == 404:  # the entity is gone: forget it, emit nothing
            if state:
                log.info("%s: removed from TMDB — state dropped", msg.key)
                yield State()
            return
        raise_for_status(response)

        flat, credits = PROJECTORS[feed](response.json())
        digest = content_hash(flat, credits)
        if digest == state.get(CONTENT_HASH):
            return  # unchanged: no output, no state write
        version = (state.get(VERSION) or 0) + 1
        version_at = response_date(response, lambda: datetime.now(timezone.utc))

        yield Message(key=msg.key, topic=self._topics[feed], value=Event({
            **Record.wrap(flat), MEDIA_TYPE: feed, VERSION: version, VERSION_AT: version_at,
            CHANGED_ON: changed_on, CONTENT_HASH: digest,
        }))
        for credit in credits:
            yield Message(key=msg.key, topic=self._credits_topic, value=Event({
                **Record.wrap(credit), MEDIA_TYPE: feed, MEDIA_ID: tmdb_id, VERSION: version,
                VERSION_AT: version_at,
            }))
        yield State({CONTENT_HASH: digest, VERSION: version, VERSION_AT: version_at})


stage = TmdbCatalog()

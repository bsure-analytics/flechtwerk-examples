"""Tape ingest — an ``Extractor`` whose resume cursor is a byte offset.

Stage 1. One poll target per session on the compacted ``f1-sessions`` config topic. Each
poll range-reads the next chunk of every one of the session's feeds, frames whole lines,
merges them across feeds under a watermark, and emits each line as one record on
``f1-timing`` — event-timed against the ``t0`` anchor — before yielding the advanced
per-feed cursors. One transaction per poll per session.

**Why this source is the interesting one.** Every other extractor in this repo reads a
*service*: a snapshot to re-derive (ADS-B, Odds), a monotonic feed with a resume mark
(GDELT, SMARD), or a rolling window with no id at all (wildfire's seen-set). This one reads
a **file that is still being written**. That collapses three problems into one code path:

* *backfill* — read a finished file from byte 0;
* *live tailing* — read a growing file from the last consumed byte;
* *crash recovery* — read from the committed cursor, which is neither of the above and needs
  no code of its own.

The framework commits the cursor in the same transaction as the records it accounts for, so
"where am I in the source" and "what have I published" can never disagree. For a re-readable
byte stream that is genuine exactly-once, not an approximation of it.

**Downtime is lossless, and that is worth stating out loud.** The source is a file, not an
ephemeral socket. Stop the stage mid-race and restart it an hour later: the cursors resume
where they stopped, the missed hour is still in the file, it arrives with its true event
times, and the stage chews through the backlog — :data:`LINE_BUDGET` lines per poll — until
it catches the tail again. Start it fresh mid-race and it reads from byte 0 and
fast-forwards. An outage costs timeliness; it never costs data.

**The one thing this stage will not do is guess at time.** A tape whose ``t0`` cannot be
anchored is not emitted at all (see :meth:`TapeIngest._anchor`): records with invented
timestamps would poison the very property — honest event time — that makes replay and live
the same dashboard.

**Let it crash.** A timeout, a 5xx, a truncated body: there is one upstream and no remedy a
retry loop could apply, so the poll cadence plus the supervisor's restart *is* the recovery,
and the cursor makes the restart free. Exactly two conditions are handled rather than
raised, because both are ordinary rather than exceptional: a **404 on a configured session**
means "the weekend hasn't started yet" (log once, keep polling — the escape hatch that lets
you request a path before the index lists it), and a **416 on a range read** means "no new
bytes", which is what a caught-up tail looks like.

The endpoint is unofficial and undocumented — see the README's posture section. This stage
is read-only, sends an honest User-Agent, and pins ``Accept-Encoding: identity`` on every
request.
"""
import json
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

import httpx
from flechtwerk import Config, Event, Extractor, Message, State

from . import tape
from .attributes import (
    CHECKED_MS,
    CURSORS,
    DONE,
    EVENT_TIME,
    FEED,
    KIND,
    LENGTHS,
    MEETING,
    OFFSET_MS,
    PATH,
    PAYLOAD,
    PHASE,
    SEEN,
    SESSION,
    SESSION_KEY,
    SESSION_NAME,
    SESSIONS_TOPIC,
    START_LOCAL,
    GMT_OFFSET,
    T0_MS,
    TELEMETRY,
    TIMING_TOPIC,
    TYPES,
    YEAR,
)

log = logging.getLogger(__name__)

ARCHIVE_BASE_URL: Final = "https://livetiming.formula1.com/static"
"""The live-timing static archive. The demo constant; injectable for tests via
``TapeIngest(base_url=…)``."""

USER_AGENT: Final = "flechtwerk-examples/f1 (+github.com/bsure-analytics/flechtwerk-examples)"
"""An honest, attributable User-Agent. The endpoint is undocumented and unauthenticated;
identifying the client is the minimum courtesy owed to somebody else's CDN."""

SESSION_KIND: Final = "session"
"""``KIND`` of a config record naming one session to ingest. Keyed by its path."""

FOLLOW_KIND: Final = "follow"
"""``KIND`` of a config record asking the stage to watch a season's index and self-produce
session records as they appear. Keyed ``season-<year>``."""

FOLLOW_PREFIX: Final = "season-"
"""Wire-key prefix of a follow record — how :mod:`.request` names one and how ``retire``
finds it again."""

CHUNK_BYTES: Final = 512 * 1024
"""**Ceiling** on the bytes requested per feed per poll; the actual size is per feed and
self-tuning (see :meth:`TapeIngest._retune`).

Sized against what a poll must achieve rather than against what the network can do: at
:data:`LINE_BUDGET` lines per poll and ~107 bytes per average ``TimingData`` line, ~500 KB is
roughly one budget's worth of the busiest feed — so the budget is what paces a backfill and the
chunk is not the thing standing in the way."""

MIN_CHUNK_BYTES: Final = 32 * 1024
"""Floor on a self-tuned chunk. Small enough that a feed with one record per minute costs almost
nothing to poll, large enough that no realistic line is smaller than the request that must hold
it (which would hand the job to :meth:`TapeIngest._frame`'s widening path every poll)."""

CHUNK_HEADROOM: Final = 2
"""How much more than last poll's consumption a feed asks for.

The slack that lets a feed's throughput *rise* — consumption is set by the watermark, so a feed
that consumed its whole chunk must be allowed to fetch more next time. Two is the smallest value
that leaves genuine headroom; the price is that a converged backfill downloads ~1.6× the tape it
ingests (measured), since bytes fetched past the watermark are re-read once. Raising it wastes
bandwidth; lowering it toward 1 leaves a feed unable to accelerate."""

LINE_BUDGET: Final = 5_000
"""Lines emitted per poll per session, at most.

The real constraint is Kafka's 10-minute transaction timeout: one poll is one transaction, so
it must finish comfortably inside that, and an unbounded poll over a 7 MB feed would not.
Bounding lines rather than bytes is what makes the pacing predictable across feeds whose line
sizes differ by three orders of magnitude. A race's 93 051 lines (measured) therefore
backfill in 24 polls — minutes at any sane interval, and the interval is not what governs it."""

LIVE_LAG: Final = timedelta(seconds=30)
"""How far behind the wall clock a live poll draws its frontier.

While live, a feed that has been read to end-of-file has said "nothing up to now" — but
"now" must be a *safe* now: the archive is written by a recorder and served through a CDN,
so a line for 12:00:00 may only become readable a few seconds later. Drawing the frontier at
``now − 30 s`` keeps the merge from concluding that a quiet ``TrackStatus`` had nothing to
say at 12:00:00 when in truth the record simply had not landed yet. The cost is 30 s of
dashboard latency on top of the CDN's own; the alternative is silently mis-ordered flags.

**Thirty seconds is a guess at the CDN's delivery delay, and it is the number most likely to be
wrong.** The archive is CloudFront over S3, and every *finalised* object is served with
``Cache-Control: max-age=3600`` — one measured with ``x-cache: Hit`` and ``age: 3430``, i.e. 57
minutes stale and no revalidation. If a growing file is cached the same way, the edge's staleness
is the real frontier and no margin set here can compensate; if the origin serves live files with a
short TTL (which the official client's own late-joiner back-fill needs), 30 s is generous. Nobody
has measured it on a live session — see the README's live section for the probe. Raise this if a
live tail shows flags landing out of order; it costs latency, not correctness."""

FOLLOW_INTERVAL: Final = timedelta(minutes=5)
"""How often a ``follow`` target re-reads its season index.

The index gains a session at most a few times a weekend, so anything faster is pure noise
against somebody else's CDN. Every *other* poll of the follow target returns immediately
having sent nothing — which is why the timestamp lives in state (:data:`~.attributes.CHECKED_MS`)
rather than in memory: a restart must not restart the clock."""

WISH_LIST: Final = (
    "SessionInfo", "SessionStatus", "TrackStatus", "RaceControlMessages", "TimingData",
    "TimingAppData", "DriverList", "LapCount", "ExtrapolatedClock", "WeatherData",
    "Heartbeat", "PitStopSeries", "OvertakeSeries", "ChampionshipPrediction",
)
"""The 14 feeds this example ingests, **intersected with each session's own index**.

A race publishes 33 feeds and a qualifying session 27; the six a race adds (``LapCount``,
``PitStopSeries``, ``OvertakeSeries``, ``ChampionshipPrediction``, ``PitStop``,
``DriverRaceInfo``) simply do not exist elsewhere. Asking for an absent feed would 404 on
every poll forever, so absence is normal and silent — and it is why the wish list is a
*wish*, checked against the index rather than assumed.

Deliberately **not** ingested, each for a stated reason — because 33 feeds is more than the
example can teach with, and a feed that adds bytes without adding a lesson is noise:

* ``TimingDataF1`` — a near-duplicate of ``TimingData``. The key *sets* are identical; over
  one matched mid-race window ``TimingData`` carried ~20× more gap/interval updates for the
  same wall time. Ingesting the richer one loses nothing and halves the biggest feed.
* ``TimingStats`` (260 KB) and ``TyreStintSeries`` (67 KB) — derivable. ``TimingData`` already
  carries each driver's best lap, their sector values with per-lap personal/overall-fastest
  flags, and every speed-trap reading; ``TimingAppData`` already carries the stints.
* ``DriverRaceInfo``, ``TopThree``, ``DriverTracker``, ``LapSeries``, ``TlaRcm``,
  ``WeatherDataSeries``, ``CurrentTyres`` — restatements or projections of feeds above.
* ``PitStop``, ``PitLaneTimeCollection`` — restate ``PitStopSeries``, which additionally
  carries each stop's own absolute ``Timestamp``.
* ``TeamRadio``, ``AudioStreams``, ``ContentStreams`` — media, not timing.
* ``SessionData`` — qualifying-segment (Q1/Q2/Q3) detail that only a segment-aware board could
  use; the leaderboard works generically off position patches instead."""

TELEMETRY_FEEDS: Final = ("CarData.z", "Position.z")
"""The two high-rate ``.z`` feeds, fetched only when the session config says
:data:`~.attributes.TELEMETRY`. ~14 MB of a race's ~28 MB, and the only feeds worth making
optional; the flag is honoured **here**, so nothing downstream needs to know about it."""

ANCHOR_FEEDS: Final = ("Heartbeat", "ExtrapolatedClock", "RaceControlMessages", "CarData.z")
"""Feeds that can supply the ``t0`` anchor, best first.

``Heartbeat`` wins because it is present in every session, beats every ~15 s, and its payload
is exactly ``{"Utc": …}`` — so anchoring costs one small read and the spread check has plenty
of samples. The fallbacks exist for a session that somehow lacks it, in descending order of
how tightly their inner clock tracks their recording offset (measured: ``ExtrapolatedClock``
implies the same ``t0`` to the millisecond across an hour; ``CarData``'s sample clock runs ~3 s
ahead of its line, because a telemetry sample is generated before the line carrying it is
recorded)."""

ARCHIVE_STATUS_FEED: Final = "ArchiveStatus"
"""The completion probe — a 24-byte **keyframe**, never streamed.

The distinction is the archive's sharpest trap: ``ArchiveStatus.json`` holds ``Complete``
once a session is finished, while ``ArchiveStatus.jsonStream`` — being a *recording* of what
the live feed said at the time — says ``Generating`` forever. Every keyframe in the archive
is the FINAL state rather than the initial one, which is exactly why the replay path never
reads keyframes for data: it would start from the end."""

COMPLETE_STATUS: Final = "Complete"
"""``ArchiveStatus.json``'s value once a session's recording is finished. Anything else
(``Generating``) means the files may still grow."""

ARCHIVE_PHASE: Final = "archive"
""":data:`~.attributes.PHASE` for a finished recording: a feed read to end-of-file is
*exhausted* and drops out of the watermark."""

LIVE_PHASE: Final = "live"
"""``PHASE`` for a recording still being written: a feed read to end-of-file is merely
*caught up*, and its frontier becomes the wall-derived ``now − LIVE_LAG``."""

DEFAULT_TYPES: Final = ("Race", "Sprint", "Qualifying", "Sprint Qualifying")
"""Which :data:`~.attributes.SESSION_NAME` values ``request-f1 season`` and a ``follow``
record take by default — the four competitive sessions. The exact strings the year index
publishes (verified across a full season index: also ``Practice 1``…``3`` and, for pre-season
testing, ``Day 1``…``3``), matched case-insensitively. Practice and testing are opt-in
because they are three quarters of the sessions and almost none of the interest."""


@dataclass(frozen=True, slots=True)
class Read:
    """One feed's HTTP read, reduced to what framing needs: bytes, position, and length."""
    chunk: bytes
    start: int
    length: int | None
    at_eof: bool


class TapeIngest(Extractor):
    """Reads (or tails) the live-timing tape for every configured session → ``f1-timing``.

    Subclasses ``Extractor`` to own the shared ``httpx`` client (built in ``__aenter__``,
    closed in ``__aexit__``); tests inject a ``MockTransport`` client, so no network is
    touched off the live path. ``chunk_bytes`` and ``line_budget`` are injectable for the
    same reason — the watermark's clamping behaviour is only observable when a chunk is
    small enough to run out mid-tape.
    """

    config_topics = [SESSIONS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 base_url: str = ARCHIVE_BASE_URL, timing_topic: str = TIMING_TOPIC,
                 sessions_topic: str = SESSIONS_TOPIC, chunk_bytes: int = CHUNK_BYTES,
                 line_budget: int = LINE_BUDGET) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._timing_topic = timing_topic
        self._sessions_topic = sessions_topic
        self._chunk_bytes = chunk_bytes
        self._line_budget = line_budget
        # Per-session in-memory caches. Neither is state: the feed list is a property of the
        # archive (re-derivable at zero cost on restart), and the chunk widening is a
        # transient reaction to one pathological line. Keeping them out of state keeps the
        # changelog record to what it must contain — the cursor.
        self._feeds: dict[tuple[str, bool], tuple[str, ...]] = {}
        self._chunks: dict[tuple[str, str], int] = {}
        self._pending: set[str] = set()

    async def __aenter__(self) -> "TapeIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(60.0), follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    async def enrich_config(self, config: Config) -> Config:
        """Fill in what a *hand-written* config record may omit — no I/O, once per record.

        ``request.py`` writes complete records, so in practice this is the identity. What it
        exists for is the minimal record somebody produces straight to the topic from Kafbat:
        ``{"path": "2026/…/2026-07-26_Race/"}`` is a perfectly clear request, and the season
        and the kind are both *derivable from the path itself* — so deriving them is better
        than rejecting the record. Spreading (``{**config, …}``) enriches without mutating.
        """
        if config.get(KIND) == FOLLOW_KIND:
            return config
        filled = Config({**config, KIND: SESSION_KIND})
        if filled.get(YEAR) is None:
            filled[YEAR] = int(filled[PATH].split("/", 1)[0])
        return filled

    # --- polling ---

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Dispatch on the config's kind: one tape target, or the season-follow target."""
        if config.get(KIND) == FOLLOW_KIND:
            async for item in self._poll_follow(config, state):
                yield item
            return
        async for item in self._poll_session(config, state):
            yield item

    async def _poll_session(self, config: Config,
                            state: State) -> AsyncIterator[Message | State]:
        """One session's next slice of tape: read, merge, emit, advance the cursors.

        The order is the two-yield contract — every ``Message`` first, the ``State`` that
        accounts for them last — so the whole poll is one transaction. A crash leaves the
        cursors unadvanced and the aborted page invisible, and the re-poll re-reads exactly
        the same bytes and re-derives exactly the same records.
        """
        path = config[PATH]
        if state.get(DONE):
            return  # terminal: not one request, for the rest of the process's life

        feeds = await self._session_feeds(path, config)
        if feeds is None:
            return  # the session has no index yet — see _session_feeds

        phase = state.get(PHASE)
        if phase != ARCHIVE_PHASE:  # unknown, or live and possibly finished by now
            phase = ARCHIVE_PHASE if await self._is_complete(path) else LIVE_PHASE

        cursors = dict(state.get(CURSORS) or {})
        lengths = dict(state.get(LENGTHS) or {})
        t0_ms = state.get(T0_MS)

        if t0_ms is None:
            async for item in self._anchor(path, feeds, cursors, lengths, phase):
                yield item
            return

        reads = {feed: await self._read(path, feed, cursors.get(feed, 0), phase)
                 for feed in feeds}
        for feed, read in reads.items():
            if read.length is not None:
                lengths[feed] = read.length

        fetches = {feed: self._frame(path, feed, read, phase) for feed, read in reads.items()}
        live_frontier = (None if phase == ARCHIVE_PHASE
                         else tape.session_offset_ms(t0_ms, _now()) - int(LIVE_LAG.total_seconds() * 1000))
        bound = tape.watermark(tape.frontier(fetch, live_frontier_ms=live_frontier)
                               for fetch in fetches.values())
        emitted = tape.merge(fetches.values(), bound=bound, limit=self._line_budget)

        for line in emitted:
            yield Message(key=path, topic=self._timing_topic,
                          value=_tape_record(path, line, t0_ms),
                          timestamp=tape.event_time(t0_ms, line.offset_ms))
        advanced = tape.cursors_after(fetches.values(), emitted)
        for feed, read in reads.items():
            self._retune(path, feed, consumed=advanced.get(feed, read.start) - read.start,
                         framed=bool(fetches[feed].lines))
        cursors.update(advanced)

        done = phase == ARCHIVE_PHASE and all(
            feed in lengths and cursors.get(feed, 0) >= lengths[feed] for feed in feeds)
        if emitted or done:
            log.info("%s: emitted %d line(s) up to %s (%s phase, %d/%d feed(s) consumed)%s",
                     path, len(emitted),
                     tape.event_time(t0_ms, emitted[-1].offset_ms).isoformat() if emitted else "—",
                     phase,
                     sum(1 for feed in feeds if cursors.get(feed, 0) >= lengths.get(feed, 1 << 62)),
                     len(feeds), " — COMPLETE" if done else "")
        yield State({PHASE: phase, T0_MS: t0_ms, CURSORS: cursors, LENGTHS: lengths,
                     **({DONE: True} if done else {})})

    async def _anchor(self, path: str, feeds: Sequence[str], cursors: dict[str, int],
                      lengths: dict[str, int], phase: str) -> AsyncIterator[Message | State]:
        """Derive ``t0`` and persist it — **as a peek**, emitting nothing and moving no cursor.

        The anchor comes from a line that will itself be emitted later, so this poll must not
        consume it: it reads one anchor feed's first chunk, computes ``t0``, and yields only
        the state. The next poll's ordinary merge then reads that same line back and emits it
        exactly once, like any other. The alternative — emitting during the anchor poll —
        would need a second code path for "the anchor line" and would emit it before ``t0``
        was known, which is precisely the thing that cannot be done.

        ``t0`` must be **persisted** rather than recomputed, because once the merge has passed
        the anchor line it is behind the cursor and unreachable without a rewind.

        A session offering no anchor-capable feed is marked ``done`` with an error: a tape that
        cannot be placed in time must not be emitted at all.
        """
        anchor_feed = next((feed for feed in ANCHOR_FEEDS if feed in feeds), None)
        if anchor_feed is None:
            log.error("%s: no anchor-capable feed among %s — refusing to emit a tape that "
                      "cannot be placed in absolute time; marking the session done",
                      path, ", ".join(feeds))
            yield State({PHASE: phase, DONE: True})
            return

        read = await self._read(path, anchor_feed, cursors.get(anchor_feed, 0), phase)
        if read.length is not None:
            lengths[anchor_feed] = read.length
        fetch = self._frame(path, anchor_feed, read, phase)
        t0_ms = tape.anchor(fetch.lines)
        if t0_ms is None:
            log.info("%s: no anchor line in %s yet (%d byte(s) read) — retrying next poll",
                     path, anchor_feed, len(read.chunk))
            return  # idempotent: nothing emitted, nothing persisted, same read next time
        log.info("%s: anchored t0 = %s from %s (%s phase)",
                 path, tape.event_time(t0_ms, 0).isoformat(), anchor_feed, phase)
        yield State({PHASE: phase, T0_MS: t0_ms, CURSORS: cursors, LENGTHS: lengths})

    def _frame(self, path: str, feed: str, read: Read, phase: str) -> tape.Fetch:
        """Frame one read, and widen this feed's chunk if not a single line fitted.

        A line longer than the chunk has never been observed (the largest is a ~20 KB
        ``TimingData`` keyframe against a 512 KB chunk), but if one existed the feed would
        otherwise never advance a byte. Doubling is transient and in-memory: it is a reaction to
        one pathological line, not a fact about the session worth committing to the changelog.
        """
        fetch = tape.frame(read.chunk, feed=feed, start=read.start, at_eof=read.at_eof,
                           final=read.at_eof and phase == ARCHIVE_PHASE)
        if not fetch.lines and not fetch.at_eof:
            widened = self._chunk_size(path, feed) * 2
            self._chunks[(path, feed)] = widened
            log.warning("%s/%s: no complete line in %d byte(s) from offset %d — widening the "
                        "chunk to %d for the next poll", path, feed, len(read.chunk),
                        read.start, widened)
        return fetch

    def _retune(self, path: str, feed: str, *, consumed: int, framed: bool) -> None:
        """Size this feed's next chunk from what this poll actually consumed.

        **Why a fixed chunk is the wrong answer, measured.** The watermark decides how much
        *tape* a poll may emit, and the feeds run at byte rates three orders of magnitude apart:
        512 KB is six minutes of ``TimingData`` but the whole of ``TrackStatus`` and twelve
        minutes of ``CarData.z``. So a uniform chunk means most feeds fetch far past the
        watermark, have the surplus held back, and **re-fetch it next poll** — on a real race
        that measured **~7×** more bytes downloaded than the tape contains. Politeness aside,
        that is 150 MB to ingest a 21 MB session.

        Nothing needs to be cached to fix it: each feed simply asks for
        :data:`CHUNK_HEADROOM` × what it got through last time, clamped to
        ``[MIN_CHUNK_BYTES, chunk_bytes]``. Consumption is set by the watermark rather than by
        the chunk, so the loop is stable in both directions — a feed that under-fetches consumes
        its whole chunk and doubles, a feed that over-fetches shrinks toward the shared pace —
        and it converges in two or three polls.

        Measured on the full 2026 Hungarian Grand Prix race: **34.8 MB downloaded for a 21.4 MB
        tape — 1.63×** — over 24 polls and 371 requests, for 93 051 lines.

        Skipped when the read framed no line at all, because :meth:`_frame` is meanwhile
        *widening* that feed and the two must not fight.
        """
        if not framed:
            return
        # The floor is itself capped by the ceiling, so an injected chunk size smaller than
        # MIN_CHUNK_BYTES (the tests use one) still tunes instead of silently pinning to the top.
        floor = min(MIN_CHUNK_BYTES, self._chunk_bytes)
        size = min(self._chunk_bytes, max(floor, consumed * CHUNK_HEADROOM))
        if size >= self._chunk_bytes:
            self._chunks.pop((path, feed), None)
        else:
            self._chunks[(path, feed)] = size

    def _chunk_size(self, path: str, feed: str) -> int:
        return self._chunks.get((path, feed), self._chunk_bytes)

    async def _poll_follow(self, config: Config,
                           state: State) -> AsyncIterator[Message | State]:
        """Watch a season's index and self-produce a config record per newly-listed session.

        **An extractor producing onto its own config topic**, which the framework supports by
        construction and not by accident: config topics are consumed group-less and
        ``read_committed``, so the record this poll writes inside its transaction is picked up
        by the same process's next config drain — and by every other instance's — once it
        commits. Nothing here is special-cased; it is an ordinary ``Message`` with the config
        topic as its destination.

        That is what makes a live weekend unattended: seed one follow record and the sessions
        appear as the archive lists them. Each is written **exactly once**, tracked by the
        ``seen`` list in this target's own state.
        """
        now_ms = int(_now().timestamp() * 1000)
        elapsed = now_ms - (state.get(CHECKED_MS) or 0)
        if elapsed < FOLLOW_INTERVAL.total_seconds() * 1000:
            return  # most polls: nothing emitted, no state change, no request

        year = config[YEAR]
        wanted = {name.casefold() for name in (config.get(TYPES) or DEFAULT_TYPES)}
        telemetry = bool(config.get(TELEMETRY))
        seen = list(state.get(SEEN) or [])
        known = set(seen)

        fresh = [session for session in await self._year_sessions(year)
                 if (session.get("Name") or "").casefold() in wanted
                 and session["Path"] not in known]
        for session in fresh:
            path = session["Path"]
            seen.append(path)
            log.info("follow %d: picked up %r — %s", year, path, session.get("Name"))
            yield Message(key=path, topic=self._sessions_topic,
                          value=Event(session_config(session, year, telemetry=telemetry)))
        if fresh:
            log.info("follow %d: %d new session(s) requested, %d known", year, len(fresh), len(seen))
        yield State({SEEN: seen, CHECKED_MS: now_ms})

    # --- HTTP ---

    async def _read(self, path: str, feed: str, cursor: int, phase: str) -> Read:
        """One ranged GET of one feed's stream file, from ``cursor``.

        ``Accept-Encoding: identity`` is **mandatory, not tidiness**: transparent gzip would
        make the bytes the server counts differ from the bytes we count, and every cursor in
        state would silently point at the wrong place in the file.

        Three answers are normal. ``206`` carries new bytes and, in ``Content-Range``, the
        file's total length — which is how completion is detected without ever issuing a HEAD.
        ``416`` means the range starts at or past the end: no new bytes, and the length is
        exactly the cursor (we only ever advanced past bytes we actually read). ``200`` means
        the server ignored the range and sent the whole file, so the prefix already consumed
        is skipped locally.
        """
        assert self._client is not None, "client is opened in __aenter__ or injected"
        size = self._chunk_size(path, feed)
        response = await self._client.get(
            f"{self._base_url}/{path}{feed}.jsonStream",
            headers={"Range": f"bytes={cursor}-{cursor + size - 1}",
                     "Accept-Encoding": "identity"},
        )
        if response.status_code == 416:
            return Read(chunk=b"", start=cursor, length=cursor, at_eof=True)
        response.raise_for_status()
        if response.status_code == 200:  # range ignored — the whole file arrived
            body = response.content
            return Read(chunk=body[cursor:], start=cursor, length=len(body), at_eof=True)
        length = _content_range_length(response.headers.get("Content-Range"))
        end = cursor + len(response.content)
        return Read(chunk=response.content, start=cursor, length=length,
                    at_eof=length is not None and end >= length)

    async def _session_feeds(self, path: str, config: Config) -> tuple[str, ...] | None:
        """The session's feeds — its own index intersected with the wish list — or ``None``.

        ``None`` means the session directory does not exist yet, which is the *expected*
        answer for a session requested before the weekend: logged once per path, then silent,
        and retried on every poll. Everything else raises.

        Cached in memory: the archive publishes a session's whole feed index up front, so
        re-reading it every poll would be a request per poll per session for a value that never
        changes. The cache key includes the ``telemetry`` flag, so flipping it on a config record
        takes effect on the next poll rather than on the next restart.
        """
        key = (path, bool(config.get(TELEMETRY)))
        if (cached := self._feeds.get(key)) is not None:
            return cached
        assert self._client is not None, "client is opened in __aenter__ or injected"
        response = await self._client.get(f"{self._base_url}/{path}Index.json",
                                          headers={"Accept-Encoding": "identity"})
        if response.status_code == 404:
            if path not in self._pending:
                self._pending.add(path)
                log.info("%s: no index yet — treating it as a session that has not started; "
                         "polling on", path)
            return None
        response.raise_for_status()
        self._pending.discard(path)
        published = set(_json(response).get("Feeds") or {})
        wanted = WISH_LIST + (TELEMETRY_FEEDS if config.get(TELEMETRY) else ())
        feeds = tuple(feed for feed in wanted if feed in published)
        missing = [feed for feed in wanted if feed not in published]
        log.info("%s: %d of %d wished feed(s) published%s", path, len(feeds), len(wanted),
                 f"; absent: {', '.join(missing)}" if missing else "")
        self._feeds[key] = feeds
        return feeds

    async def _is_complete(self, path: str) -> bool:
        """Whether the recording is finished, from the ``ArchiveStatus`` **keyframe**.

        Deliberately the ``.json`` keyframe rather than the ``.jsonStream``: the stream is a
        recording of what the live feed said at the time and therefore reports ``Generating``
        for eternity, while the keyframe holds the current — final — value. 24 bytes, once
        per poll for as long as the session is live, and never again after it flips.
        """
        assert self._client is not None, "client is opened in __aenter__ or injected"
        response = await self._client.get(
            f"{self._base_url}/{path}{ARCHIVE_STATUS_FEED}.json",
            headers={"Accept-Encoding": "identity"})
        response.raise_for_status()
        return _json(response).get("Status") == COMPLETE_STATUS

    async def _year_sessions(self, year: int) -> list[dict[str, Any]]:
        """Every session the season's index lists, flattened, with its meeting's name attached.

        The index nests sessions under meetings and only the meeting knows the Grand Prix's
        name, so it is copied onto each session here rather than threaded through the caller.
        """
        assert self._client is not None, "client is opened in __aenter__ or injected"
        response = await self._client.get(f"{self._base_url}/{year}/Index.json",
                                          headers={"Accept-Encoding": "identity"})
        response.raise_for_status()
        return [{**session, "MeetingName": meeting.get("Name")}
                for meeting in _json(response).get("Meetings") or []
                for session in meeting.get("Sessions") or []]


def session_config(session: dict[str, Any], year: int, *, telemetry: bool) -> dict[Any, Any]:
    """One index entry as a ``f1-sessions`` config record — pure.

    Shared by :mod:`.request` (which writes them from the command line) and the follow target
    (which writes them from the index), so the two can never drift into producing subtly
    different records for the same session.
    """
    record: dict[Any, Any] = {
        KIND: SESSION_KIND,
        PATH: session["Path"],
        YEAR: year,
        TELEMETRY: telemetry,
    }
    for attribute, key in ((SESSION_KEY, "Key"), (MEETING, "MeetingName"),
                           (SESSION_NAME, "Name"), (START_LOCAL, "StartDate"),
                           (GMT_OFFSET, "GmtOffset")):
        if (value := session.get(key)) is not None:
            record[attribute] = value
    return record


def _tape_record(path: str, line: tape.Line, t0_ms: int) -> Event:
    """One framed line as a ``f1-timing`` record — the envelope plus the payload, whole.

    The payload is written straight into ``.raw`` under :data:`~.attributes.PAYLOAD` rather
    than declared as an attribute: it is a foreign, per-feed schema that must survive
    round-tripping untouched, and nesting it keeps a feed's ``Status`` or ``Utc`` from
    colliding with the envelope's own fields.
    """
    record = Event({
        SESSION: path,
        FEED: line.feed,
        OFFSET_MS: line.offset_ms,
        EVENT_TIME: tape.event_time(t0_ms, line.offset_ms),
    })
    record.raw[PAYLOAD] = line.payload
    return record


def _content_range_length(header: str | None) -> int | None:
    """The total length out of ``Content-Range: bytes 0-262143/7278940`` — ``None`` if absent
    or unparseable (a ``*`` total, which the spec allows and this CDN never sends)."""
    if not header or "/" not in header:
        return None
    total = header.rsplit("/", 1)[1].strip()
    return int(total) if total.isdigit() else None


def _json(response: httpx.Response) -> dict[str, Any]:
    """A keyframe or index response as a dict — **BOM-tolerant**.

    Every JSON file in the archive is UTF-8 *with* a byte-order mark, which ``json.loads``
    rejects outright and ``httpx.Response.json()`` inherits; decoding as ``utf-8-sig`` is the
    one line that makes the whole archive readable.
    """
    value = json.loads(response.content.decode("utf-8-sig"))
    return value if isinstance(value, dict) else {}


def _now() -> datetime:
    """The wall clock, in exactly two places: the live frontier and the follow cadence.

    Factored out so both are visible at a glance. The *archive* path never calls it, which is
    what keeps a backfill perfectly reproducible and the logic tier able to drive every branch.
    """
    return datetime.now(timezone.utc)


stage = TapeIngest()
"""The stage the dispatcher runs (``python -m examples.f1_live_timing ingest``).

Unlike wildfire's ingest this *can* be a module-level singleton: the endpoint needs no
credential, so nothing has to be injected at construction time.
"""

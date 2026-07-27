"""The tape core — framing, decoding, anchoring, and the cross-feed watermark merge.

Pure, I/O-free, and the logic tier's playground. Everything here operates on **bytes and
integers**: a chunk of a ``.jsonStream`` file plus the byte offset it starts at, in and one
ordered list of :class:`Line` out. Nothing in this module knows what HTTP is, which is why
`tests/logic_test.py` can drive the whole framing-and-merge machinery — including the
awkward cases (a chunk that splits a line, a feed that runs out mid-merge, a live tail) —
without a transport.

**What the tape is.** Formula 1's live-timing archive publishes, for every session, a
directory of ``<Feed>.jsonStream`` files. Each is an append-only *recording of the live
feed*: CRLF-separated lines, every line a fixed 12-character ``HH:MM:SS.mmm`` offset
followed immediately by one JSON value. The offsets run from when the **recorder** started
— roughly 50 minutes before lights-out — not from when the session did.

**Why byte offsets are the whole idea.** Because the file is append-only and an HTTP
``Range`` read of a byte interval is exactly reproducible, "where am I in this source?" has
an honest integer answer. That single fact collapses three problems into one code path:
reading a finished file from byte 0 is *backfill*, reading a growing file from the last
consumed byte is *live tailing*, and reading from a committed cursor after a crash is
*recovery*. The framework commits that cursor in the same transaction as the records it
accounts for, so the source position and the output are never out of step.

**Framing consumes only whole lines.** A chunk almost always ends mid-line; the remainder is
simply left for the next poll, so the cursor never lands inside a line and no carry-buffer
has to survive in state. The one exception is the end of a finished file, where an
unterminated final line is genuine content — hence the ``final`` flag on :func:`frame`.

**Why a watermark, and not just "emit what you fetched".** The board downstream joins across
feeds: it tags every leaderboard row and every lap with the flag state from ``TrackStatus``,
a feed with *twelve records per race*. If ``TimingData``'s fast-moving lines were emitted
past the point ``TrackStatus`` has been read to, laps would be tagged with a stale flag —
and it would look like a bug in the join rather than in the ordering. So each feed reports a
**frontier** (the offset up to which it is known to be complete) and only lines at or below
the minimum frontier are emitted; the rest wait for the next poll. See :func:`watermark`.
"""
import base64
import json
import logging
import re
import zlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from heapq import merge as heap_merge
from typing import Any, Final

log = logging.getLogger(__name__)

OFFSET_WIDTH: Final = 12
"""Bytes of the timestamp prefix on every line: ``HH:MM:SS.mmm``, with **no delimiter**
before the JSON that follows. A fixed width rather than a separator search, because the JSON
itself is full of colons and dots — the framing is positional by design."""

OFFSET_PATTERN: Final = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{3})$")
"""What a valid offset prefix looks like. Hours are two digits and never overflow in
practice (the longest observed tape, a testing day, runs ~5 h), but a session that somehow
passed 99 h would fail this check and have its lines skipped rather than mis-parsed."""

SEPARATOR: Final = b"\r\n"
"""Line separator. CRLF, not LF — splitting on ``\\n`` alone leaves a stray ``\\r`` on the
end of every payload, which JSON tolerates and then quietly poisons any string comparison."""

BOM: Final = b"\xef\xbb\xbf"
"""UTF-8 byte-order mark. **Every file in the archive starts with one**, and it counts toward
the byte offsets — so it is stripped only when reading from offset 0, and the three bytes it
occupies are consumed. Decoding a mid-file chunk as ``utf-8-sig`` instead would be a no-op;
decoding the *first* chunk without stripping it would put a BOM inside the first offset
prefix and fail every line of every feed."""

COMPRESSED_SUFFIX: Final = ".z"
"""Feeds whose name ends in this (``CarData.z``, ``Position.z``) carry a JSON **string**
rather than an object: base64 of a raw DEFLATE stream. See :func:`inflate`."""

MAX_LINE_BYTES: Final = 900_000
"""Longest line this module will decode; anything larger is skipped with a warning.

Defensive, not observed: the largest real line is a ``TimingData`` keyframe at ~20 KB, and
nothing in the archive comes within an order of magnitude of Kafka's default ~1 MB
``max.message.bytes``. But a single oversized line would be a **deterministic poison pill** —
the produce would fail, the stage would crash, the restart would re-read the same line, and
the season would stop advancing. Skipping one line loses one patch; crashlooping loses the
season."""

BLOCKED: Final = -1
"""Frontier reported by a feed whose chunk contained no complete line at all (a line longer
than the chunk). Below every real offset, so :func:`watermark` naturally clamps emission to
nothing this poll and the caller retries with a bigger chunk — no special case anywhere."""


def parse_offset(prefix: str) -> int | None:
    """``"01:23:45.678"`` → milliseconds (5025678), or ``None`` if it is not an offset — pure.

    ``None`` rather than an exception because a malformed prefix is a line-level fault the
    caller skips past, not a reason to abandon the tape.
    """
    match = OFFSET_PATTERN.match(prefix)
    if match is None:
        return None
    hours, minutes, seconds, millis = (int(group) for group in match.groups())
    return ((hours * 60 + minutes) * 60 + seconds) * 1000 + millis


def inflate(encoded: str) -> Any:
    """Decode a ``.z`` feed's payload: base64 → **raw** DEFLATE → JSON — pure.

    ``-zlib.MAX_WBITS`` is the whole trick: the payload is a bare DEFLATE stream with no zlib
    or gzip header, so the default window-bits (which expect a header) raise
    ``zlib.error: incorrect header check``. The negative value says "raw".

    Inflation happens **here, at ingest**, so the payload reaches Kafka as ordinary JSON: no
    consumer of ``f1-timing`` — not the board, not ClickHouse, not a curious human in Kafbat
    — ever needs zlib to read the tape.
    """
    return json.loads(zlib.decompress(base64.b64decode(encoded), -zlib.MAX_WBITS))


@dataclass(frozen=True, slots=True)
class Line:
    """One framed, decoded line of one feed.

    ``end`` is the absolute byte offset **just past** this line's separator — i.e. exactly
    the cursor value that would resume immediately after it. Carrying it per line is what
    lets the merge advance a feed's cursor past the lines it actually emitted while leaving
    the fetched-but-unemitted remainder to be re-read next poll.
    """
    feed: str
    offset_ms: int
    end: int
    payload: Any


@dataclass(frozen=True, slots=True)
class Fetch:
    """One feed's worth of a single poll's read: its framed lines and where they end.

    ``tail`` is the cursor to use when *every* line was emitted — which is not simply the
    last line's ``end``, because a skipped malformed line or an unterminated final line may
    sit past it. ``at_eof`` says the chunk reached the file's current end, which is what
    turns into "exhausted" in the archive phase and "caught up with the tail" while live.
    """
    feed: str
    lines: Sequence[Line]
    tail: int
    at_eof: bool


def frame(chunk: bytes, *, feed: str, start: int, at_eof: bool = False,
          final: bool = False) -> Fetch:
    """Frame one chunk of one feed into whole lines — pure, byte-exact.

    ``start`` is the chunk's absolute byte offset in the file. The two flags are the phase
    difference in miniature: ``at_eof`` says this chunk read to the file's **current** end,
    which is all a live tail can ever know, while ``final`` says the file is **finished**, so
    a remainder with no trailing CRLF is genuine content rather than a fragment awaiting its
    other half. ``final`` implies ``at_eof``.

    Offsets are counted in **bytes**, never characters: driver and circuit names carry
    non-ASCII, and character arithmetic would drift the cursor by however many multi-byte code
    points a chunk happened to contain — a desync that surfaces only at the races whose entry
    list has an accent in it.

    A line is skipped (with a warning) when its prefix is not an offset, when its body is not
    JSON, or when it exceeds :data:`MAX_LINE_BYTES` — but its **bytes are still consumed**,
    via ``tail``. Skipping without consuming would re-read the same bad line forever, and if
    it were the last line of a feed the session would never complete.
    """
    offset = start
    if start == 0 and chunk.startswith(BOM):
        chunk, offset = chunk[len(BOM):], offset + len(BOM)

    segments = chunk.split(SEPARATOR)
    remainder = segments.pop()  # empty when the chunk ended exactly on a separator
    unterminated = bool(final and remainder)
    if unterminated:
        segments.append(remainder)  # an unterminated final line is genuine content

    lines: list[Line] = []
    for index, segment in enumerate(segments):
        # A line consumes its separator too — except the final unterminated one, which has
        # none. Hence `end` walks the running offset rather than being recomputed from start.
        terminated = not (unterminated and index == len(segments) - 1)
        end = offset + len(segment) + (len(SEPARATOR) if terminated else 0)
        line = _decode(segment, feed=feed, end=end)
        if line is not None:
            lines.append(line)
        offset = end
    return Fetch(feed=feed, lines=lines, tail=offset, at_eof=at_eof or final)


def _decode(segment: bytes, *, feed: str, end: int) -> Line | None:
    """One framed segment as a :class:`Line`, or ``None`` if it is unusable (logged)."""
    if not segment:
        return None  # a blank line between records — nothing to report
    if len(segment) > MAX_LINE_BYTES:
        log.warning("%s: skipping a %d-byte line ending at %d — over the %d-byte limit",
                    feed, len(segment), end, MAX_LINE_BYTES)
        return None
    try:
        text = segment.decode()
    except UnicodeDecodeError:
        log.warning("%s: skipping a line ending at %d — not valid UTF-8", feed, end)
        return None
    offset_ms = parse_offset(text[:OFFSET_WIDTH])
    if offset_ms is None:
        log.warning("%s: skipping a line ending at %d — %r is not an offset prefix",
                    feed, end, text[:OFFSET_WIDTH])
        return None
    try:
        payload = json.loads(text[OFFSET_WIDTH:])
    except json.JSONDecodeError as error:
        log.warning("%s: skipping the line at %s ending at %d — %s",
                    feed, text[:OFFSET_WIDTH], end, error)
        return None
    if feed.endswith(COMPRESSED_SUFFIX):
        if not isinstance(payload, str):
            log.warning("%s: skipping the line at %s — a .z feed must carry a JSON string, "
                        "got %s", feed, text[:OFFSET_WIDTH], type(payload).__name__)
            return None
        try:
            payload = inflate(payload)
        except (ValueError, zlib.error) as error:
            log.warning("%s: skipping the line at %s ending at %d — inflate failed: %s",
                        feed, text[:OFFSET_WIDTH], end, error)
            return None
    return Line(feed=feed, offset_ms=offset_ms, end=end, payload=payload)


def frontier(fetch: Fetch, *, live_frontier_ms: int | None = None) -> int | None:
    """How far this feed is known to be complete — ``None`` meaning "exhausted" — pure.

    Three cases, and the middle one is the reason the live path needs no separate code:

    * **Not at end-of-file.** The frontier is the last complete line's offset. Beyond it
      there are bytes we have not read, so we cannot know what happens there.
    * **At end-of-file, archive phase** (``live_frontier_ms is None``). The file is finished:
      this feed can never say anything again, so it is *exhausted* and drops out of the
      minimum entirely. Without this, one short feed would pin the watermark at its last
      record and freeze the rest of the tape.
    * **At end-of-file, live phase.** The file is merely caught up. Absence of a line up to
      now *is* information — a quiet ``RaceControlMessages`` means no flags, not unknown
      flags — so the frontier becomes the caller's wall-derived session offset. Otherwise a
      feed that emits twice an hour would stall the leaderboard for half an hour at a time.

    A chunk with no complete line at all reports :data:`BLOCKED`, which holds emission until
    the caller retries with a bigger chunk.
    """
    if fetch.at_eof:
        return live_frontier_ms
    if not fetch.lines:
        return BLOCKED
    return fetch.lines[-1].offset_ms


def watermark(frontiers: Iterable[int | None]) -> int | None:
    """The cross-feed ordering bound: the minimum frontier, or ``None`` if all are exhausted.

    ``None`` means *no bound* — every feed has been read to the end of a finished file, so
    whatever is left can be emitted in full. Pure.
    """
    bounded = [value for value in frontiers if value is not None]
    return min(bounded) if bounded else None


def merge(fetches: Iterable[Fetch], *, bound: int | None, limit: int) -> list[Line]:
    """The poll's emission: every line at or below ``bound``, in tape order, up to ``limit``.

    Ordered by ``(offset_ms, feed)`` — the feed name as tie-break, so that lines sharing an
    offset (they do: a status change and the timing patch it caused are recorded in the same
    millisecond) come out in a **deterministic** order on every replay. Determinism here is
    not cosmetic: the board folds these into state, so a different order is a different
    leaderboard.

    ``limit`` is the per-poll line budget. It is what keeps a 7 MB backfill from opening a
    Kafka transaction that outlives the 10-minute timeout: each poll emits a bounded slice
    and commits it, and the next poll carves the next one. Pure.
    """
    ordered = heap_merge(*(fetch.lines for fetch in fetches),
                         key=lambda line: (line.offset_ms, line.feed))
    emitted: list[Line] = []
    for line in ordered:
        if bound is not None and line.offset_ms > bound:
            break
        if len(emitted) >= limit:
            break
        emitted.append(line)
    return emitted


def cursors_after(fetches: Iterable[Fetch], emitted: Sequence[Line]) -> dict[str, int]:
    """Each feed's new cursor given what was actually emitted — pure.

    Advances **only past emitted lines**, never past merely fetched ones: bytes read but held
    back by the watermark are simply re-read next poll (at most one chunk per feed of wasted
    transfer, in exchange for never needing a buffer in state). A feed all of whose lines
    were emitted advances to ``tail`` instead of to its last line's ``end``, which is how a
    skipped malformed line or an unterminated final line gets consumed. Feeds that emitted
    nothing are absent from the result, so the caller leaves their cursor untouched.

    Relies on :func:`merge` preserving each feed's own order, which makes the emitted subset
    a *prefix* of every feed's line list — so a count is enough and no offset comparison is
    needed.
    """
    counts: dict[str, int] = {}
    for line in emitted:
        counts[line.feed] = counts.get(line.feed, 0) + 1
    cursors: dict[str, int] = {}
    for fetch in fetches:
        count = counts.get(fetch.feed, 0)
        if not count:
            continue
        cursors[fetch.feed] = fetch.tail if count == len(fetch.lines) else fetch.lines[count - 1].end
    return cursors


# --- the t0 anchor ---

ANCHOR_SPREAD: Final = timedelta(seconds=1)
"""How far apart the first few anchor estimates may be before it is worth a warning.

Real spread over the first five ``Heartbeat`` lines of a race: **0.18 s**. The check is
cheap insurance against anchoring off a line whose inner clock has nothing to do with its
recording position."""

ANCHOR_SAMPLE: Final = 5
"""How many anchor-bearing lines the spread check looks at.

Deliberately a handful **from the start of the tape**, never the whole feed: at the very end
of a recording the offsets stop advancing while the heartbeats keep ticking (the recorder
flushes its tail), so the same session's last beats imply a ``t0`` twenty minutes late. Over
the full 708 beats of one race the implied ``t0`` spans 1200 s; over the first five, 0.18 s.
The tape's beginning is the only part of it that is trustworthy for this."""


def inner_utc(payload: Any) -> datetime | None:
    """The absolute instant a payload carries in its own right, if any — pure, aware UTC.

    Four shapes, in the order they are tried:

    * ``{"Utc": …}`` — ``Heartbeat`` and ``ExtrapolatedClock``.
    * ``{"Entries": [{"Utc": …}, …]}`` — ``CarData.z``, the first sample of the line.
    * ``{"Position": [{"Timestamp": …}, …]}`` — ``Position.z``.
    * ``{"Messages": …}`` — ``RaceControlMessages``, whose payload is an **array early in the
      session and an index-keyed dict later**, so both are accepted.

    Timestamps are inconsistent across feeds by nature: some carry ``Z`` and seven fractional
    digits, race control carries seconds and **no zone at all** (verified UTC). Both are
    parsed and normalized to aware UTC, because the alternative — a mix of naive and aware
    datetimes — raises ``TypeError`` at the first subtraction, in a place far from the cause.
    """
    if not isinstance(payload, dict):
        return None
    for key in ("Utc", "Timestamp"):
        if isinstance(raw := payload.get(key), str):
            return parse_utc(raw)
    for key in ("Entries", "Position", "Messages"):
        entries = payload.get(key)
        if isinstance(entries, dict):
            entries = list(entries.values())
        if isinstance(entries, list) and entries:
            return inner_utc(entries[0])
    return None


def parse_utc(raw: str) -> datetime | None:
    """One upstream timestamp string as aware UTC, or ``None`` if it will not parse.

    ``fromisoformat`` handles the archive's whole vocabulary on Python 3.11+ — trailing
    ``Z``, seven fractional digits (truncated to microseconds), and no fraction at all. A
    zone-less value is *assumed* UTC, which is what the feed means by it.
    """
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def anchor(lines: Sequence[Line]) -> int | None:
    """``t0`` in epoch milliseconds from the tape's first anchor-bearing lines — pure.

    ``t0 = inner_utc − offset``: a line that states both where it sits in the recording *and*
    what time it was gives the recording's own start instant, and from then on every line's
    :data:`~.attributes.EVENT_TIME` follows from its offset alone.

    The **first** usable line wins; the next :data:`ANCHOR_SAMPLE` are used only to check the
    spread and warn. ``None`` when no line carries an inner instant — the caller must then
    treat the session as un-anchorable rather than guess, because a tape that cannot be placed
    in time must not be emitted at all.

    **On accuracy, honestly.** Different feeds imply anchors a few seconds apart: heartbeats
    give ``12:09:12.47`` for one race where ``ExtrapolatedClock`` gives ``12:09:10.34`` and
    ``CarData``'s sample clock gives ``12:09:09.38``. That spread is the broadcast pipeline
    itself — a telemetry sample is generated before the line carrying it is recorded — and no
    choice of anchor removes it. Each feed is *internally* consistent to a fraction of a
    second, so relative order and every duration on the tape are exact; the absolute placement
    carries a few seconds of systematic bias, which is immaterial for a two-hour race and is
    documented rather than hidden.
    """
    estimates: list[datetime] = []
    for line in lines:
        utc = inner_utc(line.payload)
        if utc is None:
            continue
        estimates.append(utc - timedelta(milliseconds=line.offset_ms))
        if len(estimates) >= ANCHOR_SAMPLE:
            break
    if not estimates:
        return None
    if (spread := max(estimates) - min(estimates)) > ANCHOR_SPREAD:
        log.warning("anchor estimates from the first %d line(s) span %.3fs (over %.3fs) — "
                    "absolute event times may be off by that much",
                    len(estimates), spread.total_seconds(), ANCHOR_SPREAD.total_seconds())
    return int(estimates[0].timestamp() * 1000)


def event_time(t0_ms: int, offset_ms: int) -> datetime:
    """A line's absolute instant: ``t0 + offset``, aware UTC — pure."""
    return datetime.fromtimestamp((t0_ms + offset_ms) / 1000, tz=timezone.utc)


def session_offset_ms(t0_ms: int, now: datetime) -> int:
    """``now`` expressed as a recording offset — the inverse of :func:`event_time`.

    The live phase's frontier is built from this: "no line up to *this* offset" is a claim
    about the wall clock, and it is the only place on the ingest path where a wall clock
    appears at all.
    """
    return int(now.timestamp() * 1000) - t0_ms

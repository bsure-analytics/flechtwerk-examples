"""GDELT event coverage — a co-partitioned stream–stream join (Events ⋈ Mentions).

Stage 2a. It consumes two raw topics that ingest keyed identically by
``GlobalEventID`` — ``gdelt-events-raw`` (one coded event) and ``gdelt-mentions-raw``
(one article mentioning an event) — and folds them into one **coverage** record per
event: the event's summary (root action code, action location, tone, source URL) plus
mention aggregates (how many mentions, how many distinct outlets, first/last mention
time). That record lands on ``gdelt-event-coverage`` and drives the breaking-news
velocity and tone-map panels.

**Why this is a co-partitioned join and not two independent streams.** Both topics
key by ``GlobalEventID`` and have the same partition count, so an event and every
mention of it hash to the same partition → the same task → the same state bucket
(``extract_state_key`` defaults to the message key). The framework processes all
records sharing a ``(task, key)`` bucket serially, in ``input_topics`` order then
offset order, so one ``transform`` sees them one at a time against the accumulating
state — that *is* the join. See the framework's co-partitioning best-practices guide.

**Out-of-order arrival is the interesting part.** Ordering across the two topics is
not guaranteed: a mention can arrive before its event row (GDELT emits them in the same
15-minute slice, but Kafka interleaves partitions freely). So a mention whose event has
not yet been seen is **buffered as an orphan** — its aggregates accumulate and the
coverage record is emitted with ``event_seen = 0`` — and reconciled the moment the event
lands. An orphan that is never resolved would leak state forever, so it carries a TTL:
once a straggler mention arrives more than ``ORPHAN_TTL`` (event time) after the buffer
was first orphaned, the dead buffer is **tombstoned via a falsy ``State``** (the key is
deleted from the store, atomically) and the straggler dropped. This bounds the store to
live events; abandoning a few 48-hours-late stragglers for an event GDELT never coded is
the documented trade-off.

**Event time is the file timestamp, never ``SQLDATE``** — read from ``metadata.file_ts``,
which is also the deterministic clock the TTL compares against (no wall-clock, so the
logic tier drives every path).

The whole join is the pure async generator :func:`join_coverage`; the stage is a thin
``@transformer`` over it.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from flechtwerk import Event, IncomingMessage, Message, State, transformer
from flechtwerk.attribute import Attribute, Record

from .ingest import EVENTS_RAW_TOPIC, MENTIONS_RAW_TOPIC
from .schema import (
    ACTION_GEO_COUNTRY,
    ACTION_GEO_FULLNAME,
    ACTION_GEO_LAT,
    ACTION_GEO_LONG,
    AVG_TONE,
    DISTINCT_SOURCES,
    EVENT_ROOT_CODE,
    EVENT_SEEN,
    FILE_TS,
    FIRST_MENTION_AT,
    GLOBAL_EVENT_ID,
    LAST_MENTION_AT,
    MENTION_COUNT,
    MENTION_SOURCE_NAME,
    MENTION_TIME_DATE,
    METADATA,
    ORPHANED_AT,
    ROW,
    SOURCE_URL,
    SOURCES,
    TABLE,
    UPDATED_AT,
    parse_gdelt_datetime,
)

log = logging.getLogger(__name__)

COVERAGE_TOPIC = "gdelt-event-coverage"

ORPHAN_TTL = timedelta(hours=48)
"""How long an unresolved orphan (mentions buffered, event row never seen) is kept before
a later straggler mention tombstones it — bounds the state store to genuinely live events."""

# The event-summary fields lifted from an event row into coverage state / output.
_EVENT_SUMMARY = (EVENT_ROOT_CODE, ACTION_GEO_FULLNAME, ACTION_GEO_COUNTRY,
                  ACTION_GEO_LAT, ACTION_GEO_LONG, AVG_TONE, SOURCE_URL)
# Scalar state fields carried forward across merges (SOURCES, a set, is carried separately).
_CARRIED = (EVENT_SEEN, MENTION_COUNT, FIRST_MENTION_AT, LAST_MENTION_AT, ORPHANED_AT, *_EVENT_SUMMARY)


def _carry(state: State) -> dict[Attribute, Any]:
    """Decode the running coverage state into an Attribute-keyed working dict.

    Building the next ``State`` from typed attributes (not by poking ``.raw``) keeps the
    encode/decode discipline: ``.raw`` holds wire-encoded values, so hand-merging it would
    mix encoded and decoded forms. Reads the fields we keep; ``SOURCES`` (a set) is copied.
    """
    work: dict[Attribute, Any] = {attr: value for attr in _CARRIED if (value := state.get(attr)) is not None}
    if (sources := state.get(SOURCES)) is not None:
        work[SOURCES] = set(sources)
    return work


def _coverage_record(event_id: str, state: State) -> Event:
    """Project the merged join state into a ``gdelt-event-coverage`` output record.

    Carries the event summary through as strings (the sink coerces tone/lat/lon at query
    time, the same "carry the wire value, coerce where you compute" rule as ADS-B), and
    exposes the mention aggregates: the running count, the distinct-source *count* (from the
    ``sources`` set kept in state), and the first/last mention window.
    """
    record = Event({
        GLOBAL_EVENT_ID: event_id,
        EVENT_SEEN: state.get(EVENT_SEEN) or 0,
        MENTION_COUNT: state.get(MENTION_COUNT) or 0,
        DISTINCT_SOURCES: len(state.get(SOURCES) or set()),
        UPDATED_AT: state[UPDATED_AT],
    })
    for attr in _EVENT_SUMMARY:
        if (value := state.get(attr)) is not None:
            record[attr] = value
    for attr in (FIRST_MENTION_AT, LAST_MENTION_AT):
        if (value := state.get(attr)) is not None:
            record[attr] = value
    return record


def _merge_event(work: dict[Attribute, Any], row: Record, file_ts: datetime) -> None:
    """Fold an event row into the coverage state: store its summary, mark it seen,
    clear the orphan clock (the buffered mentions, if any, are now reconciled)."""
    for attr in _EVENT_SUMMARY:
        if (value := row.get(attr)) is not None:
            work[attr] = value
    work[EVENT_SEEN] = 1
    work.pop(ORPHANED_AT, None)


def _merge_mention(work: dict[Attribute, Any], row: Record, file_ts: datetime) -> None:
    """Fold a mention row into the coverage state: bump the count, add its outlet to the
    distinct-source set, widen the first/last-mention window, and — while the event row is
    still unseen — start (or keep) the orphan TTL clock."""
    work[MENTION_COUNT] = (work.get(MENTION_COUNT) or 0) + 1
    sources = set(work.get(SOURCES) or set())
    if source := row.get(MENTION_SOURCE_NAME):
        sources.add(source)
    work[SOURCES] = sources
    at = parse_gdelt_datetime(row.get(MENTION_TIME_DATE)) or file_ts
    first, last = work.get(FIRST_MENTION_AT), work.get(LAST_MENTION_AT)
    work[FIRST_MENTION_AT] = min(at, first) if first else at
    work[LAST_MENTION_AT] = max(at, last) if last else at
    if not work.get(EVENT_SEEN) and ORPHANED_AT not in work:
        work[ORPHANED_AT] = file_ts


async def join_coverage(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Fold one raw event/mention into the per-event coverage record.

    Reconciliation and TTL both key off the message's ``file_ts`` (event time). If the
    stored buffer is an unresolved orphan older than ``ORPHAN_TTL``: a straggler mention
    tombstones it (falsy ``State``, key deleted) and is dropped; an event that finally
    arrives rebuilds fresh (the stale mention aggregates discarded). Otherwise the row is
    merged, the coverage record emitted, and the new ``State`` yielded as the commit
    boundary. Pure and I/O-free.
    """
    table = msg.value[METADATA][TABLE]
    file_ts = msg.value[METADATA][FILE_TS]
    row = msg.value[ROW]
    event_id = msg.key

    orphaned_at = state.get(ORPHANED_AT)
    expired = not (state.get(EVENT_SEEN) or 0) and orphaned_at is not None and file_ts - orphaned_at > ORPHAN_TTL
    if expired and table == "mentions":
        yield State()  # dead orphan + a late straggler → tombstone the buffer, drop the straggler
        return
    work: dict[Attribute, Any] = {} if expired else _carry(state)  # a late event rebuilds from empty

    if table == "events":
        _merge_event(work, row, file_ts)
    else:
        _merge_mention(work, row, file_ts)
    work[UPDATED_AT] = file_ts

    new_state = State(work)
    yield Message(key=event_id, topic=COVERAGE_TOPIC, value=_coverage_record(event_id, new_state))
    yield new_state


@transformer(input_topics=[EVENTS_RAW_TOPIC, MENTIONS_RAW_TOPIC])
async def coverage(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in join_coverage(state, msg):
        yield item


stage = coverage
"""The stage the dispatcher runs (``python -m examples.gdelt_news_stories coverage``)."""

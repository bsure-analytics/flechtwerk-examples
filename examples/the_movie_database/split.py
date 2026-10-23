"""TMDB split — a ``Transformer`` that turns backed-up change pages into per-entity changes.

Stage 2. Each ``tmdb-changes-raw`` record is one change-feed page exactly as TMDB delivered it
(the extractor's backup). This stage is where the page is first *read*: it parses the ids
(``projection.parse_changes_page`` — ``adult: true`` ids dropped, duplicates collapsed, unknown
fields ignored) and emits one ``tmdb-changes`` record per id, keyed ``<feed>:<id>``, so the
catalog's state and a title's versions and credits co-partition.

**The open day is deduped here.** The extractor re-lists *today* in full on every hourly poll, so
the same ids arrive again and again while the day accrues. The state — one record per feed, the
wire key of the raw topic — is a ``SEEN`` set of the ids already emitted for the open day, and an
``open`` page emits only ids not in it. A ``closed`` page (the day's one listing after midnight
UTC, or any backfill day) emits **every** id again: an entity edited after its hourly fetch is
fetched once more, and the catalog's content-hash gate turns a no-op second fetch into nothing.

**Why this is a stage of its own.** It is the transformation the extractor must not do. Keeping
the backup raw means a change of rule here — a different adult policy, a different dedupe — is a
replay of ``tmdb-changes-raw``, not a re-crawl of TMDB.

**Ordering.** All pages of a feed share one key, so they arrive at one task in the order the
extractor listed them: a day's ``closed`` pages always follow its ``open`` ones, which is what
lets a closed page drop that day's bucket.
"""
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, Transformer

from .attributes import (
    CHANGED_ON,
    CHANGES_RAW_TOPIC,
    CHANGES_TOPIC,
    DISCOVERED_AT,
    FEED,
    LISTING,
    METADATA,
    RESPONSE,
    SEEN,
    TMDB_ID,
)
from .projection import parse_changes_page, prune_seen


class TmdbSplit(Transformer):
    """Splits raw change pages into per-entity change records, deduping the open day."""

    input_topics = [CHANGES_RAW_TOPIC]

    def __init__(self, *, changes_topic: str = CHANGES_TOPIC) -> None:
        super().__init__()
        self._topic = changes_topic

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        if msg.value is None:
            return
        meta = msg.value[METADATA]
        feed, day, listing = meta[FEED], meta[CHANGED_ON], meta[LISTING]
        ids, _ = parse_changes_page(msg.value[RESPONSE].raw)
        seen = {d: list(i) for d, i in (state.get(SEEN) or {}).items()}

        if listing == "closed":
            fresh = ids
            seen.pop(day.isoformat(), None)
        else:
            known = set(seen.get(day.isoformat(), []))
            fresh = [tmdb_id for tmdb_id in ids if tmdb_id not in known]
            seen[day.isoformat()] = sorted(known | set(ids))
            seen = prune_seen(seen, keep_from=day)  # an open page means every earlier day is past

        for tmdb_id in fresh:
            yield Message(key=f"{feed}:{tmdb_id}", topic=self._topic, value=Event({
                FEED: feed, TMDB_ID: tmdb_id, CHANGED_ON: day, LISTING: listing,
                DISCOVERED_AT: meta[DISCOVERED_AT],
            }))
        yield State({SEEN: seen})


stage = TmdbSplit()

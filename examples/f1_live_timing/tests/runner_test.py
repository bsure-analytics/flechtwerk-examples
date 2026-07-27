"""Tier 2 — the runners, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker, no
network — with the archive served from the committed fixture session by an ``httpx.MockTransport``
that implements **real HTTP range semantics**: ``206`` with a ``Content-Range``, ``416`` past the
end, and a ``visible`` byte budget per file so a test can make a file *grow between polls* and
exercise the live-tail path.

- **ingest** drives the real ``ExtractorRunner``/``poll_one``. What is worth proving here is
  everything the pure tier cannot see: that the ``t0`` anchor is a genuine *peek* (no cursor
  moves, the anchor line is emitted later by the ordinary merge, exactly once), that ``t0``
  survives a restart once the merge has consumed it, that a resumed poll sends the right
  ``Range`` header, that a completed session then costs **zero** HTTP calls, and that a 404 is
  treated as "the weekend hasn't started".
- **timing** drives ``TransformerRunner.process_batch`` over a batch of real tape records built
  from the same fixture, so the board's emissions are asserted through the framework's own
  serialization rather than around it.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from flechtwerk import Config, Event
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.transformer import Task

from examples.f1_live_timing import tape
from examples.f1_live_timing.attributes import (
    CURSORS,
    DONE,
    EVENT_TIME,
    EVENTS_TOPIC,
    FEED,
    KIND,
    LENGTHS,
    OFFSET_MS,
    PATH,
    PAYLOAD,
    PHASE,
    SEEN,
    SESSION,
    SESSIONS_TOPIC,
    STATUS_TOPIC,
    T0_MS,
    TELEMETRY,
    TELEMETRY_TOPIC,
    TIMING_TOPIC,
    YEAR,
)
from examples.f1_live_timing.ingest import (
    ARCHIVE_PHASE,
    MIN_CHUNK_BYTES,
    FOLLOW_KIND,
    FOLLOW_PREFIX,
    LIVE_PHASE,
    SESSION_KIND,
    TELEMETRY_FEEDS,
    WISH_LIST,
    TapeIngest,
)
from examples.f1_live_timing.timing import LAP_KIND, STANDINGS, timing

FIXTURES = Path(__file__).parent / "fixtures" / "session"
BASE = "http://archive.test/static"
SESSION_PATH = "2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/"
SESSION_CONFIG = {"kind": SESSION_KIND, "path": SESSION_PATH, "year": 2026, "telemetry": True}

UTC = timezone.utc
T0 = datetime(2026, 7, 26, 12, 9, 12, 467000, tzinfo=UTC)
INGESTED = set(WISH_LIST) | set(TELEMETRY_FEEDS)
# The fixture publishes two feeds the wish list does not want, on purpose.
UNWANTED = {"TimingStats", "TyreStintSeries"}

YEAR_INDEX = {"Meetings": [{
    "Name": "Hungarian Grand Prix",
    "Sessions": [
        {"Key": 11337, "Type": "Practice", "Name": "Practice 3", "GmtOffset": "02:00:00",
         "StartDate": "2026-07-25T12:30:00",
         "Path": "2026/2026-07-26_Hungarian_Grand_Prix/2026-07-25_Practice_3/"},
        {"Key": 11338, "Type": "Qualifying", "Name": "Qualifying", "GmtOffset": "02:00:00",
         "StartDate": "2026-07-25T16:00:00",
         "Path": "2026/2026-07-26_Hungarian_Grand_Prix/2026-07-25_Qualifying/"},
        {"Key": 11342, "Type": "Race", "Name": "Race", "GmtOffset": "02:00:00",
         "StartDate": "2026-07-26T15:00:00", "Path": SESSION_PATH},
    ],
}]}


class Archive:
    """The fixture session behind a ``MockTransport``, with honest range semantics.

    ``visible`` caps how many bytes of a file the server admits to having, which is how a test
    makes a file grow between polls; ``ignore_range`` makes it answer 200 with the whole body,
    the one behaviour the ingest stage has to compensate for locally; ``missing`` and ``status``
    cover an absent session and a hard failure.
    """

    def __init__(self, *, feeds: set[str] | None = None, complete: bool = True) -> None:
        self.bodies = {path.name: path.read_bytes() for path in FIXTURES.iterdir()}
        self.feeds = feeds if feeds is not None else {
            name.removesuffix(".jsonStream") for name in self.bodies if name.endswith(".jsonStream")}
        self.complete = complete
        self.visible: dict[str, int] = {}
        self.ignore_range = False
        self.missing = False
        self.requests: list[tuple[str, str | None]] = []
        self.status: int | None = None
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self.handle),
                                        base_url="http://archive.test")

    def body(self, name: str) -> bytes:
        return self.bodies[name][:self.visible.get(name, len(self.bodies[name]))]

    def handle(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        self.requests.append((name, request.headers.get("Range")))
        assert request.headers.get("Accept-Encoding") == "identity", "cursors need raw bytes"
        if self.status is not None:
            return httpx.Response(self.status, text="boom")
        if name == "Index.json" and request.url.path.startswith("/static/2026/Index"):
            return httpx.Response(200, content=b"\xef\xbb\xbf" + json.dumps(YEAR_INDEX).encode())
        if self.missing:
            return httpx.Response(404, text="not found")
        if name == "Index.json":
            index = {"Feeds": {feed: {"KeyFramePath": f"{feed}.json",
                                      "StreamPath": f"{feed}.jsonStream"} for feed in self.feeds}}
            return httpx.Response(200, content=b"\xef\xbb\xbf" + json.dumps(index).encode())
        if name == "ArchiveStatus.json":
            status = "Complete" if self.complete else "Generating"
            return httpx.Response(
                200, content=b"\xef\xbb\xbf" + json.dumps({"Status": status}).encode())
        body = self.body(name)
        header = request.headers.get("Range")
        if header is None or self.ignore_range:
            return httpx.Response(200, content=body)
        start, end = (int(part) for part in header.removeprefix("bytes=").split("-"))
        if start >= len(body):
            return httpx.Response(416, text="Requested Range Not Satisfiable")
        chunk = body[start:end + 1]
        return httpx.Response(206, content=chunk, headers={
            "Content-Range": f"bytes {start}-{start + len(chunk) - 1}/{len(body)}"})

    def stream_requests(self) -> list[tuple[str, str | None]]:
        return [entry for entry in self.requests if entry[0].endswith(".jsonStream")]

    def feeds_read(self) -> set[str]:
        return {name.removesuffix(".jsonStream") for name, _ in self.stream_requests()}


def _runner(stage: Extractor, entries: dict[str, dict],
            store: InMemoryStateStore | None = None) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """A single-token ExtractorRunner over the shipped fakes, seeded with config records."""
    producer = FakeKafkaProducer()
    inner = store or InMemoryStateStore()
    runner = ExtractorRunner()
    runner.changelog_topic = "f1-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer([
        make_record(key=key, value=json.dumps(config), topic=SESSIONS_TOPIC, offset=offset)
        for offset, (key, config) in enumerate(entries.items())])
    runner.create_restore_consumer = lambda: FakeKafkaConsumer()
    runner.create_token_producer = lambda token: producer
    runner.extractor = stage
    runner.inner_store = inner
    runner.observer = Observer()
    runner.poll_interval = timedelta(0)
    runner.num_tokens = 1
    runner.tokens = frozenset({0})
    changelog = ChangelogStateStore()
    changelog.inner = inner
    changelog.producer = FakeKafkaProducer()
    changelog.topic = runner.changelog_topic
    runner.tasks[0] = TokenTask(asyncio.Lock(), producer, changelog)
    return runner, producer


async def _ingest(archive: Archive, *, entries: dict[str, dict] | None = None,
                  store: InMemoryStateStore | None = None,
                  **kwargs) -> tuple[TapeIngest, ExtractorRunner, FakeKafkaProducer]:
    stage = TapeIngest(archive.client, base_url=BASE, **kwargs)
    runner, producer = _runner(stage, entries or {SESSION_PATH: SESSION_CONFIG}, store)
    await runner.load_initial_configs()
    return stage, runner, producer


def _sent(producer: FakeKafkaProducer, topic: str = TIMING_TOPIC) -> list[dict]:
    return [json.loads(payload["value"]) for sent_topic, payload in producer.sent
            if sent_topic == topic]


async def _poll(runner: ExtractorRunner, key: str = SESSION_PATH) -> None:
    await runner.poll_one(runner.entries[key])


async def _drain(runner: ExtractorRunner, *, key: str = SESSION_PATH, limit: int = 40) -> int:
    """Poll until the session reports done, returning how many polls it took."""
    for count in range(1, limit + 1):
        await _poll(runner, key)
        state = await runner.tasks[0].store.get(key)
        if state is not None and state.get(DONE):
            return count
    raise AssertionError(f"session never completed in {limit} polls")


# --- ingest: the archive path ---

async def test_the_first_poll_only_anchors_and_moves_nothing() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)

    assert producer.sent == []                       # a peek emits nothing
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None
    assert state[T0_MS] == int(T0.timestamp() * 1000)
    assert state[PHASE] == ARCHIVE_PHASE
    assert state.get(CURSORS) == {}                  # ... and advances no cursor
    # Only the anchor feed was read — the whole point of doing this as a separate poll.
    assert archive.feeds_read() == {"Heartbeat"}


async def test_the_anchor_line_is_emitted_exactly_once_by_the_next_poll() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)                          # anchor
        await _drain(runner)

    heartbeats = [record for record in _sent(producer) if record["feed"] == "Heartbeat"]
    assert len(heartbeats) == len(tape.frame(
        (FIXTURES / "Heartbeat.jsonStream").read_bytes(), feed="Heartbeat", start=0,
        final=True).lines)
    assert heartbeats[0][OFFSET_MS.name] == 13844
    assert heartbeats[0][EVENT_TIME.name] == "2026-07-26T12:09:26.311000Z"


async def test_a_backfill_emits_the_whole_tape_in_merged_order() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)

    records = _sent(producer)
    expected = tape.merge([tape.frame((FIXTURES / f"{feed}.jsonStream").read_bytes(),
                                      feed=feed, start=0, final=True)
                           for feed in sorted(INGESTED)], bound=None, limit=10_000)
    assert [(record[OFFSET_MS.name], record[FEED.name]) for record in records] == [
        (line.offset_ms, line.feed) for line in expected]
    assert all(record[SESSION.name] == SESSION_PATH for record in records)
    assert {payload["key"].decode() for _, payload in producer.sent} == {SESSION_PATH}

    # Every record carries its event time both as a field and as the Kafka timestamp.
    for topic, payload in producer.sent:
        assert topic == TIMING_TOPIC and payload["timestamp_ms"] is not None
    first = records[0]
    assert first[FEED.name] == "SessionInfo" and first[OFFSET_MS.name] == 0
    assert first[EVENT_TIME.name] == "2026-07-26T12:09:12.467000Z"


async def test_z_payloads_reach_kafka_already_inflated() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
    car, = [record for record in _sent(producer) if record[FEED.name] == "CarData.z"]
    # The feed keeps its .z name (that is its identity in the index) but the payload is JSON.
    assert car[PAYLOAD]["Entries"][0]["Cars"]["1"]["Channels"]["2"] == 148


async def test_only_wished_feeds_are_ever_requested() -> None:
    archive = Archive()
    stage, runner, _ = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
    assert archive.feeds_read() == INGESTED
    assert not archive.feeds_read() & UNWANTED     # published, wanted by nobody, never fetched


async def test_telemetry_off_never_touches_the_z_feeds() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(
        archive, entries={SESSION_PATH: {**SESSION_CONFIG, "telemetry": False}})
    async with stage:
        await _poll(runner)
        await _drain(runner)
    assert archive.feeds_read() == set(WISH_LIST)
    assert not any(record[FEED.name].endswith(".z") for record in _sent(producer))


async def test_a_missing_feed_is_skipped_silently() -> None:
    archive = Archive(feeds={"Heartbeat", "SessionInfo", "TrackStatus", "LapCount"})
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
    assert archive.feeds_read() == {"Heartbeat", "SessionInfo", "TrackStatus", "LapCount"}
    assert {record[FEED.name] for record in _sent(producer)} == archive.feeds_read()


async def test_the_index_is_read_once_per_session_not_once_per_poll() -> None:
    archive = Archive()
    stage, runner, _ = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
    assert sum(1 for name, _ in archive.requests if name == "Index.json") == 1


async def test_completion_sets_done_and_then_costs_nothing() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
        state = await runner.tasks[0].store.get(SESSION_PATH)
        assert state is not None and state[DONE] is True
        # The state is KEPT, not tombstoned: an empty state would re-ingest from byte 0.
        assert state[CURSORS] and state[LENGTHS]

        before = len(archive.requests)
        producer.sent.clear()
        await _poll(runner)
    assert len(archive.requests) == before           # not one request
    assert producer.sent == []


async def test_a_small_chunk_paces_the_backfill_and_resumes_with_the_right_range() -> None:
    # 3000 bytes clears the fixture's 2782-byte TimingData keyframe but not the 4514-byte file,
    # so the feed genuinely spans several polls without tripping the widening path below.
    archive = Archive()
    stage, runner, producer = await _ingest(archive, chunk_bytes=3000)
    async with stage:
        await _poll(runner)                          # anchor
        polls = await _drain(runner)
    assert polls > 1, "a 3 KB chunk must take several polls over a 4.5 KB feed"

    ranges = [header for name, header in archive.stream_requests()
              if name == "TimingData.jsonStream"]
    # The anchor poll read only Heartbeat, so TimingData's very first range is this one.
    assert ranges[0] == "bytes=0-2999"
    # Each subsequent range resumes exactly where the last emitted line ended, never re-reading
    # from the start and never skipping a byte.
    starts = [int(header.removeprefix("bytes=").split("-")[0]) for header in ranges]
    assert starts == sorted(starts) and len(set(starts)) > 1
    cursors = (await runner.tasks[0].store.get(SESSION_PATH))[CURSORS]
    assert starts[-1] < cursors["TimingData"] == 4514
    # ... and the whole tape still arrives exactly once.
    records = _sent(producer)
    assert len(records) == len({(r[FEED.name], r[OFFSET_MS.name]) for r in records})


async def test_each_feed_retunes_its_chunk_to_what_it_actually_consumes() -> None:
    # The politeness property: bytes fetched past the watermark are re-read once, not on every
    # poll. Without the retune a real race downloaded ~7× the tape it ingested.
    archive = Archive()
    stage, runner, producer = await _ingest(archive, chunk_bytes=200_000, line_budget=6)
    async with stage:
        await _poll(runner)                          # anchor
        for _ in range(4):
            await _poll(runner)

        def widths(feed: str) -> list[int]:
            return [int(header.removeprefix("bytes=").split("-")[1])
                    - int(header.removeprefix("bytes=").split("-")[0]) + 1
                    for name, header in archive.stream_requests()
                    if name == f"{feed}.jsonStream" and header]

        # Every feed asks for the ceiling once, then settles at what it can actually get through
        # (here the floor, since the whole fixture is smaller than one chunk).
        for feed in ("TrackStatus", "TimingData", "CarData.z"):
            assert widths(feed)[0] == 200_000, feed
            assert widths(feed)[-1] == MIN_CHUNK_BYTES, feed

        # Nothing is lost by shrinking: the tape still arrives in order, exactly once.
        await _drain(runner)
    records = _sent(producer)
    assert len(records) == len({(r[FEED.name], r[OFFSET_MS.name]) for r in records})
    assert [record[OFFSET_MS.name] for record in records] == sorted(
        record[OFFSET_MS.name] for record in records)


async def test_a_line_longer_than_the_chunk_widens_it_until_it_fits(caplog) -> None:
    # Never observed live (the largest real line is a ~20 KB keyframe against a 512 KB chunk),
    # but without the widening such a feed would never advance a single byte.
    archive = Archive()
    stage, runner, producer = await _ingest(archive, chunk_bytes=600)
    with caplog.at_level("WARNING"):
        async with stage:
            await _poll(runner)                      # anchor
            await _poll(runner)                      # 600 bytes: not one TimingData line fits
            assert producer.sent == []               # ... so the watermark clamps EVERYTHING
            await _drain(runner)
    assert "widening the chunk to 1200" in caplog.text
    assert "widening the chunk to 4800" in caplog.text
    records = _sent(producer)
    assert len(records) == len({(r[FEED.name], r[OFFSET_MS.name]) for r in records})


async def test_the_line_budget_paces_a_poll_and_the_next_one_continues() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive, line_budget=5)
    async with stage:
        await _poll(runner)                          # anchor
        await _poll(runner)
        assert len(_sent(producer)) == 5
        cursors = (await runner.tasks[0].store.get(SESSION_PATH))[CURSORS]
        await _poll(runner)
        assert len(_sent(producer)) == 10
        assert (await runner.tasks[0].store.get(SESSION_PATH))[CURSORS] != cursors
        await _drain(runner)
    records = _sent(producer)
    assert len(records) == len({(r[FEED.name], r[OFFSET_MS.name]) for r in records})


async def test_a_restart_resumes_from_the_committed_cursor_and_keeps_t0() -> None:
    archive = Archive()
    store = InMemoryStateStore()
    stage, runner, producer = await _ingest(archive, store=store, line_budget=8)
    async with stage:
        await _poll(runner)                          # anchor
        await _poll(runner)                          # ... and one page of tape
    first = _sent(producer)
    assert len(first) == 8

    # A brand-new stage and runner over the SAME store — the anchor heartbeat is behind the
    # cursor by now, so t0 can only come from state.
    restarted = Archive()
    stage, runner, producer = await _ingest(restarted, store=store, line_budget=8)
    async with stage:
        state = await runner.tasks[0].store.get(SESSION_PATH)
        assert state is not None and state[T0_MS] == int(T0.timestamp() * 1000)
        await _drain(runner)
    second = _sent(producer)

    assert second and not ({(r[FEED.name], r[OFFSET_MS.name]) for r in first}
                          & {(r[FEED.name], r[OFFSET_MS.name]) for r in second})
    assert len(first) + len(second) == sum(
        len(tape.frame((FIXTURES / f"{feed}.jsonStream").read_bytes(), feed=feed, start=0,
                       final=True).lines) for feed in INGESTED)


async def test_a_session_without_heartbeat_anchors_from_the_fallback_feed() -> None:
    archive = Archive(feeds={"SessionInfo", "TrackStatus", "ExtrapolatedClock", "TimingData"})
    stage, runner, _ = await _ingest(archive)
    async with stage:
        await _poll(runner)
    assert archive.feeds_read() == {"ExtrapolatedClock"}
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None and state[T0_MS] is not None
    # ExtrapolatedClock's inner clock tracks its offset to the millisecond, so it lands on t0.
    assert tape.event_time(state[T0_MS], 0) == T0


async def test_a_session_with_no_anchor_capable_feed_is_refused() -> None:
    archive = Archive(feeds={"SessionInfo", "TrackStatus", "TimingData"})
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
    assert producer.sent == []
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None and state[DONE] is True   # a tape that cannot be placed in time
    assert archive.stream_requests() == []


async def test_an_anchor_poll_that_finds_no_line_is_idempotent() -> None:
    archive = Archive()
    archive.visible["Heartbeat.jsonStream"] = 3        # BOM only: no complete line yet
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        assert await runner.tasks[0].store.get(SESSION_PATH) is None   # nothing persisted
        assert producer.sent == []
        archive.visible.pop("Heartbeat.jsonStream")
        await _poll(runner)
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None and state[T0_MS] == int(T0.timestamp() * 1000)


async def test_a_range_ignoring_server_is_compensated_for_locally() -> None:
    archive = Archive()
    archive.ignore_range = True
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
    records = _sent(producer)
    # Every line exactly once, despite the server resending the whole file on every poll.
    assert len(records) == len({(r[FEED.name], r[OFFSET_MS.name]) for r in records})


async def test_a_416_is_a_no_op_not_an_error() -> None:
    archive = Archive()
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)
        await _drain(runner)
        # Everything is consumed; force one more read by clearing `done`.
        state = await runner.tasks[0].store.get(SESSION_PATH)
        del state.raw[DONE.name]
        await runner.tasks[0].store.put(SESSION_PATH, state)
        producer.sent.clear()
        await _poll(runner)
    assert producer.sent == []
    assert any(header for name, header in archive.stream_requests())
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None and state[DONE] is True   # a 416 still proves completion


async def test_an_http_error_propagates() -> None:
    archive = Archive()
    stage, runner, _ = await _ingest(archive)
    async with stage:
        archive.status = 503
        with pytest.raises(httpx.HTTPStatusError):
            await _poll(runner)


# --- ingest: the live path ---

async def test_a_live_session_tails_a_growing_file() -> None:
    archive = Archive(complete=False)
    heartbeat = "Heartbeat.jsonStream"
    timing_file = "TimingData.jsonStream"
    # Show only the tape's opening: enough for the anchor and the first TimingData keyframe.
    archive.visible[heartbeat] = 60
    archive.visible[timing_file] = len(archive.bodies[timing_file].split(b"\r\n")[0]) + 2
    stage, runner, producer = await _ingest(archive)
    async with stage:
        await _poll(runner)                          # anchor from the visible heartbeat
        state = await runner.tasks[0].store.get(SESSION_PATH)
        assert state is not None and state[PHASE] == LIVE_PHASE
        await _poll(runner)
        early = _sent(producer)
        # A live feed read to the current end of file is only *caught up*, so everything visible
        # is emitted — but only what is visible: TimingData has published one line so far.
        assert [record[OFFSET_MS.name] for record in early
                if record[FEED.name] == "TimingData"] == [8610]
        assert [record[OFFSET_MS.name] for record in early
                if record[FEED.name] == "Heartbeat"] == [13844]

        # The files grow; the same cursors pick up exactly the appended bytes.
        producer.sent.clear()
        archive.visible.pop(timing_file)
        archive.visible.pop(heartbeat)
        await _poll(runner)
        appended = _sent(producer)
    assert [record[OFFSET_MS.name] for record in appended
            if record[FEED.name] == "TimingData"][0] > 8610
    assert not ({(r[FEED.name], r[OFFSET_MS.name]) for r in early}
                & {(r[FEED.name], r[OFFSET_MS.name]) for r in appended})


async def test_a_live_session_is_never_marked_done_until_the_archive_completes() -> None:
    archive = Archive(complete=False)
    stage, runner, _ = await _ingest(archive)
    async with stage:
        await _poll(runner)
        for _ in range(4):
            await _poll(runner)
        state = await runner.tasks[0].store.get(SESSION_PATH)
        assert state is not None and state.get(DONE) is None and state[PHASE] == LIVE_PHASE

        # The recording finishes: the next poll notices and completes.
        archive.complete = True
        await _drain(runner)
    state = await runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None and state[DONE] is True and state[PHASE] == ARCHIVE_PHASE


async def test_a_session_that_has_not_started_is_polled_on_quietly(caplog) -> None:
    archive = Archive()
    archive.missing = True
    stage, runner, producer = await _ingest(archive)
    with caplog.at_level("INFO"):
        async with stage:
            await _poll(runner)
            await _poll(runner)
    assert producer.sent == []
    assert await runner.tasks[0].store.get(SESSION_PATH) is None
    assert caplog.text.count("has not started") == 1     # logged once, then silent
    assert sum(1 for name, _ in archive.requests if name == "Index.json") == 2


# --- ingest: the season-follow target ---

async def test_the_follow_target_writes_one_config_per_new_session() -> None:
    archive = Archive()
    key = f"{FOLLOW_PREFIX}2026"
    stage, runner, producer = await _ingest(archive, entries={key: {
        "kind": FOLLOW_KIND, "year": 2026, "types": ["Race", "Qualifying"], "telemetry": False}})
    async with stage:
        await _poll(runner, key)

    written = [(payload["key"].decode(), json.loads(payload["value"]))
               for topic, payload in producer.sent if topic == SESSIONS_TOPIC]
    assert [entry[0] for entry in written] == [
        "2026/2026-07-26_Hungarian_Grand_Prix/2026-07-25_Qualifying/", SESSION_PATH]
    race = dict(written)[SESSION_PATH]
    assert race == {"kind": SESSION_KIND, "path": SESSION_PATH, "year": 2026,
                    "telemetry": False, "session_key": 11342,
                    "meeting": "Hungarian Grand Prix", "session_name": "Race",
                    "start_local": "2026-07-26T15:00:00", "gmt_offset": "02:00:00"}
    state = await runner.tasks[0].store.get(key)
    assert state is not None and len(state[SEEN]) == 2


async def test_the_follow_target_never_writes_the_same_session_twice() -> None:
    archive = Archive()
    key = f"{FOLLOW_PREFIX}2026"
    stage, runner, producer = await _ingest(
        archive, entries={key: {"kind": FOLLOW_KIND, "year": 2026}})
    async with stage:
        await _poll(runner, key)
        first = len([1 for topic, _ in producer.sent if topic == SESSIONS_TOPIC])
        # Force the interval to have elapsed, so the index really is re-read.
        state = await runner.tasks[0].store.get(key)
        state.raw["checked_ms"] = 0
        await runner.tasks[0].store.put(key, state)
        producer.sent.clear()
        await _poll(runner, key)
    assert first == 2                # Race + Qualifying: the default competitive types
    assert [topic for topic, _ in producer.sent if topic == SESSIONS_TOPIC] == []


async def test_the_follow_target_rate_limits_its_index_reads() -> None:
    archive = Archive()
    key = f"{FOLLOW_PREFIX}2026"
    stage, runner, _ = await _ingest(archive, entries={key: {"kind": FOLLOW_KIND, "year": 2026}})
    async with stage:
        await _poll(runner, key)
        await _poll(runner, key)
        await _poll(runner, key)
    assert sum(1 for name, _ in archive.requests if name == "Index.json") == 1


async def test_enrich_config_completes_a_hand_written_record() -> None:
    stage = TapeIngest(Archive().client, base_url=BASE)
    enriched = await stage.enrich_config(Config.wrap({"path": SESSION_PATH}))
    assert enriched[KIND] == SESSION_KIND and enriched[YEAR] == 2026
    assert enriched[PATH] == SESSION_PATH and enriched.get(TELEMETRY) is None
    # A follow record is returned untouched — it has no path to derive a year from.
    follow = Config.wrap({"kind": FOLLOW_KIND, "year": 2025})
    assert await stage.enrich_config(follow) == follow


# --- timing: TransformerRunner.process_batch over real tape records ---

def _tape_records() -> list[bytes]:
    """The fixture session as ``f1-timing`` record values, in merged tape order."""
    fetches = [tape.frame((FIXTURES / f"{feed}.jsonStream").read_bytes(), feed=feed, start=0,
                          final=True) for feed in sorted(INGESTED)]
    t0_ms = int(T0.timestamp() * 1000)
    values = []
    for line in tape.merge(fetches, bound=None, limit=10_000):
        record = Event({SESSION: SESSION_PATH, FEED: line.feed, OFFSET_MS: line.offset_ms,
                        EVENT_TIME: tape.event_time(t0_ms, line.offset_ms)})
        record.raw[PAYLOAD] = line.payload
        values.append(json.dumps(record.raw).encode())
    return values


def _module(values: list[bytes]) -> _FlechtwerkModule:
    module = _FlechtwerkModule()
    module.application_id = "f1-timing"
    module.client_id = "f1-timing"
    module.bootstrap_servers = "localhost:9092"
    module.metrics_labels = {}
    module.metrics_port = 0
    module.mqtt = None
    module.stage = timing
    module.consumer = FakeKafkaConsumer([
        make_record(key=SESSION_PATH, value=value, topic=TIMING_TOPIC, offset=offset)
        for offset, value in enumerate(values)])
    module.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return module


async def test_the_board_builds_the_session_from_a_real_tape_batch() -> None:
    module = _module(_tape_records())
    await module.runner.process_batch(await module.runner.consumer.getmany(timeout_ms=1000))
    producer = module.runner.tasks[0].producer

    def rows(topic: str, kind: str) -> list[dict]:
        return [record for record in _sent(producer, topic) if record["kind"] == kind]

    standings = rows(STATUS_TOPIC, STANDINGS)
    assert standings and all(row["session_key"] == 11342 for row in standings)
    assert {row["racing_number"] for row in standings} == {"1", "16", "81"}

    laps = rows(EVENTS_TOPIC, LAP_KIND)
    vsc, = [row for row in laps if row["racing_number"] == "1" and row["lap"] == 3]
    assert vsc["track_status"] == "VSCDeployed" and vsc["clean"] is False
    clean, = [row for row in laps if row["racing_number"] == "1" and row["lap"] == 5]
    assert clean["clean"] is True

    assert rows(TELEMETRY_TOPIC, "car") and rows(TELEMETRY_TOPIC, "pos")
    assert all(payload["key"].decode() == SESSION_PATH for _, payload in producer.sent)
    assert all(payload["timestamp_ms"] is not None for _, payload in producer.sent)

    # The tape ends with SessionStatus "Ends" → the bucket is tombstoned.
    assert await module.runner.tasks[0].store.get(SESSION_PATH) is None


async def test_the_board_holds_its_state_until_the_tape_ends() -> None:
    values = _tape_records()
    ends = next(index for index, value in enumerate(values)
                if json.loads(value)["feed"] == "SessionStatus"
                and json.loads(value)[PAYLOAD]["Status"] == "Ends")
    module = _module(values[:ends])
    await module.runner.process_batch(await module.runner.consumer.getmany(timeout_ms=1000))
    state = await module.runner.tasks[0].store.get(SESSION_PATH)
    assert state is not None
    assert state.raw["meta"]["session_key"] == 11342
    assert set(state.raw["drivers"]) == {"1", "16", "81"}
    assert len(json.dumps(state.raw)) < 4_000        # one changelog record, comfortably


async def test_a_batch_that_starts_mid_tape_emits_nothing() -> None:
    # The SessionInfo gate, through the real runner: no session key, no rows.
    values = [value for value in _tape_records() if json.loads(value)["feed"] != "SessionInfo"]
    module = _module(values)
    await module.runner.process_batch(await module.runner.consumer.getmany(timeout_ms=1000))
    assert module.runner.tasks[0].producer.sent == []
    assert await module.runner.tasks[0].store.get(SESSION_PATH) is None

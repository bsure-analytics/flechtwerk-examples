# F1 Live Timing (`f1`)

**F1 Live Timing** (`f1`) is the
**append-only-file** case and the repo's only *deterministic, replayable* example: Formula 1's public
live-timing archive records every session as a directory of line-framed `<Feed>.jsonStream` files, so
`ingest` keeps a **per-feed byte offset** as its resume cursor — which makes backfilling a finished
file, tailing a growing one, and recovering from a crash literally the same code path, and makes an
outage cost timeliness but never data. Config records on `f1-sessions` are keyed by the session
**path** (`2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/`), which is also the Kafka key of
every record the example produces; `request.py` seeds them by `season` / `session <path>` / `follow`,
and the **follow** target is the repo's one extractor that *produces onto its own config topic*
(legal by construction: config topics are consumed group-less and read_committed). Each poll
range-reads a self-tuning chunk of all 14 feeds (16 with `telemetry`, which gates the two big `.z`
feeds), frames whole lines, and emits them under a **watermark** — the minimum offset every feed is
known to be complete to — because the board's flag join would otherwise tag laps with a `TrackStatus`
it has not read yet. Pure cores: `tape.py` (BOM, CRLF framing, raw-deflate inflate, the `t0` anchor,
frontier/watermark/merge) and `board.py` (patch merge, gap/duration parsing, lap detection, the flag
severity order). Two archive traps are load-bearing: **keyframes hold the FINAL state** (so replay
never reads one for data — `ArchiveStatus.json` is the completion probe precisely because its
`.jsonStream` twin says `Generating` forever), and **collections switch from array to index-keyed
dict mid-stream, per field** (`board.indexed`). `t0` is anchored ONCE per session from the first
`Heartbeat` line and **persisted as part of the cursor** — a later beat cannot be used (at the tape's
end the offsets freeze while the beats tick, so the implied `t0` walks 1200 s forward), and a session
with no anchor-capable feed is marked `done` rather than emitted with invented times. The `timing`
transformer folds the feed's *partial patches* into `drivers{}` keyed state and emits **only when a
driver's projection actually changed** (88 % of `TimingData` lines move nothing but a marshalling
segment), producing `f1-status` (standings/weather/clock/heartbeat), `f1-events`
(lap/pit/track_period/race_control/overtake/championship + the session and driver dimensions) and
`f1-telemetry` (car/pos); the session's bucket is tombstoned on `SessionStatus = Ends`, not on
`Finalised` (minutes of tape follow the latter). The tape topic itself is **deliberately not sunk to
ClickHouse** — it is Kafka-durable and the board is re-runnable over it under a fresh application id.
All four data topics are created with **`retention.ms=-1`**, which is correctness and not hoarding:
backfilled records carry event times months in the past and Kafka's time retention judges a segment
by its max record timestamp. The three dashboards share one mechanism — an **as-of cursor** in plain
SQL (`event_time <= cursor`, no lower bound) that makes live, scrub, and animated replay the same
query with no timestamp ever rewritten.

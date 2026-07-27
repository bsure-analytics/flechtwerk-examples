"""F1 Live Timing — a whole season of official live timing, replayed from tape.

Two host processes over Formula 1's live-timing static archive:

* ``ingest`` — an Extractor. For each session on the compacted ``f1-sessions`` config topic it
  range-reads the next chunk of every feed the session publishes, frames whole lines, merges
  them across feeds under a watermark, and emits each as one event-timed record on
  ``f1-timing``. Its resume cursor is a **per-feed byte offset**, which makes backfilling a
  finished file, tailing a growing one, and recovering from a crash the same code path.
* ``timing`` — a Transformer. The feed sends *partial patches*, never a leaderboard, so this
  folds them into per-driver keyed state and emits wide snapshots: ``f1-status`` (standings,
  weather, clock, heartbeat), ``f1-events`` (laps, pit stops, flag periods, race control,
  dimensions), ``f1-telemetry`` (car and position samples).

Why this example exists — three shapes the others don't teach:

1. **The tape is the stream.** A byte offset into an append-only file is a genuine
   exactly-once source position, committed in the same transaction as the records it accounts
   for. Downtime therefore costs timeliness and never data.
2. **Delta accumulation into a materialized view.** The leaderboard *is* keyed state; snapshots
   are its materialization, emitted only when a patch actually moved something.
3. **A replayable, deterministic demo.** Every other example depends on what the world is doing
   right now. This one reproduces an identical stream on demand — run it on a quiet Tuesday and
   get the Hungarian Grand Prix, byte for byte. It is the best example to learn the framework
   with, because a bug reproduces exactly.

Secondary: a **broadcast SCD join** (a 585-byte flag feed tags every lap and every snapshot),
**as-of dashboards** (live, scrub, and animated replay are one Grafana mechanism over true event
time — no timestamp is ever rewritten), and **event-time anchoring** (deriving absolute UTC for
a stream whose native clock is a session-relative offset).

Keyless public data from an **unofficial, undocumented** endpoint, read-only. Not associated
with Formula 1 — see ``README.md``.
"""
from .attributes import (
    EVENTS_TOPIC,
    SESSIONS_TOPIC,
    STATUS_TOPIC,
    TELEMETRY_TOPIC,
    TIMING_TOPIC,
)
from .board import indexed, parse_duration_ms, parse_gap, severity_of
from .tape import Fetch, Line, anchor, event_time, frame, frontier, merge, watermark

__all__ = [
    "EVENTS_TOPIC",
    "Fetch",
    "Line",
    "SESSIONS_TOPIC",
    "STATUS_TOPIC",
    "TELEMETRY_TOPIC",
    "TIMING_TOPIC",
    "anchor",
    "event_time",
    "frame",
    "frontier",
    "indexed",
    "merge",
    "parse_duration_ms",
    "parse_gap",
    "severity_of",
    "watermark",
]

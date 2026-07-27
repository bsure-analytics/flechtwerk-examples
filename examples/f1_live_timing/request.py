"""Request (or retire) sessions to ingest — validated, then written to ``f1-sessions``.

    uv run poe request-f1 season                  # every competitive session of the current year
    uv run poe request-f1 season 2025 --practice  # ... including practice
    uv run poe request-f1 follow                  # watch the index; pick up new sessions unattended
    uv run poe request-f1 session 2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/
    uv run poe request-f1 retire 2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/
    uv run poe request-f1 retire season-2026

The ops step that replaces a hard-coded seed (the ADS-B ``request-region`` / wildfire
``request-wildfire`` pattern). It reads the archive's own index, **prints exactly what it is
about to seed and what that will cost**, and only then writes.

**Why it prints before it writes.** Asking for a season is asking for a gigabyte. The archive
lists pre-season testing days alongside races, a sprint weekend has five sessions and a normal
one has five different ones, and the two ``.z`` telemetry feeds are half the bytes — so the
difference between what you meant and what you asked for is easily 10×. The listing shows the
session count by type, the estimated download, and which sessions are already being ingested.

**Three ways to ask, in ascending order of laziness.**

* ``session <path>`` — one session, by path. The **escape hatch**: the path format is
  deterministic (``<year>/<date>_<Meeting_Name>/<date>_<Session_Name>/``), so a session can be
  requested *before* the index lists it, and the ingest stage treats the resulting 404 as "not
  started yet" rather than as an error. This is how you follow a live weekend by hand.
* ``season [year]`` — every session of a year whose name matches (Race, Sprint, Qualifying,
  Sprint Qualifying by default; ``--practice`` and ``--testing`` opt in).
* ``follow [year]`` — one record, and from then on the ingest stage watches the index itself and
  self-produces a config record per newly-listed session. Unattended live coverage.

Either kind is retired with ``retire <path>`` or ``retire season-<year>``, which writes a
compacted-topic tombstone.

Any producer works too (Kafbat UI included) — a bare ``{"path": "…"}`` is a complete request,
since the stage derives the rest. This is just the convenient, checked one.
"""
import asyncio
import json
import sys
from collections import Counter
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from flechtwerk import Config, Event

from examples._setup import quiet_fresh_topic_produce_race

from .attributes import KIND, MEETING, PATH, SESSION_NAME, SESSIONS_TOPIC, TELEMETRY, TYPES, YEAR
from .ingest import (
    ARCHIVE_BASE_URL,
    DEFAULT_TYPES,
    FOLLOW_KIND,
    FOLLOW_PREFIX,
    SESSION_KIND,
    USER_AGENT,
    session_config,
)

BOOTSTRAP_SERVERS = "localhost:9092"

CURRENT_YEAR = 2026
"""The season ``season`` and ``follow`` default to. A deliberately *pinned* constant rather than
``date.today().year``: this repo pins its whole world (framework version, image tags, Python),
and a tool whose behaviour silently changes at midnight on New Year's Eve is not in that spirit.
Bump it with the rest of the pins."""

PRACTICE_TYPES = ("Practice 1", "Practice 2", "Practice 3")
"""Session names ``--practice`` adds. Three quarters of a normal weekend's sessions and, for a
leaderboard, much the least interesting — hence opt-in."""

TESTING_TYPES = ("Day 1", "Day 2", "Day 3")
"""Session names ``--testing`` adds: the pre-season testing days, which the index files under
their own meetings and types as ``Practice``. Five-hour tapes of cars circulating without a
classification — genuinely useful for telemetry, useless for a race wall."""

MB_PER_SESSION = {"Race": 22, "Sprint": 14, "Qualifying": 12, "Sprint Qualifying": 12}
"""Rough download per session in MB **with telemetry on**, measured against one 2026 race
(21.4 MB across 14 feeds, of which 13.8 MB is the two ``.z`` feeds). Used only for the estimate
the tool prints — being wrong by 30 % still tells you whether you are asking for 200 MB or 2 GB,
which is the decision at hand."""

DEFAULT_MB = 18
"""Estimate for a session type not in :data:`MB_PER_SESSION` (practice, testing)."""

TELEMETRY_SHARE = 0.64
"""Fraction of a session's bytes that the two ``.z`` feeds account for, so the estimate can
respond to ``--no-telemetry``. Measured: 13.8 MB of 21.4 MB."""


async def year_sessions(year: int) -> list[dict[str, Any]]:
    """Every session the archive's index lists for a year, with its meeting's name attached.

    A **403 is a real answer, not a bug**: access is granted per season and the archive already
    refuses 2022 (and any future year), so the message says so plainly instead of dumping an
    HTTP error. That is also the standing argument for the data topics' unlimited retention — a
    season you have ingested stays ingested even if the door closes later.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True,
                                 headers={"User-Agent": USER_AGENT}) as client:
        response = await client.get(f"{ARCHIVE_BASE_URL}/{year}/Index.json",
                                    headers={"Accept-Encoding": "identity"})
    if response.status_code == 403:
        raise SystemExit(
            f"\n  ✗ The archive refuses {year} with HTTP 403.\n"
            "    Access is granted per season and this one is not public — 2022 is famously\n"
            "    gated, and a year that has not started yet answers the same way. Try another\n"
            "    year (2018–2021, 2023–2026 were readable when this example was written).")
    response.raise_for_status()
    index = json.loads(response.content.decode("utf-8-sig"))
    return [{**session, "MeetingName": meeting.get("Name")}
            for meeting in index.get("Meetings") or []
            for session in meeting.get("Sessions") or []]


async def requested() -> dict[str, Config]:
    """Everything currently on the compacted config topic, as ``key -> Config``.

    Reads from the beginning with **no group id** — a read-only peek that must never commit
    offsets or disturb the extractor's own consumption — and keeps the last value per key, so
    compaction semantics are reproduced: a later record wins, a tombstone removes the entry. An
    absent topic (setup not run yet) yields ``{}``.
    """
    # Subscribing via the constructor rather than assign() lets aiokafka own the assignment,
    # which is the form the integration tests use; a group-less consumer built with assign()
    # raises CancelledError out of stop() from its coordinator's internal reset task.
    consumer = AIOKafkaConsumer(SESSIONS_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
                                group_id=None, enable_auto_commit=False,
                                auto_offset_reset="earliest")
    await consumer.start()
    try:
        assignment = list(consumer.assignment())
        if not assignment:
            return {}  # topic absent — setup has not run yet
        starts, ends = (await consumer.beginning_offsets(assignment),
                        await consumer.end_offsets(assignment))
        pending = sum(ends[tp] - starts[tp] for tp in assignment)
        if pending <= 0:
            return {}
        latest: dict[str, dict | None] = {}
        while pending > 0:
            batch = await consumer.getmany(timeout_ms=2000)
            if not batch:
                break  # nothing more forthcoming; take what we have
            for records in batch.values():
                for record in records:
                    pending -= 1
                    latest[record.key.decode()] = json.loads(record.value) if record.value else None
        return {key: Config.wrap(raw) for key, raw in latest.items() if raw is not None}
    finally:
        await consumer.stop()


def wanted_names(*, practice: bool, testing: bool) -> tuple[str, ...]:
    """The session names to select, given the opt-in flags."""
    return (DEFAULT_TYPES + (PRACTICE_TYPES if practice else ())
            + (TESTING_TYPES if testing else ()))


def estimate_mb(sessions: list[dict[str, Any]], *, telemetry: bool) -> int:
    """Rough total download for a set of sessions, in MB — see :data:`MB_PER_SESSION`."""
    total = sum(MB_PER_SESSION.get(session.get("Name") or "", DEFAULT_MB) for session in sessions)
    return round(total if telemetry else total * (1 - TELEMETRY_SHARE))


def _describe(sessions: list[dict[str, Any]], *, telemetry: bool) -> None:
    """Print what is about to be seeded, grouped by type, with the download estimate."""
    by_name = Counter(session.get("Name") or "?" for session in sessions)
    meetings = {session.get("MeetingName") for session in sessions}
    print(f"\n{len(sessions)} session(s) across {len(meetings)} meeting(s):")
    for name, count in sorted(by_name.items()):
        print(f"  {count:>3} × {name}")
    print(f"\nEstimated download: ~{estimate_mb(sessions, telemetry=telemetry)} MB "
          f"(telemetry {'ON' if telemetry else 'OFF'}"
          f"{'' if telemetry else ' — the two .z feeds are skipped entirely'}).")
    print("The ingest stage paces itself by a per-poll line budget, so a session backfills in\n"
          "minutes and then costs nothing: a completed session's poll issues no request at all.")


def _warn_if_already_requested(keys: list[str], existing: dict[str, Config]) -> None:
    """Say which of these are already on the topic, and what re-requesting them does.

    Re-requesting is **cheap and safe**, and worth saying so: the config record is replaced, but
    the ingest stage's *cursor state* is keyed separately and untouched, so a completed session
    stays completed rather than re-downloading. The one thing that does change behaviour is
    flipping ``telemetry`` on for a session already finished — the cursors are past the end, so
    the two ``.z`` feeds will not be picked up retroactively.
    """
    overlap = sorted(key for key in keys if key in existing)
    if not overlap:
        return
    print(f"\n  ℹ️  {len(overlap)} of these are already requested — the record is replaced, but "
          f"the ingest\n      cursors are not, so a finished session will not re-download. "
          f"(Turning telemetry ON\n      for an already-finished session has no retroactive "
          f"effect for the same reason: its\n      cursors are past the end. Retire it and "
          f"re-request under a fresh application id.)")
    for key in overlap[:5]:
        print(f"        {key}")
    if len(overlap) > 5:
        print(f"        … and {len(overlap) - 5} more")


async def _write(records: dict[str, Event | None]) -> None:
    """Produce config records (or tombstones) keyed by session path / follow key."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for key, record in records.items():
                await producer.send_and_wait(
                    SESSIONS_TOPIC, key=key.encode(),
                    value=None if record is None else json.dumps(record.raw).encode())
    finally:
        await producer.stop()


async def request_season(year: int, *, practice: bool, testing: bool, telemetry: bool) -> None:
    """Seed one config record per matching session of a season."""
    names = {name.casefold() for name in wanted_names(practice=practice, testing=testing)}
    sessions = [session for session in await year_sessions(year)
                if (session.get("Name") or "").casefold() in names]
    if not sessions:
        raise SystemExit(f"No session of {year} matches "
                         f"{', '.join(wanted_names(practice=practice, testing=testing))}.")
    _describe(sessions, telemetry=telemetry)
    _warn_if_already_requested([session["Path"] for session in sessions], await requested())
    await _write({session["Path"]: Event(session_config(session, year, telemetry=telemetry))
                  for session in sessions})
    print(f"\nRequested {len(sessions)} session(s) of {year}.")


async def request_follow(year: int, *, practice: bool, testing: bool, telemetry: bool) -> None:
    """Seed the season-follow record: the stage watches the index and requests sessions itself.

    Note what this does **not** do: it does not seed the sessions already listed. A follow record
    picks up sessions the *stage* has not seen before, and its first index read will find the
    whole season at once — so following 2026 today backfills 2026 as a side effect. That is
    usually what you want; ``season`` exists for when you want to choose.
    """
    names = wanted_names(practice=practice, testing=testing)
    key = f"{FOLLOW_PREFIX}{year}"
    record = Event({KIND: FOLLOW_KIND, YEAR: year, TYPES: list(names), TELEMETRY: telemetry})
    print(f"\nFollowing {year}: the ingest stage will re-read the season index every few "
          f"minutes and\nrequest each newly-listed session matching "
          f"{', '.join(names)} (telemetry {'ON' if telemetry else 'OFF'}).")
    print("Its FIRST read finds the whole season at once, so this also backfills everything\n"
          f"listed so far (~{estimate_mb(await year_sessions(year), telemetry=telemetry)} MB "
          "if that is all of it — use 'season' instead to pick).")
    await _write({key: record})
    print(f"\nRequested {key!r}.")


async def request_session(path: str, *, telemetry: bool) -> None:
    """Seed one session by path, listed or not.

    A path is **not validated against the index**, on purpose: requesting a session before the
    archive publishes it is the supported way to be ready for a live weekend, and the stage
    handles the resulting 404 as "not started yet". What is checked is the *shape* — a path with
    no trailing slash silently produces URLs like ``…_RaceIndex.json``, which 404 forever and
    look exactly like a session that has not started.
    """
    if not path.endswith("/") or path.count("/") != 3:
        raise SystemExit(
            f"\n  ✗ {path!r} does not look like a session path.\n"
            "    Expected <year>/<date>_<Meeting_Name>/<date>_<Session_Name>/ — three slashes,\n"
            "    trailing slash included, exactly as the index publishes it. For example:\n"
            "      2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/")
    year = int(path.split("/", 1)[0])
    listed = {session["Path"]: session for session in await year_sessions(year)}
    if (session := listed.get(path)) is not None:
        print(f"\n{path}\n  listed as {session.get('MeetingName')} — {session.get('Name')} "
              f"(key {session.get('Key')}, starts {session.get('StartDate')} "
              f"{session.get('GmtOffset')})")
        record = Event(session_config(session, year, telemetry=telemetry))
    else:
        print(f"\n{path}\n  NOT in the {year} index yet — seeding it anyway. The ingest stage "
              f"treats a 404 as\n  'not started', logs it once, and keeps polling; it will pick "
              f"the session up the moment\n  the archive publishes it.")
        record = Event({KIND: SESSION_KIND, PATH: path, YEAR: year, TELEMETRY: telemetry})
    _warn_if_already_requested([path], await requested())
    await _write({path: record})
    print(f"\nRequested {path!r} (telemetry {'ON' if telemetry else 'OFF'}).")


async def retire(key: str) -> None:
    """Tombstone one config record — a session path, or ``season-<year>``.

    Compacted-topic tombstone: the extractor's config bootstrap treats an empty value as a
    deletion, so the target drops out of every instance's active set on the next config drain.
    The ingest stage's cursor state is **not** removed (it lives in the changelog, keyed by the
    same path), so re-requesting the session resumes where it stopped rather than starting over.
    """
    await _write({key: None})
    print(f"Retired {key!r} (tombstone written)")


async def show() -> None:
    """List what is currently requested — the no-argument default."""
    existing = await requested()
    if not existing:
        print("Nothing requested yet. Try:  uv run poe request-f1 season")
        return
    follows = {key: config for key, config in existing.items()
               if config.get(KIND) == FOLLOW_KIND}
    sessions = {key: config for key, config in existing.items() if key not in follows}
    for key, config in sorted(follows.items()):
        print(f"{key:<22} follow {config.get(YEAR)} — {', '.join(config.get(TYPES) or DEFAULT_TYPES)}"
              f" (telemetry {'ON' if config.get(TELEMETRY) else 'OFF'})")
    by_meeting: dict[str, list[Config]] = {}
    for config in sessions.values():
        by_meeting.setdefault(config.get(MEETING) or "?", []).append(config)
    print(f"{len(sessions)} session(s) requested across {len(by_meeting)} meeting(s):")
    for meeting, configs in sorted(by_meeting.items()):
        names = ", ".join(sorted(config.get(SESSION_NAME) or config[PATH] for config in configs))
        print(f"  {meeting:<28} {names}")


def main() -> None:
    argv = sys.argv[1:]
    flags = {arg for arg in argv if arg.startswith("--")}
    positional = [arg for arg in argv if not arg.startswith("--")]
    unknown = flags - {"--practice", "--testing", "--no-telemetry"}
    if unknown:
        sys.exit(f"unknown flag(s): {', '.join(sorted(unknown))}")
    options = {"practice": "--practice" in flags, "testing": "--testing" in flags,
               "telemetry": "--no-telemetry" not in flags}

    if not positional:
        asyncio.run(show())
        return
    command, rest = positional[0], positional[1:]
    if command == "season" and len(rest) <= 1:
        asyncio.run(request_season(int(rest[0]) if rest else CURRENT_YEAR, **options))
        return
    if command == "follow" and len(rest) <= 1:
        asyncio.run(request_follow(int(rest[0]) if rest else CURRENT_YEAR, **options))
        return
    if command == "session" and len(rest) == 1:
        asyncio.run(request_session(rest[0], telemetry=options["telemetry"]))
        return
    if command == "retire" and len(rest) == 1:
        asyncio.run(retire(rest[0]))
        return
    sys.exit("usage: python -m examples.f1_live_timing.request "
             "[season [<year>] | follow [<year>] | session <path> | retire <path|season-<year>>]\n"
             "       flags: --practice --testing --no-telemetry\n"
             "       (no arguments lists what is currently requested)")


if __name__ == "__main__":
    main()

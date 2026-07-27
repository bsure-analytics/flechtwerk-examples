"""The board builder — tape lines in, a leaderboard and a session's story out.

Stage 2. It consumes ``f1-timing`` (every line of every feed, keyed by the session path, so
one task owns a session's whole stream in tape order) and holds the session's accumulated
state in one keyed bucket. The arithmetic lives in the framework-free :mod:`.board`; this
module is the wiring: decode, dispatch on the feed, emit, persist.

**One input, three outputs, split by cadence.** ``f1-status`` is continuous — a wide
per-driver snapshot whenever a patch actually changed something about that driver, plus
weather, the session clock, and a tape heartbeat. ``f1-events`` is sparse and meaningful —
laps, pit stops, flag periods, race-control messages, the two dimension upserts.
``f1-telemetry`` is the firehose, exploded per car per sample. Splitting by cadence is what
lets one dashboard refresh a leaderboard every ten seconds while another keeps a season's lap
history without a TTL.

**Nothing is emitted before ``SessionInfo``.** Every output row carries the numeric
``session_key`` the dashboards filter on, and that arrives in the tape's very first line —
``SessionInfo`` sits at offset ~0 in every session ever recorded. Records that beat it (a
heartbeat or two, at most) are folded into state and emit nothing, rather than being emitted
with a fabricated key or dropped outright. This is the one gate; everything downstream may
assume the key is present.

**Emit-on-change, not emit-per-line.** 88 % of ``TimingData`` lines are marshalling-segment
status updates that touch nothing the board promotes. A row per line would be seven eighths
duplicates; a row per *changed* driver projection is the leaderboard actually moving. Hence
:func:`_standings` is built before and after each fold and compared — the projection is both
the wire format and the change detector, so the two can never disagree.

**The session's end is two records, not one.** ``SessionStatus`` reaches ``Finalised`` (results
official) and then, minutes later, ``Ends`` (the tape is over). The official classification is
emitted on the first and the state bucket is tombstoned on the second — so the changelog stays
bounded across a 30-session backfill while the final snapshot still lands, and the tape lines
that follow ``Finalised`` (nine minutes' worth, in one practice session) still update state.
A record arriving after the tombstone rebuilds from empty, finds no ``SessionInfo``, and emits
nothing: the gate above doubles as the guard here.

Every instant used is event time, read off the tape record itself, so :func:`run_board` is pure
and I/O-free and the logic tier drives every branch.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Final

from flechtwerk import Event, IncomingMessage, Message, State, transformer

from . import board, tape
from .attributes import (
    AIR_TEMP,
    BEST_LAP_MS,
    BRAKE,
    C_EXTRAPOLATING,
    C_LAP,
    C_REMAINING,
    C_TOTAL,
    CATCHING,
    CATEGORY,
    CIRCUIT,
    CLEAN,
    CLOCK,
    CODE,
    COUNTRY,
    D_BEST_LAP,
    D_CATCHING,
    D_COLOUR,
    D_COMPOUND,
    D_FIRST_NAME,
    D_FULL_NAME,
    D_GAP,
    D_IN_PIT,
    D_INTERVAL,
    D_LAPS,
    D_LAST_LAP,
    D_LAST_LAP_OB,
    D_LAST_LAP_PB,
    D_LAST_NAME,
    D_LINE,
    D_PIT_COUNT,
    D_PIT_OUT,
    D_POSITION,
    D_REFERENCE,
    D_RETIRED,
    D_SECTORS,
    D_SPEEDS,
    D_STINT,
    D_STOPPED,
    D_TEAM,
    D_TLA,
    D_TYRE_AGE,
    DRIVERS,
    END_UTC,
    ENDED_AT,
    ENTITY_ID,
    ENTITY_TYPE,
    EVENT_TIME,
    EVENTS_TOPIC,
    EXTRAPOLATING,
    FEED,
    FINALISED,
    FIRST_NAME,
    FLAG,
    FULL_NAME,
    GAP_LAPS,
    GAP_RAW,
    GAP_S,
    GEAR,
    GMT_OFFSET,
    HUMIDITY,
    IN_PIT,
    INTERVAL_LAPS,
    INTERVAL_RAW,
    INTERVAL_S,
    KIND,
    LABEL,
    LAP,
    LAP_MS,
    LAPS_COMPLETED,
    LAST_LAP_MS,
    LAST_LAP_OVERALL_BEST,
    LAST_LAP_PERSONAL_BEST,
    LAST_NAME,
    LINE,
    LOCATION,
    M_CIRCUIT,
    M_COUNTRY,
    M_END_UTC,
    M_GMT_OFFSET,
    M_KEY,
    M_LABEL,
    M_LOCATION,
    M_MEETING,
    M_NAME,
    M_START_LOCAL,
    M_START_UTC,
    M_STATUS,
    M_TYPE,
    M_YEAR,
    MEETING,
    MESSAGE,
    META,
    OVERTAKES,
    PAYLOAD,
    PIT_COUNT,
    PIT_LANE_S,
    PIT_OUT,
    PITTED,
    POINTS,
    POSITION,
    PREDICTED_POINTS,
    PREDICTED_POSITION,
    PRESSURE,
    RACING_NUMBER,
    RAINFALL,
    REFERENCE,
    REMAINING_S,
    RETIRED,
    RPM,
    SCOPE,
    SECTOR,
    SECTOR1_MS,
    SECTOR2_MS,
    SECTOR3_MS,
    SESSION,
    SESSION_KEY,
    SESSION_NAME,
    SESSION_TYPE,
    SEVERITY,
    SPEED,
    SPEED_FL,
    SPEED_I1,
    SPEED_I2,
    SPEED_ST,
    START_LOCAL,
    START_UTC,
    STARTED_AT,
    STATIONARY_S,
    STATUS,
    STATUS_TOPIC,
    STINT,
    STOPPED,
    T_CODE,
    T_LABEL,
    T_SEVERITY,
    T_STARTED,
    TEAM,
    TEAM_COLOUR,
    TELEMETRY_TOPIC,
    THROTTLE,
    TIMING_TOPIC,
    TLA,
    TOTAL_LAPS,
    TRACK,
    TRACK_STATUS,
    TRACK_TEMP,
    TYRE_AGE,
    TYRE_COMPOUND,
    UTC,
    WIND_DIRECTION,
    WIND_SPEED,
    X,
    Y,
    YEAR,
    Z,
)

log = logging.getLogger(__name__)

STANDINGS: Final = "standings"
"""``KIND`` of a wide per-driver leaderboard snapshot on ``f1-status``."""
WEATHER: Final = "weather"
"""``KIND`` of a weather row on ``f1-status``."""
CLOCK_KIND: Final = "clock"
"""``KIND`` of a session-clock row on ``f1-status`` — lap counter and session clock. Without it
the wall's lap-X-of-Y and time-remaining panels have no data source at all."""
HEARTBEAT: Final = "heartbeat"
"""``KIND`` of a tape-freshness beat on ``f1-status``.

The ``wildfire_sweeps`` rationale restated: a freshness panel built on standings alone cannot
tell "the session is under a red flag and nothing is moving" from "the ingest stage died".
``Heartbeat`` beats every ~15 s regardless of what is happening on track, so it can."""

LAP_KIND: Final = "lap"
"""``KIND`` of a completed-lap event on ``f1-events`` — the broadcast-join output, carrying the
worst flag state the lap ran under and whether it was therefore clean."""
PIT: Final = "pit"
"""``KIND`` of a pit-stop event on ``f1-events``."""
TRACK_PERIOD: Final = "track_period"
"""``KIND`` of a flag-period event on ``f1-events``.

Emitted **twice per period**: once when it opens (with no ``ended_at``, so an annotation layer
has a row the moment a flag flies) and once when it closes. The closed row supersedes the open
one by version in ClickHouse — a ``ReplacingMergeTree`` keyed on the period's start."""
RACE_CONTROL: Final = "race_control"
"""``KIND`` of a race-control message on ``f1-events``."""
OVERTAKE: Final = "overtake"
"""``KIND`` of an overtake record on ``f1-events``."""
CHAMPIONSHIP: Final = "championship"
"""``KIND`` of a championship-standings row on ``f1-events``."""
SESSION_KIND: Final = "session"
"""``KIND`` of the session dimension upsert on ``f1-events`` — what the dashboards' session
picker and its replay links are built from."""
DRIVER: Final = "driver"
"""``KIND`` of a driver dimension upsert on ``f1-events``."""
CAR: Final = "car"
"""``KIND`` of a car-telemetry sample on ``f1-telemetry``."""
POS: Final = "pos"
"""``KIND`` of a position sample on ``f1-telemetry``."""

FINALISED_STATUS: Final = "Finalised"
"""``SessionStatus`` value meaning the classification is official — when the final standings
snapshot is emitted and the last flag period is closed. Verified as the second-to-last status
of a race, a qualifying session, and a practice session alike."""

ENDS_STATUS: Final = "Ends"
"""``SessionStatus`` value meaning the tape is over — when the session's bucket is tombstoned.

Deliberately this and not ``Finalised``: minutes of tape follow the latter, and tombstoning
early would discard the state the remaining lines still update. A tape that never reaches
``Ends`` (an abandoned session) simply keeps its bucket — bounded at ~50–100 KB, and reset by
``poe clean`` like everything else."""


# --- projections (each is both a wire record and, by comparison, a change detector) ---

def _standings(number: str, entry: dict, *, track: str | None) -> dict[Any, Any]:
    """One driver's wide leaderboard snapshot — pure.

    Optional fields are **omitted when absent**, never defaulted: an unknown gap must reach
    ClickHouse as NULL, because a fabricated ``0.0`` reads as "level with the leader".
    """
    gap_s, gap_laps = board.parse_gap(entry.get(D_GAP))
    interval_s, interval_laps = board.parse_gap(entry.get(D_INTERVAL))
    sectors = (list(entry.get(D_SECTORS) or []) + [None, None, None])[:3]
    speeds = entry.get(D_SPEEDS) or {}
    return _present({
        RACING_NUMBER: number,
        TLA: entry.get(D_TLA),
        TEAM: entry.get(D_TEAM),
        POSITION: entry.get(D_POSITION),
        LINE: entry.get(D_LINE),
        GAP_RAW: entry.get(D_GAP),
        GAP_S: gap_s,
        GAP_LAPS: gap_laps,
        INTERVAL_RAW: entry.get(D_INTERVAL),
        INTERVAL_S: interval_s,
        INTERVAL_LAPS: interval_laps,
        CATCHING: entry.get(D_CATCHING),
        LAST_LAP_MS: entry.get(D_LAST_LAP),
        LAST_LAP_PERSONAL_BEST: entry.get(D_LAST_LAP_PB),
        LAST_LAP_OVERALL_BEST: entry.get(D_LAST_LAP_OB),
        BEST_LAP_MS: entry.get(D_BEST_LAP),
        SECTOR1_MS: sectors[0],
        SECTOR2_MS: sectors[1],
        SECTOR3_MS: sectors[2],
        SPEED_I1: speeds.get("I1"),
        SPEED_I2: speeds.get("I2"),
        SPEED_FL: speeds.get("FL"),
        SPEED_ST: speeds.get("ST"),
        IN_PIT: entry.get(D_IN_PIT),
        PIT_OUT: entry.get(D_PIT_OUT),
        RETIRED: entry.get(D_RETIRED),
        STOPPED: entry.get(D_STOPPED),
        PIT_COUNT: entry.get(D_PIT_COUNT),
        TYRE_COMPOUND: entry.get(D_COMPOUND),
        TYRE_AGE: entry.get(D_TYRE_AGE),
        STINT: entry.get(D_STINT),
        LAPS_COMPLETED: entry.get(D_LAPS),
        TRACK_STATUS: track,
    })


def _driver_row(number: str, entry: dict) -> dict[Any, Any]:
    """One driver's dimension fields — the identity half of the entry, pure."""
    return _present({
        RACING_NUMBER: number,
        TLA: entry.get(D_TLA),
        FULL_NAME: entry.get(D_FULL_NAME),
        FIRST_NAME: entry.get(D_FIRST_NAME),
        LAST_NAME: entry.get(D_LAST_NAME),
        TEAM: entry.get(D_TEAM),
        TEAM_COLOUR: entry.get(D_COLOUR),
        REFERENCE: entry.get(D_REFERENCE),
        LINE: entry.get(D_LINE),
    })


def _session_row(meta: dict) -> dict[Any, Any]:
    """The session dimension row from folded ``SessionInfo`` — pure.

    ``start_utc`` / ``end_utc`` come back out of state as ISO strings and are re-parsed here,
    because the ``DATETIME`` codec takes a real ``datetime`` (a ``State`` nests only JSON
    scalars, so the round trip through a string is unavoidable and explicit).
    """
    return _present({
        SESSION_KEY: meta.get(M_KEY),
        YEAR: meta.get(M_YEAR),
        MEETING: meta.get(M_MEETING),
        SESSION_NAME: meta.get(M_NAME),
        SESSION_TYPE: meta.get(M_TYPE),
        LABEL: meta.get(M_LABEL),
        START_LOCAL: meta.get(M_START_LOCAL),
        GMT_OFFSET: meta.get(M_GMT_OFFSET),
        START_UTC: _instant(meta.get(M_START_UTC)),
        END_UTC: _instant(meta.get(M_END_UTC)),
        CIRCUIT: meta.get(M_CIRCUIT),
        COUNTRY: meta.get(M_COUNTRY),
        LOCATION: meta.get(M_LOCATION),
        STATUS: meta.get(M_STATUS),
    })


def _present(fields: dict[Any, Any]) -> dict[Any, Any]:
    """The fields that actually have a value — the "absent, never zero" rule, in one place."""
    return {attribute: value for attribute, value in fields.items() if value is not None}


def _instant(raw: Any) -> datetime | None:
    """An ISO string from state back to an aware ``datetime``, or ``None``."""
    return tape.parse_utc(raw) if isinstance(raw, str) else None


# --- the fold ---

async def run_board(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Fold one tape line into the session's board — pure, I/O-free.

    Dispatches on the feed, emits whatever that line made true, and yields the updated
    ``State`` **last** so messages and state commit in one transaction.
    """
    value = msg.value
    session, feed = value[SESSION], value[FEED]
    at = value[EVENT_TIME]
    payload = value.raw.get(PAYLOAD)

    meta = dict(state.get(META) or {})
    drivers = {number: dict(entry) for number, entry in (state.get(DRIVERS) or {}).items()}
    track = dict(state.get(TRACK) or {})
    clock = dict(state.get(CLOCK) or {})
    finalised = bool(state.get(FINALISED))

    emissions: list[Message] = []
    tombstone = False

    def emit(topic: str, kind: str, fields: dict[Any, Any], *,
             when: datetime | None = None) -> None:
        """One output record: the kind, the session identity, the event time, the fields.

        Every row carries ``session`` (the path, for humans and for Kafbat) *and*
        ``session_key`` (the int the dashboards filter on) — the one pair every panel joins on.
        """
        instant = when or at
        emissions.append(Message(
            key=session, topic=topic, timestamp=instant,
            value=Event({KIND: kind, SESSION: session, SESSION_KEY: meta.get(M_KEY),
                         EVENT_TIME: instant, **fields})))

    def emit_standings(numbers: list[str], before: dict[str, dict]) -> None:
        """A snapshot per driver whose projection changed — the emit-on-change rule."""
        label = track.get(T_LABEL)
        for number in numbers:
            after = _standings(number, drivers.get(number, {}), track=label)
            if after != before.get(number):
                emit(STATUS_TOPIC, STANDINGS, after)

    def close_period(when: datetime) -> None:
        """Close the open flag period, if there is one."""
        if not track:
            return
        emit(EVENTS_TOPIC, TRACK_PERIOD, _present({
            CODE: track.get(T_CODE), LABEL: track.get(T_LABEL),
            SEVERITY: track.get(T_SEVERITY), STARTED_AT: _instant(track.get(T_STARTED)),
            ENDED_AT: when,
        }), when=when)

    # --- SessionInfo is the gate: it is the only feed that may act on empty state ---
    if feed == "SessionInfo":
        meta = board.session_meta(payload, meta)
        if meta.get(M_KEY) is not None:
            emit(EVENTS_TOPIC, SESSION_KIND, _session_row(meta))
    elif meta.get(M_KEY) is None:
        # Before SessionInfo: fold nothing, emit nothing. At most a heartbeat or two, and
        # emitting them without a session key would put unfilterable rows in every table.
        return
    elif feed == "SessionStatus":
        status = payload.get("Status") if isinstance(payload, dict) else None
        if isinstance(status, str) and status:
            meta[M_STATUS] = status
            emit(EVENTS_TOPIC, SESSION_KIND, _session_row(meta))
        if status in (FINALISED_STATUS, ENDS_STATUS) and not finalised:
            finalised = True
            for number in sorted(drivers):
                emit(STATUS_TOPIC, STANDINGS,
                     _standings(number, drivers[number], track=track.get(T_LABEL)))
            close_period(at)
            log.info("%s: classification official (%s) — %d driver(s) in the final snapshot",
                     session, status, len(drivers))
        if status == ENDS_STATUS:
            tombstone = True
    elif feed == "TrackStatus":
        track_next, drivers, changed = board.fold_track(track, drivers, payload, at)
        if changed:
            close_period(at)
            track = track_next
            emit(EVENTS_TOPIC, TRACK_PERIOD, _present({
                CODE: track.get(T_CODE), LABEL: track.get(T_LABEL),
                SEVERITY: track.get(T_SEVERITY), STARTED_AT: at,
            }))
            log.info("%s: track status → %s at %s", session, track.get(T_LABEL), at.isoformat())
    elif feed == "TimingData":
        touched = list(board.lines_of(payload))
        before = {number: _standings(number, drivers.get(number, {}), track=track.get(T_LABEL))
                  for number in touched}
        drivers, laps = board.fold_timing_data(
            drivers, payload,
            severity=track.get(T_SEVERITY, board.severity_of(board.ALL_CLEAR)),
            label=track.get(T_LABEL, board.ALL_CLEAR))
        emit_standings(touched, before)
        for fact in laps:
            emit(EVENTS_TOPIC, LAP_KIND, _present({
                RACING_NUMBER: fact["number"],
                TLA: drivers.get(fact["number"], {}).get(D_TLA),
                LAP: fact["lap"],
                LAP_MS: fact["lap_ms"],
                SECTOR1_MS: fact["sectors"][0],
                SECTOR2_MS: fact["sectors"][1],
                SECTOR3_MS: fact["sectors"][2],
                POSITION: fact["position"],
                TYRE_COMPOUND: fact["compound"],
                TYRE_AGE: fact["tyre_age"],
                STINT: fact["stint"],
                SPEED_ST: fact["speed_st"],
                PITTED: fact["pitted"],
                TRACK_STATUS: fact["track_status"],
                CLEAN: fact["clean"],
            }))
    elif feed == "TimingAppData":
        touched = list(board.lines_of(payload))
        before = {number: _standings(number, drivers.get(number, {}), track=track.get(T_LABEL))
                  for number in touched}
        drivers = board.fold_timing_app_data(drivers, payload)
        emit_standings(touched, before)
    elif feed == "DriverList":
        touched = list(board.lines_of(payload))
        before = {number: _driver_row(number, drivers.get(number, {})) for number in touched}
        drivers = board.fold_driver_list(drivers, payload)
        for number in touched:
            after = _driver_row(number, drivers.get(number, {}))
            if after != before.get(number):
                emit(EVENTS_TOPIC, DRIVER, after)
    elif feed in ("LapCount", "ExtrapolatedClock"):
        folded = board.fold_clock(clock, payload)
        if folded != clock:
            clock = folded
            emit(STATUS_TOPIC, CLOCK_KIND, _present({
                LAP: clock.get(C_LAP), TOTAL_LAPS: clock.get(C_TOTAL),
                REMAINING_S: clock.get(C_REMAINING),
                EXTRAPOLATING: clock.get(C_EXTRAPOLATING),
            }))
    elif feed == "WeatherData":
        if (row := board.weather_row(payload)):
            emit(STATUS_TOPIC, WEATHER, {
                AIR_TEMP: row.get("air_temp"), TRACK_TEMP: row.get("track_temp"),
                HUMIDITY: row.get("humidity"), PRESSURE: row.get("pressure"),
                RAINFALL: row.get("rainfall"), WIND_SPEED: row.get("wind_speed"),
                WIND_DIRECTION: row.get("wind_direction"),
            })
    elif feed == "Heartbeat":
        emit(STATUS_TOPIC, HEARTBEAT, {})
    elif feed == "RaceControlMessages":
        for entry in board.race_control_entries(payload):
            emit(EVENTS_TOPIC, RACE_CONTROL, _present({
                UTC: tape.parse_utc(entry["Utc"]) if isinstance(entry.get("Utc"), str) else None,
                CATEGORY: entry.get("Category"), FLAG: entry.get("Flag"),
                SCOPE: entry.get("Scope"), SECTOR: board.parse_int(entry.get("Sector")),
                LAP: board.parse_int(entry.get("Lap")), MESSAGE: entry.get("Message"),
                RACING_NUMBER: entry.get("RacingNumber"),
            }))
    elif feed == "PitStopSeries":
        for number, entry in board.series_entries(payload, "PitTimes"):
            stop = entry.get("PitStop") if isinstance(entry.get("PitStop"), dict) else {}
            # The stop carries its own absolute Timestamp — a real upstream clock, so it wins
            # over the tape offset for this record.
            when = tape.parse_utc(entry["Timestamp"]) if isinstance(entry.get("Timestamp"), str) else None
            emit(EVENTS_TOPIC, PIT, _present({
                RACING_NUMBER: stop.get("RacingNumber") or number,
                LAP: board.parse_int(stop.get("Lap")),
                STATIONARY_S: board.parse_float(stop.get("PitStopTime")),
                PIT_LANE_S: board.parse_float(stop.get("PitLaneTime")),
            }), when=when or at)
    elif feed == "OvertakeSeries":
        for number, entry in board.series_entries(payload, "Overtakes"):
            when = tape.parse_utc(entry["Timestamp"]) if isinstance(entry.get("Timestamp"), str) else None
            emit(EVENTS_TOPIC, OVERTAKE, _present({
                RACING_NUMBER: number, OVERTAKES: board.parse_int(entry.get("count")),
            }), when=when or at)
    elif feed == "ChampionshipPrediction":
        for row in board.championship_rows(payload):
            emit(EVENTS_TOPIC, CHAMPIONSHIP, _present({
                ENTITY_TYPE: row["entity_type"], ENTITY_ID: row["entity_id"],
                POSITION: row["position"], POINTS: row["points"],
                PREDICTED_POSITION: row["predicted_position"],
                PREDICTED_POINTS: row["predicted_points"],
            }))
    elif feed == "CarData.z":
        for utc, number, channels in board.car_samples(payload):
            emit(TELEMETRY_TOPIC, CAR, _present({
                RACING_NUMBER: number, RPM: channels.get("rpm"), SPEED: channels.get("speed"),
                GEAR: channels.get("gear"), THROTTLE: channels.get("throttle"),
                BRAKE: channels.get("brake"),
            }), when=(utc and tape.parse_utc(utc)) or at)
    elif feed == "Position.z":
        for stamp, number, sample in board.position_samples(payload):
            emit(TELEMETRY_TOPIC, POS, _present({
                RACING_NUMBER: number, X: sample["x"], Y: sample["y"], Z: sample["z"],
                STATUS: sample["status"],
            }), when=(stamp and tape.parse_utc(stamp)) or at)

    for message in emissions:
        yield message
    if tombstone:
        log.info("%s: tape ended — tombstoning the session's board state", session)
        yield State()
    else:
        yield State(_present({META: meta, DRIVERS: drivers, TRACK: track, CLOCK: clock,
                              FINALISED: finalised or None}))


@transformer(input_topics=[TIMING_TOPIC])
async def timing(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in run_board(state, msg):
        yield item


stage = timing
"""The stage the dispatcher runs (``python -m examples.f1_live_timing timing``)."""

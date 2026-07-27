"""The board core — folding partial patches into a leaderboard. Pure, I/O-free, clock-free.

**The feed never sends a leaderboard.** It sends "car 41's sector 3 was 24.849 and that was
lap 28", "car 31 is now +83.497 behind", "car 77 has stopped". Turning that into the wide
per-driver snapshot a pit wall reads means holding the accumulated state and applying each
patch to it — which is a **materialized view built by accumulation rather than by query**, and
the second reason this example exists.

Everything here is a fold: ``(state, payload) → state`` plus whatever facts fell out. No
framework types, no I/O, and no wall clock appears in any signature, so
``tests/logic_test.py`` drives every branch — including the ones that are awkward to reach
live, like a lap that runs entirely under a virtual safety car.

**Four shape traps the archive sets, all handled here.**

1. *List or index-keyed dict, for the same field.* A keyframe line sends ``Sectors`` as a
   three-element **list**; every later patch sends ``{"0": {...}}``, an index-keyed **dict**.
   ``RaceControlMessages`` does the same with ``Messages``, ``TimingAppData`` with ``Stints``,
   ``PitStopSeries`` with each driver's stop list. :func:`indexed` normalizes both, and it has
   to be applied everywhere rather than at one seam, because the shift happens
   *per field, mid-stream*.
2. *Numbers as strings.* ``Position`` is ``"1"``, ``AirTemp`` is ``"30.0"``,
   ``PitStopTime`` is ``"2.1"``, ``GridPos`` is ``"7"``. Every one goes through an explicit
   parse, and an unparseable value becomes **absent, never zero**.
3. *Durations as display strings.* ``"1:26.406"``. Stored as integer milliseconds, because a
   lap time is a quantity.
4. *Gaps as five different things in one field.* ``""``, ``"LAP 24"``, ``"+1.234"``, ``"1L"``,
   ``"15L"`` — see :func:`parse_gap`, which keeps the raw string alongside whatever it managed
   to extract.

**Why the flag tag is folded and not joined.** ``TrackStatus`` is a 585-byte feed with twelve
records per race; the leaderboard is tens of thousands of rows. Correlating them in SQL is a
range join against an interval table — and every lap-time comparison in the example needs it,
because a lap behind a safety car is not a lap. Folding the flag into per-driver state instead
turns it into a plain column, and "the worst flag this lap ran under" becomes a ``GROUP BY``
rather than a query problem. That is the broadcast-SCD-join teaching point, and
:data:`SEVERITY` is what makes "worst" well-defined.
"""
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from .attributes import (
    C_EXTRAPOLATING,
    C_LAP,
    C_REMAINING,
    C_TOTAL,
    D_BEST_LAP,
    D_CATCHING,
    D_COLOUR,
    D_COMPOUND,
    D_FIRST_NAME,
    D_FULL_NAME,
    D_GAP,
    D_IN_PIT,
    D_INTERVAL,
    D_LAP_PITTED,
    D_LAP_WORST,
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
    T_CODE,
    T_LABEL,
    T_SEVERITY,
    T_STARTED,
)

log = logging.getLogger(__name__)

SPEED_CHANNELS: Final = ("I1", "I2", "FL", "ST")
"""The four speed traps every ``Speeds`` block is keyed by: two intermediates, the finish
line, and the longest straight. Fixed order so the folded dict is stable."""

TRACK_LABELS: Final = {
    "1": "AllClear", "2": "Yellow", "3": "Yellow", "4": "SCDeployed", "5": "Red",
    "6": "VSCDeployed", "7": "VSCEnding",
}
"""Fallback names for ``TrackStatus.Status``, used only when the record omits ``Message``
(it never has, in practice). Codes ``1``, ``2``, ``6``, ``7`` were seen live in one race; ``4``
(safety car) and ``5`` (red flag) are what the ecosystem documents and are mapped defensively.
``3`` appears in some historical data as a second yellow code."""

SEVERITY: Final = {
    "AllClear": 0, "VSCEnding": 1, "Yellow": 2, "VSCDeployed": 3, "SCDeployed": 4, "Red": 5,
}
"""How bad each flag state is, as a **total order** — the thing the codes are not.

Ranking by code would be wrong in both directions: ``7`` (VSCEnding, the all-clear on its way)
would outrank ``6`` (VSC actually deployed), and ``5`` (red) would outrank nothing at all. A
lap's tag is the ``max`` over this order of everything that happened during it, so the order
has to mean something."""

UNKNOWN_SEVERITY: Final = 2
"""Severity assumed for a ``TrackStatus`` code this module has never seen.

Deliberately *not* 0: an unrecognized flag is far more likely to be some new kind of caution
than a green track, and treating it as clear would silently promote a compromised lap into
the pace analysis. Treating it as a yellow costs at worst the exclusion of a clean lap, which
is the harmless direction to be wrong in."""

ALL_CLEAR: Final = "AllClear"
"""The label of a green track — the only one a lap may be tagged with and still be ``clean``."""


# --- scalar parsing ---

def parse_int(raw: Any) -> int | None:
    """An upstream number as ``int``, or ``None`` — pure.

    Accepts the ``int`` the feed sometimes sends and the ``str`` it usually does. ``None`` for
    ``""``, for a non-numeric string, and for ``bool`` (which is an ``int`` in Python and never
    what a count means). Absent rather than zero, always: a leaderboard with a fabricated
    ``position = 0`` sorts a driver to the front of the field.
    """
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str) and (text := raw.strip()):
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def parse_float(raw: Any) -> float | None:
    """An upstream number as ``float``, or ``None`` — pure. See :func:`parse_int`.

    Every value goes through ``float()`` even when it arrives as an ``int``, because the
    ``FLOAT`` codec is exact-type and rejects ``int`` outright.
    """
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str) and (text := raw.strip()):
        try:
            return float(text)
        except ValueError:
            return None
    return None


_DURATION: Final = re.compile(r"^(?:(\d+):)?(\d+)(?:\.(\d{1,3}))?$")
"""``m:ss.mmm``, ``ss.mmm``, or bare seconds — a lap time and a sector time in one pattern."""


def parse_duration_ms(raw: Any) -> int | None:
    """``"1:26.406"`` → 86406, ``"23.042"`` → 23042, ``""`` → ``None`` — pure.

    Milliseconds because a duration has to be summable and comparable; the display string is
    a rendering, and keeping only the rendering is how a dashboard ends up unable to plot a
    lap time. Fractional digits are right-padded (``"1:22.5"`` is 82500 ms, not 82005), which
    is the one detail a naive split gets wrong.
    """
    if not isinstance(raw, str) or not (text := raw.strip()):
        return None
    match = _DURATION.match(text)
    if match is None:
        return None
    minutes, seconds, fraction = match.groups()
    millis = int((fraction or "").ljust(3, "0"))
    return ((int(minutes or 0) * 60) + int(seconds)) * 1000 + millis


def parse_clock_s(raw: Any) -> float | None:
    """``"01:59:59"`` → 7199.0 seconds — pure. The session clock's ``Remaining`` field."""
    if not isinstance(raw, str) or not (text := raw.strip()):
        return None
    parts = text.split(":")
    if not all(part.strip().isdigit() or "." in part for part in parts):
        return None
    total = 0.0
    for part in parts:
        value = parse_float(part)
        if value is None:
            return None
        total = total * 60 + value
    return total


_LAPS_DOWN: Final = re.compile(r"^(\d+)\s*L$", re.IGNORECASE)
"""``"1L"``, ``"15L"``, and the spaced ``"1 L"`` some seasons emit."""


def parse_gap(raw: Any) -> tuple[float | None, int | None]:
    """A gap or interval string as ``(seconds, laps_down)`` — pure, and both may be ``None``.

    The one field with five meanings, all real:

    * ``""`` — not known yet (the formation lap, a car in the garage) → ``(None, None)``.
    * ``"LAP 24"`` — what the *leader* shows while nobody is lapped: not a gap at all, it is
      the lap counter leaking into the gap column → ``(None, None)``.
    * ``"+1.234"`` — seconds → ``(1.234, None)``.
    * ``"1L"`` / ``"15L"`` — whole laps down → ``(None, 1)`` / ``(None, 15)``.

    Seconds and laps are deliberately **separate and never merged**: a lapped car's gap is not
    a duration, and inventing one (lap time × laps) is how a backmarker ends up sorted onto
    the podium. Anything unrecognized yields ``(None, None)`` — the raw string is kept on the
    record regardless, so a shape we failed to read stays visible instead of vanishing.
    """
    if not isinstance(raw, str) or not (text := raw.strip()):
        return None, None
    if (match := _LAPS_DOWN.match(text)) is not None:
        return None, int(match.group(1))
    if text.upper().startswith("LAP"):
        return None, None
    seconds = parse_float(text.lstrip("+"))
    return (seconds, None) if seconds is not None else (None, None)


def parse_gmt_offset(raw: Any) -> timedelta | None:
    """``"02:00:00"`` → 2 hours, ``"-05:00:00"`` → −5 hours — pure.

    The archive states a session's start in **local track time with no zone** and its offset
    separately; this is the half that makes the pair an instant.
    """
    if not isinstance(raw, str) or not (text := raw.strip()):
        return None
    sign = -1 if text.startswith("-") else 1
    seconds = parse_clock_s(text.lstrip("+-"))
    return None if seconds is None else timedelta(seconds=sign * seconds)


def indexed(value: Any) -> dict[int, Any]:
    """A list **or** an index-keyed dict as ``{index: entry}``, in index order — pure.

    The archive's single most pervasive shape trap. A feed's first line (its keyframe state)
    sends collections as JSON **arrays**; every subsequent patch sends the same collection as
    a **dict keyed by stringified index**, carrying only the entries that changed:
    ``"Sectors": [s0, s1, s2]`` becomes ``"Sectors": {"2": {"Value": "24.849"}}``. Both are
    the same collection, so both normalize here — and the caller merges by index rather than
    replacing, because the patch is genuinely partial.

    Non-integer keys (``PitLaneTimeCollection``'s ``"_deleted"``) are dropped: they are a
    different kind of message riding in the same field, and no fold here acts on deletions.
    """
    if isinstance(value, list):
        return dict(enumerate(value))
    if isinstance(value, dict):
        pairs = [(int(key), entry) for key, entry in value.items()
                 if isinstance(key, str) and key.lstrip("-").isdigit()]
        return dict(sorted(pairs))
    return {}


def lines_of(payload: Any) -> dict[str, Any]:
    """A per-driver payload's ``{racing_number: patch}`` map — pure.

    Most feeds wrap it in ``Lines`` (``TimingData``, ``TimingAppData``); ``DriverList`` keys
    drivers at the top level. Accepting both is one branch, and it removes a per-feed special
    case from every caller.
    """
    if not isinstance(payload, dict):
        return {}
    inner = payload.get("Lines", payload)
    if not isinstance(inner, dict):
        return {}
    return {key: value for key, value in inner.items() if isinstance(value, dict)}


# --- the folds ---

def session_meta(payload: Any, previous: dict[str, Any]) -> dict[str, Any]:
    """``SessionInfo`` folded into the session's identity — pure.

    The two computed fields are the point. :data:`~.attributes.M_START_UTC` /
    ``M_END_UTC`` turn the archive's *zone-less local* ``StartDate``/``EndDate`` plus
    ``GmtOffset`` into real instants, so no dashboard ever does timezone arithmetic in SQL;
    and ``M_LABEL`` is the display name (``"Hungarian Grand Prix — Race (2026)"``) built once
    here instead of reassembled in every panel.

    Later ``SessionInfo`` lines repeat the whole record with a changed ``SessionStatus``, so
    this merges rather than replaces — nothing already known is lost to a field the repeat
    happens to omit.
    """
    if not isinstance(payload, dict):
        return previous
    meeting = payload.get("Meeting") if isinstance(payload.get("Meeting"), dict) else {}
    country = meeting.get("Country") if isinstance(meeting.get("Country"), dict) else {}
    circuit = meeting.get("Circuit") if isinstance(meeting.get("Circuit"), dict) else {}
    meta = dict(previous)
    for key, value in (
        (M_KEY, parse_int(payload.get("Key"))),
        # SessionInfo restates the session's status on every repeat, and it must be folded here
        # too: the final SessionInfo and the SessionStatus that reports the same transition share
        # a recording offset, so a dimension row built without it would carry a *stale* status at
        # the very instant the results became official — and the two rows would then disagree at
        # identical (key, event_time), which is exactly the tie a ReplacingMergeTree cannot break.
        (M_STATUS, payload.get("SessionStatus")),
        (M_NAME, payload.get("Name")),
        (M_TYPE, payload.get("Type")),
        (M_MEETING, meeting.get("Name")),
        (M_CIRCUIT, circuit.get("ShortName")),
        (M_COUNTRY, country.get("Name")),
        (M_LOCATION, meeting.get("Location")),
        (M_START_LOCAL, payload.get("StartDate")),
        (M_GMT_OFFSET, payload.get("GmtOffset")),
    ):
        if value is not None:
            meta[key] = value
    if isinstance(path := payload.get("Path"), str) and "/" in path:
        year = parse_int(path.split("/", 1)[0])
        if year is not None:
            meta[M_YEAR] = year

    offset = parse_gmt_offset(meta.get(M_GMT_OFFSET))
    for key, field in ((M_START_UTC, "StartDate"), (M_END_UTC, "EndDate")):
        instant = _local_to_utc(payload.get(field), offset)
        if instant is not None:
            meta[key] = instant.isoformat()
    if meta.get(M_MEETING) and meta.get(M_NAME):
        year_suffix = f" ({meta[M_YEAR]})" if meta.get(M_YEAR) else ""
        meta[M_LABEL] = f"{meta[M_MEETING]} — {meta[M_NAME]}{year_suffix}"
    return meta


def _local_to_utc(local: Any, offset: timedelta | None) -> datetime | None:
    """A zone-less local timestamp plus its UTC offset as an aware UTC instant — pure."""
    if not isinstance(local, str) or offset is None:
        return None
    try:
        naive = datetime.fromisoformat(local)
    except ValueError:
        return None
    return (naive - offset).replace(tzinfo=timezone.utc)


def fold_driver_list(drivers: dict[str, dict], payload: Any) -> dict[str, dict]:
    """``DriverList`` folded into the driver dimension — pure.

    The first line is the full entry list; later lines are ``{"1": {"Line": 2}}`` patches as
    the running order changes, so this merges per driver rather than replacing the map.
    """
    merged = dict(drivers)
    for number, patch in lines_of(payload).items():
        entry = dict(merged.get(number, {}))
        for key, field in ((D_TLA, "Tla"), (D_FULL_NAME, "FullName"),
                           (D_FIRST_NAME, "FirstName"), (D_LAST_NAME, "LastName"),
                           (D_TEAM, "TeamName"), (D_COLOUR, "TeamColour"),
                           (D_REFERENCE, "Reference")):
            if isinstance(value := patch.get(field), str) and value:
                entry[key] = value
        if (line := parse_int(patch.get("Line"))) is not None:
            entry[D_LINE] = line
        merged[number] = entry
    return merged


def fold_timing_app_data(drivers: dict[str, dict], payload: Any) -> dict[str, dict]:
    """``TimingAppData`` folded into tyre state: compound, age, stint index — pure.

    Only the **latest** stint of each driver is kept, because that is what "current tyre"
    means; the full stint history is reconstructable from the ``pit`` and ``lap`` event streams
    (and lives on the tape regardless).

    Tyre age is ``TotalLaps + StartLaps``, and the second term is the one that matters:
    ``StartLaps`` counts laps the set had already done before this stint began, so a scrubbed
    set fitted at lap 64 starts life at age 3, not 0. Omitting it makes a used-tyre stint look
    mysteriously slow for its age — the classic F1-data mistake.
    """
    merged = dict(drivers)
    for number, patch in lines_of(payload).items():
        stints = indexed(patch.get("Stints"))
        if not stints:
            continue
        entry = dict(merged.get(number, {}))
        index, stint = max(stints.items())
        if not isinstance(stint, dict):
            continue
        if isinstance(compound := stint.get("Compound"), str) and compound:
            entry[D_COMPOUND] = compound
        total = parse_int(stint.get("TotalLaps"))
        start = parse_int(stint.get("StartLaps")) or 0
        if total is not None:
            entry[D_TYRE_AGE] = total + start
        entry[D_STINT] = index + 1
        merged[number] = entry
    return merged


_SCALAR_FIELDS: Final = (
    (D_GAP, "GapToLeader", str),
    (D_RETIRED, "Retired", bool),
    (D_IN_PIT, "InPit", bool),
    (D_PIT_OUT, "PitOut", bool),
    (D_STOPPED, "Stopped", bool),
)
"""``TimingData`` fields that map straight onto a driver-state key, with the type they must be
to be believed. A ``GapToLeader`` that arrived as a number, or an ``InPit`` that arrived as a
string, is a shape change worth ignoring rather than coercing."""


def fold_timing_data(drivers: dict[str, dict], payload: Any, *,
                     severity: int, label: str) -> tuple[dict[str, dict], list[dict]]:
    """``TimingData`` folded into the leaderboard — the central fold. Pure.

    Returns the new driver map and one fact per **completed lap**, each carrying the lap's
    number, times, position, tyre, and the flag tag accumulated over it.

    **Lap completion is detected from ``NumberOfLaps`` increasing**, and that choice is
    load-bearing. The feed lands ``NumberOfLaps``, ``LastLapTime``, the third sector, and the
    finish-line speed on **one line** — so a single patch carries both "a lap finished" and
    everything about it, with no correlation needed. Watching ``LastLapTime`` instead would
    misfire whenever the same time is re-sent with only a ``PersonalFastest`` flag attached.

    The flag tag is the ``max`` of :data:`SEVERITY` over the lap, accumulated in
    ``D_LAP_WORST`` — bumped here for the current status and, crucially, also by
    :func:`fold_track` for every driver the moment a flag changes. Sampling only at patch time
    would miss a caution that came and went between two of a driver's own updates. On
    completion the accumulator resets to whatever is flying *now*, so the tag always describes
    the lap that just ran and never the one starting.
    """
    merged = dict(drivers)
    laps: list[dict] = []
    for number, patch in lines_of(payload).items():
        entry = dict(merged.get(number, {}))
        if D_LAP_WORST not in entry:
            entry[D_LAP_WORST] = [severity, label]
        for key, field, kind in _SCALAR_FIELDS:
            if isinstance(value := patch.get(field), kind):
                entry[key] = value
        for key, field in ((D_POSITION, "Position"), (D_LINE, "Line"),
                           (D_PIT_COUNT, "NumberOfPitStops")):
            if (value := parse_int(patch.get(field))) is not None:
                entry[key] = value
        if isinstance(interval := patch.get("IntervalToPositionAhead"), dict):
            if isinstance(value := interval.get("Value"), str):
                entry[D_INTERVAL] = value
            if isinstance(catching := interval.get("Catching"), bool):
                entry[D_CATCHING] = catching
        if isinstance(last := patch.get("LastLapTime"), dict):
            if (millis := parse_duration_ms(last.get("Value"))) is not None:
                entry[D_LAST_LAP] = millis
            for key, field in ((D_LAST_LAP_PB, "PersonalFastest"), (D_LAST_LAP_OB, "OverallFastest")):
                if isinstance(flag := last.get(field), bool):
                    entry[key] = flag
        if isinstance(best := patch.get("BestLapTime"), dict):
            if (millis := parse_duration_ms(best.get("Value"))) is not None:
                entry[D_BEST_LAP] = millis
        _fold_sectors(entry, patch.get("Sectors"))
        _fold_speeds(entry, patch.get("Speeds"))

        # The lap accumulators: worst flag so far, and whether the pits were involved.
        entry[D_LAP_WORST] = max(entry[D_LAP_WORST], [severity, label])
        if entry.get(D_IN_PIT) or entry.get(D_PIT_OUT):
            entry[D_LAP_PITTED] = True

        completed = parse_int(patch.get("NumberOfLaps"))
        if completed is not None and completed > (merged.get(number, {}).get(D_LAPS) or 0):
            entry[D_LAPS] = completed
            laps.append(_lap_fact(number, entry, completed))
            entry[D_LAP_WORST] = [severity, label]
            entry[D_LAP_PITTED] = bool(entry.get(D_IN_PIT) or entry.get(D_PIT_OUT))
        merged[number] = entry
    return merged, laps


def _fold_sectors(entry: dict, sectors: Any) -> None:
    """Merge a ``Sectors`` patch into the entry's fixed three-slot sector list, in place.

    Kept a fixed-length list rather than a dict so the three slots always exist and a missing
    sector reads as ``None``. An empty ``Value`` **clears** the slot — that is not a no-op, it
    is the feed telling us the driver started a fresh lap (it happens on every pit exit).
    """
    updates = indexed(sectors)
    if not updates:
        return
    current = list(entry.get(D_SECTORS) or [None, None, None])
    current += [None] * (3 - len(current))
    for index, sector in updates.items():
        if not isinstance(sector, dict) or "Value" not in sector or not 0 <= index < 3:
            continue
        current[index] = parse_duration_ms(sector.get("Value"))
    entry[D_SECTORS] = current[:3]


def _fold_speeds(entry: dict, speeds: Any) -> None:
    """Merge a ``Speeds`` patch into the entry's four speed-trap readings, in place."""
    if not isinstance(speeds, dict):
        return
    current = dict(entry.get(D_SPEEDS) or {})
    for channel in SPEED_CHANNELS:
        trap = speeds.get(channel)
        if isinstance(trap, dict) and "Value" in trap:
            current[channel] = parse_int(trap.get("Value"))
    entry[D_SPEEDS] = current


def _lap_fact(number: str, entry: dict, lap: int) -> dict:
    """One completed lap, as the facts the ``lap`` event carries — pure.

    ``clean`` is the whole reason this record exists: a lap is comparable only if nothing was
    flying and the driver was not in the pit lane. Both conditions come from accumulators, so
    an in-lap, an out-lap, and a lap that spent ten seconds under a yellow are all correctly
    excluded from a pace analysis.
    """
    severity, label = entry[D_LAP_WORST]
    sectors = list(entry.get(D_SECTORS) or [None, None, None])
    return {
        "number": number,
        "lap": lap,
        "lap_ms": entry.get(D_LAST_LAP),
        "sectors": sectors[:3],
        "position": entry.get(D_POSITION),
        "compound": entry.get(D_COMPOUND),
        "tyre_age": entry.get(D_TYRE_AGE),
        "stint": entry.get(D_STINT),
        "speed_st": (entry.get(D_SPEEDS) or {}).get("ST"),
        "pitted": bool(entry.get(D_LAP_PITTED)),
        "track_status": label,
        "clean": severity == SEVERITY[ALL_CLEAR] and not entry.get(D_LAP_PITTED),
    }


def fold_track(track: dict[str, Any], drivers: dict[str, dict], payload: Any,
               started_at: datetime) -> tuple[dict[str, Any], dict[str, dict], bool]:
    """``TrackStatus`` folded into the open flag period — pure.

    Returns ``(track, drivers, changed)``. ``changed`` is what the caller emits a period pair
    on; a repeat of the same code is a no-op, so a feed that restates ``AllClear`` does not
    litter the annotation layer.

    **Every driver's lap accumulator is bumped here**, which is the non-obvious half of the
    SCD join: a caution that starts and ends between two of a driver's own timing patches
    would otherwise leave no trace on the lap it spoiled.
    """
    if not isinstance(payload, dict):
        return track, drivers, False
    code = payload.get("Status")
    if not isinstance(code, str) or not code:
        return track, drivers, False
    label = payload.get("Message")
    if not isinstance(label, str) or not label:
        label = TRACK_LABELS.get(code, f"Status{code}")
    severity = severity_of(label)
    if track.get(T_CODE) == code and track.get(T_LABEL) == label:
        return track, drivers, False
    bumped = {
        number: {**entry, D_LAP_WORST: max(entry.get(D_LAP_WORST) or [severity, label],
                                           [severity, label])}
        for number, entry in drivers.items()
    }
    return ({T_CODE: code, T_LABEL: label, T_SEVERITY: severity,
             T_STARTED: started_at.isoformat()}, bumped, True)


def severity_of(label: str) -> int:
    """A flag label's rank on the total order — :data:`UNKNOWN_SEVERITY` if unrecognized."""
    return SEVERITY.get(label, UNKNOWN_SEVERITY)


def fold_clock(clock: dict[str, Any], payload: Any) -> dict[str, Any]:
    """``LapCount`` or ``ExtrapolatedClock`` folded into the session clock — pure.

    Folded rather than passed through because ``LapCount`` sends ``TotalLaps`` **once**, with
    the first record, and then only ``CurrentLap`` deltas: reading the latest patch alone would
    lose the race distance for the rest of the session.
    """
    if not isinstance(payload, dict):
        return clock
    merged = dict(clock)
    for key, field in ((C_LAP, "CurrentLap"), (C_TOTAL, "TotalLaps")):
        if (value := parse_int(payload.get(field))) is not None:
            merged[key] = value
    if (remaining := parse_clock_s(payload.get("Remaining"))) is not None:
        merged[C_REMAINING] = remaining
    if isinstance(extrapolating := payload.get("Extrapolating"), bool):
        merged[C_EXTRAPOLATING] = extrapolating
    return merged


# --- pass-through extractions ---

def race_control_entries(payload: Any) -> list[dict]:
    """Every race-control message present in one line — pure.

    ``Messages`` starts the session as an **array** (the accumulated list so far) and becomes
    an **index-keyed dict** carrying only the new message a few lines in. Both shapes yield
    exactly the messages that line delivers, so emitting all of them per line is neither
    lossy nor duplicative.
    """
    if not isinstance(payload, dict):
        return []
    return [entry for entry in indexed(payload.get("Messages")).values()
            if isinstance(entry, dict)]


def series_entries(payload: Any, field: str) -> list[tuple[str, dict]]:
    """``{driver: [entry, …] | {index: entry}}`` flattened to ``[(driver, entry), …]`` — pure.

    The shape ``PitStopSeries`` and ``OvertakeSeries`` share: a per-driver collection that is
    an array in the feed's opening line and an index-keyed dict thereafter. Because a patch
    line contains only the entries it is *adding*, emitting everything present in the line is
    precisely "the new ones" — no accumulation and no dedupe state needed.
    """
    if not isinstance(payload, dict) or not isinstance(collection := payload.get(field), dict):
        return []
    return [(number, entry)
            for number, entries in collection.items()
            for entry in indexed(entries).values()
            if isinstance(entry, dict)]


def championship_rows(payload: Any) -> list[dict]:
    """``ChampionshipPrediction`` flattened to one row per driver and per team — pure.

    Drivers are keyed by racing number, teams by their **constructor** name (``"Red Bull
    Racing Red Bull Ford"``, which is not the ``TeamName`` the driver list uses — hence a
    discriminated ``entity_type``/``entity_id`` pair rather than a join).
    """
    if not isinstance(payload, dict):
        return []
    rows: list[dict] = []
    for kind, field in (("driver", "Drivers"), ("team", "Teams")):
        entries = payload.get(field)
        if not isinstance(entries, dict):
            continue
        for identity, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            rows.append({
                "entity_type": kind,
                "entity_id": identity,
                "position": parse_int(entry.get("CurrentPosition")),
                "points": parse_float(entry.get("CurrentPoints")),
                "predicted_position": parse_int(entry.get("PredictedPosition")),
                "predicted_points": parse_float(entry.get("PredictedPoints")),
            })
    return rows


CAR_CHANNELS: Final = {"0": "rpm", "2": "speed", "3": "gear", "4": "throttle", "5": "brake"}
"""The ``CarData`` channel numbers, mapped to what they measure.

Five channels, and **no DRS**: the widely-quoted channel ``45`` does not appear anywhere in
2026 data (a 400 KB mid-race sample contains only ``0``, ``2``, ``3``, ``4``, ``5``), so it is
not mapped rather than mapped to a column that would always be NULL. Throttle and brake are
kept on the upstream's own 0–104 scale — see :data:`~.attributes.THROTTLE`."""


def car_samples(payload: Any) -> list[tuple[str | None, str, dict[str, int | None]]]:
    """``CarData`` exploded to ``[(utc, racing_number, {channel_name: value}), …]`` — pure.

    One line carries ~5 samples of all 22 cars, so a single 6 KB inflated payload becomes ~110
    rows. Each keeps **its own** ``Utc``: telemetry samples predate the broadcast line that
    delivers them by a second or two, and the sample's clock is the truthful one.
    """
    if not isinstance(payload, dict) or not isinstance(entries := payload.get("Entries"), list):
        return []
    samples: list[tuple[str | None, str, dict[str, int | None]]] = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(cars := entry.get("Cars"), dict):
            continue
        utc = entry.get("Utc") if isinstance(entry.get("Utc"), str) else None
        for number, car in cars.items():
            channels = car.get("Channels") if isinstance(car, dict) else None
            if not isinstance(channels, dict):
                continue
            samples.append((utc, number, {
                name: parse_int(channels.get(code)) for code, name in CAR_CHANNELS.items()
                if code in channels
            }))
    return samples


def position_samples(payload: Any) -> list[tuple[str | None, str, dict[str, Any]]]:
    """``Position`` exploded to ``[(timestamp, racing_number, {x, y, z, status}), …]`` — pure.

    Same shape as :func:`car_samples` but a different envelope (``Position`` / ``Entries`` /
    ``Timestamp`` instead of ``Entries`` / ``Cars`` / ``Utc``) — the archive is not consistent
    about this, and pretending otherwise is how one of the two feeds silently yields nothing.
    """
    if not isinstance(payload, dict) or not isinstance(frames := payload.get("Position"), list):
        return []
    samples: list[tuple[str | None, str, dict[str, Any]]] = []
    for frame in frames:
        if not isinstance(frame, dict) or not isinstance(cars := frame.get("Entries"), dict):
            continue
        stamp = frame.get("Timestamp") if isinstance(frame.get("Timestamp"), str) else None
        for number, car in cars.items():
            if not isinstance(car, dict):
                continue
            samples.append((stamp, number, {
                "x": parse_int(car.get("X")), "y": parse_int(car.get("Y")),
                "z": parse_int(car.get("Z")),
                "status": car.get("Status") if isinstance(car.get("Status"), str) else None,
            }))
    return samples


WEATHER_FIELDS: Final = (
    ("air_temp", "AirTemp"), ("track_temp", "TrackTemp"), ("humidity", "Humidity"),
    ("pressure", "Pressure"), ("rainfall", "Rainfall"), ("wind_speed", "WindSpeed"),
    ("wind_direction", "WindDirection"),
)
"""``WeatherData``'s seven fields — **all of them strings on the wire** (``"30.0"``,
``"145"``), which is why every one goes through ``float()``."""


def weather_row(payload: Any) -> dict[str, float]:
    """``WeatherData`` as a row of floats, omitting anything unparseable — pure."""
    if not isinstance(payload, dict):
        return {}
    values = {name: parse_float(payload.get(field)) for name, field in WEATHER_FIELDS}
    return {name: value for name, value in values.items() if value is not None}

"""Delay monitor — a co-partitioned join of live updates against static profiles.

Stage 2. It consumes two topics keyed identically by ``trip_id`` —
``gtfs-trip-profiles`` (the schedule dimension from the loader) and
``gtfs-trip-updates`` (live delays from ingest) — and folds them into one **delay
record** per trip on ``gtfs-train-delays``: the train's current delay, the station it
is at or approaching (with that station's coordinates — the map marker), journey
progress, and the predicted delay at its destination.

**Why this is a co-partitioned join.** Both topics key by ``trip_id`` with the same
partition count, so a trip's profile and its updates land on the same task/state
bucket (``extract_state_key`` defaults to the message key). A profile message stores
the profile as that key's state; an update message reads it back and computes against
it — that *is* the join, exactly as GDELT joins Events ⋈ Mentions.

**The snapshot source makes buffering unnecessary — the deliberate contrast with
GDELT coverage.** GDELT must buffer an orphan mention because its event row never
re-arrives; here the RT feed re-sends the full snapshot every ~10 s, so an update that
arrives before its profile is simply **dropped** — the next snapshot re-delivers it
once the profile is in place. No orphan buffer, no TTL, no tombstone.

**No geometry.** The train is placed at its next station's coordinates — honest to the
data (there is no free route polyline; see the package docstring). Position is
*derived* from the schedule and the live delay, not measured.

**Event time is the update's ``FEED_TS``** (the RT header timestamp) — never
wall-clock, so :func:`build_delay_state` and everything under it is pure and the logic
tier drives every branch. ``service_time_to_utc`` anchors GTFS local clock-times to the
service day via the noon−12 h rule, which stays exact across the DST changeover.
"""
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flechtwerk import Event, IncomingMessage, Message, State, transformer

from .attributes import (
    DELAY_S,
    DESTINATION,
    FEED_TS,
    LAT,
    LINE,
    LON,
    NEXT_STOP,
    ROUTE_TYPE,
    SKIPPED,
    START_DATE,
    STATUS,
    STOP_ARR_S,
    STOP_DEP_S,
    STOP_ID,
    STOP_LAT,
    STOP_LON,
    STOP_NAME,
    STOPS,
    STOPS_DONE,
    STOPS_TOTAL,
    STOP_TIME_UPDATE,
    TERMINUS_DELAY_S,
    TRIP,
    TRIP_ID,
    TRIP_REL,
)
from .ingest import UPDATES_TOPIC
from .loader import PROFILES_TOPIC

log = logging.getLogger(__name__)

DELAYS_TOPIC = "gtfs-train-delays"

BERLIN = ZoneInfo("Europe/Berlin")
"""GTFS clock-times are local; German rail runs on Europe/Berlin (CET/CEST)."""

# status thresholds (seconds); the on_time ceiling is DB's < 6 min "pünktlich".
_EARLY = -60
_ON_TIME = 360
_LATE = 1800


def service_time_to_utc(start_date: str, seconds: int, *, tz: ZoneInfo = BERLIN) -> datetime:
    """Anchor a GTFS clock-time (seconds since local midnight, possibly > 24 h) to a UTC
    instant on the ``YYYYMMDD`` service day.

    Uses **local noon minus 12 h** as the day's zero, not midnight: that is the GTFS
    convention that keeps a "24:30" or a spring-forward night exact (midnight may not
    exist or may be ambiguous under DST; noon always does). Pure."""
    noon = datetime.strptime(start_date, "%Y%m%d").replace(hour=12, tzinfo=tz)
    return (noon - timedelta(hours=12) + timedelta(seconds=seconds)).astimezone(ZoneInfo("UTC"))


def classify(delay_s: int) -> str:
    """Bucket a delay (seconds) into ``early`` / ``on_time`` / ``late`` / ``severe``."""
    if delay_s < _EARLY:
        return "early"
    if delay_s <= _ON_TIME:
        return "on_time"
    if delay_s <= _LATE:
        return "late"
    return "severe"


def effective_delays(stops: list[dict], stus: list[dict]) -> tuple[list[int], list[bool]]:
    """Per profile stop, the delay in force there and whether it is SKIPPED.

    GTFS-RT stop-time-updates are sparse: a delay applies from its stop onward until the
    next update overrides it. We match updates to stops by ``stop_id`` (``stop_sequence``
    is dropped by ``MessageToDict`` when it is 0), carry the delay forward, reset to 0 on
    ``NO_DATA``, and flag ``SKIPPED`` stops without disturbing the carried delay. A
    ``departure`` delay wins over ``arrival``; an update present but delay-less is 0
    (on time — the wire omits a zero delay)."""
    by_id = {stu[STOP_ID]: stu for stu in stus if stu.get(STOP_ID)}
    delays: list[int] = []
    skipped: list[bool] = []
    current = 0
    for stop in stops:
        stu = by_id.get(stop[STOP_ID])
        is_skipped = False
        if stu is not None:
            rel = stu.get("schedule_relationship", "SCHEDULED")
            if rel == "SKIPPED":
                is_skipped = True
            elif rel == "NO_DATA":
                current = 0
            else:
                dep, arr = stu.get("departure") or {}, stu.get("arrival") or {}
                delay = dep.get("delay", arr.get("delay"))
                current = int(delay) if delay is not None else 0
        delays.append(current)
        skipped.append(is_skipped)
    return delays, skipped


@dataclass(frozen=True, slots=True)
class Progress:
    """Where a trip is at ``feed_ts``: the next (or current) stop and the delays around it."""
    next_idx: int
    current_delay_s: int
    stops_done: int
    terminus_delay_s: int


def locate(stops: list[dict], delays: list[int], start_date: str, feed_ts: datetime) -> Progress | None:
    """Locate the train at ``feed_ts`` — the first stop it has not yet departed.

    Delay-adjusted departures/arrivals are compared to ``feed_ts``: the next stop is the
    first whose adjusted departure is still in the future (the train is at or approaching
    it); before the first departure that is the origin, and a trip whose last adjusted
    arrival is already past has terminated (``None`` — nothing to show). Pure."""
    deps = [service_time_to_utc(start_date, s[STOP_DEP_S]) + timedelta(seconds=d) for s, d in zip(stops, delays)]
    arrs = [service_time_to_utc(start_date, s[STOP_ARR_S]) + timedelta(seconds=d) for s, d in zip(stops, delays)]
    if feed_ts > arrs[-1]:
        return None  # already arrived at its destination
    next_idx = next((i for i, dep in enumerate(deps) if dep > feed_ts), len(stops) - 1)
    stops_done = sum(1 for dep in deps if dep <= feed_ts)
    return Progress(next_idx, delays[next_idx], stops_done, delays[-1])


def build_delay_state(profile: Event, update: Event, feed_ts: datetime) -> Event | None:
    """Fold one live update against its trip profile into a delay record, or ``None``.

    ``None`` when the trip is CANCELED or has already terminated — there is nothing to
    place on the map. Otherwise the train is snapped to its next stop's coordinates and
    the record carries the current delay, its bucket, journey progress, the SKIPPED-stop
    count, and the predicted terminus delay. Pure and I/O-free."""
    trip = update.get(TRIP)
    if trip is not None and trip.get(TRIP_REL) == "CANCELED":
        return None
    stops = profile[STOPS]
    if not stops:
        return None
    stus = update.get(STOP_TIME_UPDATE) or []
    delays, skipped = effective_delays(stops, stus)
    start_date = (trip.get(START_DATE) if trip is not None else None) or feed_ts.strftime("%Y%m%d")
    progress = locate(stops, delays, start_date, feed_ts)
    if progress is None:
        return None
    at = stops[progress.next_idx]
    return Event({
        TRIP_ID: profile[TRIP_ID],
        LINE: profile[LINE],
        ROUTE_TYPE: profile[ROUTE_TYPE],
        DESTINATION: profile.get(DESTINATION),
        DELAY_S: progress.current_delay_s,
        STATUS: classify(progress.current_delay_s),
        NEXT_STOP: at[STOP_NAME],
        LAT: at[STOP_LAT],
        LON: at[STOP_LON],
        STOPS_TOTAL: len(stops),
        STOPS_DONE: progress.stops_done,
        SKIPPED: sum(skipped),
        TERMINUS_DELAY_S: progress.terminus_delay_s,
        FEED_TS: feed_ts,
    })


async def run_delays(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Join one profile-or-update message against the per-trip state.

    A profile message stores the profile as this ``trip_id``'s state (no output). An
    update message reads the stored profile and emits a delay record; an update with no
    profile yet is dropped (the snapshot re-delivers it next poll — no buffering). Pure."""
    if msg.topic == PROFILES_TOPIC:
        yield State(msg.value)  # store the profile as this trip's state
        return
    if STOPS not in state:
        log.debug("No profile yet for %s — dropping update (next snapshot re-delivers)", msg.key)
        return
    record = build_delay_state(state, msg.value, msg.value[FEED_TS])
    if record is not None:
        yield Message(key=msg.key, topic=DELAYS_TOPIC, value=record)
    # profile state is unchanged — nothing to persist


@transformer(input_topics=[PROFILES_TOPIC, UPDATES_TOPIC])
async def delays(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in run_delays(state, msg):
        yield item


stage = delays
"""The stage the dispatcher runs (``python -m examples.gtfs_german_rail_delays delays``)."""

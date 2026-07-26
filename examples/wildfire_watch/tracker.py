"""The fire tracker — point detections in, persistent fire objects out.

Stage 2. It consumes ``wildfire-detections`` (hotspot pixels **and** the poller's ``sweep``
markers, all keyed by the region slug, so one task owns a region's entire lifecycle) and holds
``FIRES = {fire_id: entry}`` per region in keyed state. The clustering arithmetic lives in the
framework-free :mod:`.tracking`; this module is the wiring: decode, decide, emit, persist.

**Sessionization with two different clocks, and only one of them ticks.**
A *detection* changes what we know about space: it founds a fire, joins one, or bridges two into
one. It emits a lifecycle event when the topology changes — but **no status**. A *sweep* changes
what we know about time: it is the only thing that can decide a fire has stopped burning, and
the only thing that paces the status heartbeat. Splitting the two is what keeps the output
streams honest — status rows appear on a steady cadence (so the map and the FRP timeline have a
continuous signal even for a fire nothing new was seen of), while events stay sparse and
meaningful.

The framework has no timers, so this is *the* pattern for periodic transformer work: the poller,
which does own a clock, emits punctuation, and the transformer hangs its time-based logic off it.
SMARD's settle marker finalizes one interval; the sweep marker here drives a whole lifecycle —
heartbeats *and* extinction. The trade is explicit and worth stating plainly: **no input means no
time means no extinction.** If ingest stops, fires freeze as they were rather than silently
ageing out — which is the safer failure, but it does mean a stalled poller looks like a
still-burning landscape. The sweep's ``new_detections = 0`` is what lets a dashboard tell "quiet"
from "stopped".

**Lifecycle and boundedness.** An extinguished fire leaves the dict; when the last one goes the
region's whole bucket is tombstoned with a falsy ``State()``, so the store holds *burning*
regions rather than every region ever watched, and the next ignition rebuilds it from scratch. A
sweep against empty state is a no-op (a marker replaying after its tombstone).

**The self-healing false extinction.** An unlucky combination of a wide satellite revisit gap and
NRT latency can age out a fire that is still burning. The next detection then links to nothing
and founds a *new* fire with a new id. This is documented rather than hidden: the alternative — a
timeout long enough to never be wrong — would leave dead fires on the map for days. The event log
shows exactly what happened (an ``extinguished`` followed by an ``ignition`` in the same place),
which is more useful than a silent fudge.

Every instant used here is event time — the detection's ``ACQUIRED_AT``, the sweep's
``SWEEP_AT`` — so :func:`run_tracker` is pure and I/O-free, and the logic tier drives every
branch including extinction.
"""
import logging
from collections.abc import AsyncIterator
from datetime import datetime

from flechtwerk import Event, IncomingMessage, Message, State, transformer
from flechtwerk.attribute import DATETIME

from .attributes import (
    ACQUIRED_AT,
    AS_OF,
    DETECTION_ID,
    DETECTIONS,
    DETECTIONS_TOPIC,
    EVENTS_TOPIC,
    F_COUNT,
    F_FIRST_SEEN,
    F_FRP_MAX,
    F_FRP_SUM,
    F_LAST_SEEN,
    F_LAT,
    F_LON,
    FIRE_ID,
    FIRES,
    FIRST_SEEN,
    FRP,
    FRP_MAX,
    FRP_SUM,
    KIND,
    LAST_SEEN,
    LAT,
    LON,
    MERGED_INTO,
    OCCURRED_AT,
    REGION,
    SATELLITE,
    STATUS,
    STATUS_TOPIC,
    SWEEP_AT,
)
from .tracking import (
    Detection,
    MAX_FIRES,
    absorb,
    detection_links,
    evict_stalest,
    expired,
    found_fire,
    merge_fires,
)

log = logging.getLogger(__name__)

ACTIVE = "active"
"""``STATUS`` of a fire still being detected — what the live map filters on."""
EXTINGUISHED = "extinguished"
"""``STATUS`` of a fire's final snapshot, and the ``KIND`` of the event that retires it."""
IGNITION = "ignition"
"""``KIND`` of the event a detection that links to no existing fire produces."""
MERGED = "merged"
"""``KIND`` of the event emitted per fire absorbed when one detection bridges several."""


def _detection(value: Event) -> Detection:
    """The framework-free :class:`~.tracking.Detection` for one ``detection`` record."""
    return Detection(
        detection_id=value[DETECTION_ID],
        lat=value[LAT],
        lon=value[LON],
        acquired_at=value[ACQUIRED_AT],
        satellite=value[SATELLITE],
        frp=value.get(FRP),
    )


def _decode_fires(state: State) -> dict[str, dict]:
    """The state's fires as :mod:`.tracking` wants them — a fresh dict, datetimes revived.

    A State nests only JSON scalars, so the two time bounds live in the store as ISO strings;
    :mod:`.tracking` folds them as real ``datetime``s (which is also what makes its tests
    readable). One level of copying is enough — the entries themselves are rebuilt here."""
    return {
        fire_id: {
            **entry,
            F_FIRST_SEEN: datetime.fromisoformat(entry[F_FIRST_SEEN]),
            F_LAST_SEEN: datetime.fromisoformat(entry[F_LAST_SEEN]),
        }
        for fire_id, entry in (state.get(FIRES) or {}).items()
    }


def _encode_fires(fires: dict[str, dict]) -> dict[str, dict]:
    """The inverse of :func:`_decode_fires` — datetimes back to the ``DATETIME`` codec's ISO
    rendering, so what lands in the changelog round-trips exactly."""
    return {
        fire_id: {
            **entry,
            F_FIRST_SEEN: DATETIME.encode(entry[F_FIRST_SEEN]),
            F_LAST_SEEN: DATETIME.encode(entry[F_LAST_SEEN]),
        }
        for fire_id, entry in fires.items()
    }


def _status(region: str, fire_id: str, fire: dict, *, status: str, as_of: datetime) -> Event:
    """One per-fire status snapshot. FRP fields ride along only once something carried FRP —
    never a fabricated 0."""
    record = Event({
        REGION: region, FIRE_ID: fire_id, STATUS: status,
        LAT: fire[F_LAT], LON: fire[F_LON],
        DETECTIONS: fire[F_COUNT],
        FIRST_SEEN: fire[F_FIRST_SEEN], LAST_SEEN: fire[F_LAST_SEEN],
        AS_OF: as_of,
    })
    for attribute, key in ((FRP_SUM, F_FRP_SUM), (FRP_MAX, F_FRP_MAX)):
        if key in fire:
            record[attribute] = fire[key]
    return record


def _event(kind: str, region: str, fire_id: str, fire: dict, occurred_at: datetime) -> Event:
    """The common shell of a lifecycle event: what happened, to which fire, where, and when."""
    return Event({
        KIND: kind, REGION: region, FIRE_ID: fire_id,
        LAT: fire[F_LAT], LON: fire[F_LON], OCCURRED_AT: occurred_at,
    })


def _extinguish(key: str, region: str, fire_id: str, fire: dict,
                occurred_at: datetime) -> tuple[Message, Message]:
    """The two records that retire a fire, however it dies: the ``extinguished`` lifecycle
    event with the fire's life summary, and the final status snapshot — which must outlive the
    state entry, because it is what flips the fire off the live map (``wildfire_active`` is
    read ``FINAL WHERE status = 'active'``)."""
    record = _event(EXTINGUISHED, region, fire_id, fire, occurred_at)
    record[DETECTIONS] = fire[F_COUNT]
    record[FIRST_SEEN] = fire[F_FIRST_SEEN]
    record[LAST_SEEN] = fire[F_LAST_SEEN]
    if F_FRP_MAX in fire:
        record[FRP_MAX] = fire[F_FRP_MAX]
    return (Message(key=key, topic=EVENTS_TOPIC, value=record),
            Message(key=key, topic=STATUS_TOPIC,
                    value=_status(region, fire_id, fire, status=EXTINGUISHED, as_of=occurred_at)))


async def run_tracker(state: State, msg: IncomingMessage) -> AsyncIterator[Message | State]:
    """Fold one detection-or-sweep into the region's fires — pure, I/O-free.

    Detections mutate the fire topology and emit events; sweeps advance time, emit the status
    heartbeat, and retire fires. The updated state is yielded last so messages and state change
    commit in one transaction."""
    value = msg.value
    region = value[REGION]

    if value[KIND] == "sweep":
        async for item in _sweep(state, msg, region):
            yield item
        return

    detection = _detection(value)
    fires = _decode_fires(state)
    linked = {
        fire_id: fire for fire_id, fire in fires.items()
        if detection_links(fire, detection.lat, detection.lon)
    }

    if not linked:
        fire_id, entry = found_fire(detection)
        fires[fire_id] = entry
        log.info("%s: ignition %s at %.5f,%.5f (%s)",
                 region, fire_id, detection.lat, detection.lon, detection.satellite)
        yield Message(key=msg.key, topic=EVENTS_TOPIC,
                      value=_event(IGNITION, region, fire_id, entry, detection.acquired_at))
    elif len(linked) == 1:
        (fire_id,) = linked
        fires[fire_id] = absorb(linked[fire_id], detection)
    else:
        survivor_id, merged, absorbed_ids = merge_fires(linked)
        for absorbed_id in absorbed_ids:
            record = _event(MERGED, region, absorbed_id, fires.pop(absorbed_id),
                            detection.acquired_at)
            record[MERGED_INTO] = survivor_id
            log.info("%s: %s merged into %s (bridged by one detection)",
                     region, absorbed_id, survivor_id)
            yield Message(key=msg.key, topic=EVENTS_TOPIC, value=record)
        fires[survivor_id] = absorb(merged, detection)

    # The bucket is ONE changelog record: past MAX_FIRES the stalest fires are forced out —
    # the same self-healing degradation as a false extinction, never an oversized record.
    fires, evicted = evict_stalest(fires, MAX_FIRES)
    for fire_id, fire in evicted.items():
        log.warning("%s: %s force-extinguished — bucket over the %d-fire cap "
                    "(%d detection(s), last seen %s)",
                    region, fire_id, MAX_FIRES, fire[F_COUNT], fire[F_LAST_SEEN].isoformat())
        for message in _extinguish(msg.key, region, fire_id, fire,
                                   max(detection.acquired_at, fire[F_LAST_SEEN])):
            yield message

    yield State({FIRES: _encode_fires(fires)})


async def _sweep(state: State, msg: IncomingMessage, region: str) -> AsyncIterator[Message | State]:
    """The sweep branch: retire what has aged out, heartbeat what survives, persist or tombstone.

    A sweep against empty state yields **nothing at all** — not even a tombstone — because there
    is nothing to retire and writing one would churn the changelog on every quiet poll of a
    region with no fires (the SMARD marker-replay no-op)."""
    fires = _decode_fires(state)
    if not fires:
        return
    sweep_at = msg.value[SWEEP_AT]

    survivors: dict[str, dict] = {}
    for fire_id, fire in fires.items():
        if not expired(fire, sweep_at):
            survivors[fire_id] = fire
            yield Message(key=msg.key, topic=STATUS_TOPIC,
                          value=_status(region, fire_id, fire, status=ACTIVE, as_of=sweep_at))
            continue
        log.info("%s: %s extinguished — %d detection(s), last seen %s",
                 region, fire_id, fire[F_COUNT], fire[F_LAST_SEEN].isoformat())
        for message in _extinguish(msg.key, region, fire_id, fire, sweep_at):
            yield message

    if survivors:
        yield State({FIRES: _encode_fires(survivors)})
    else:
        # The region's last fire is out: drop the whole bucket. The next ignition rebuilds it.
        log.info("%s: no fires left — tombstoning the region's state", region)
        yield State()


@transformer(input_topics=[DETECTIONS_TOPIC])
async def tracker(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in run_tracker(state, msg):
        yield item


stage = tracker
"""The stage the dispatcher runs (``python -m examples.wildfire_watch tracker``)."""

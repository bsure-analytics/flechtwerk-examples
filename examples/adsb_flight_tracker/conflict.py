"""ADS-B conflict detection — a stateful spatial self-join (baby TCAS).

Stage 3 of the pipeline. It consumes ``adsb-cells`` — positioned aircraft that
``enrich.py`` re-keyed by grid cell — and flags **pairs of aircraft that get too
close**: within ~5 nm horizontally *and* ~1000 ft vertically. Each cell is a
serial bucket (the input is cell-keyed, so every aircraft in a cell lands on one
partition/task), so the stage's ``State`` accumulates the cell's recent positions
and every new aircraft is checked against the ones already there — a stream
self-join, the thing no raw-feed viewer can do.

Why the ``adsb-cells`` repartition exists: a self-join needs the aircraft it
compares on the *same* partition. ``adsb-aircraft`` is keyed by ICAO, so two
aircraft in the same airspace hash to different partitions and never meet. Keying
by grid cell instead puts co-located aircraft together — that is the whole reason
``enrich.py`` emits the extra cell-keyed stream. Cell records keep the feed's wire
keys (``lat``/``lon``/``alt_baro``), read here through ``SRC_*`` handles.

**Limitation (documented, deliberate):** checks are within a single cell — there
is no 3×3 neighbour halo — so a conflict straddling a cell boundary is missed.
Real conflict detection handles neighbours; a coarse single-cell grid is honest
enough for a demo and keeps the state model simple.

The geometry (:func:`detect_conflicts`, haversine, thresholds, onset dedup) is
pure, so the logic tier drives it with nothing mocked.
"""
import math
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, transformer
from flechtwerk.attribute import Record

from .attributes import (
    ACTIVE_PAIRS,
    altitude_ft,
    AT,
    DETAIL,
    EVENT_TYPE,
    HEX,
    POLLED_AT,
    POSITIONS,
    REQUESTED_REGION,
    SRC_ALTITUDE,
    SRC_LAT,
    SRC_LON,
)
from .enrich import CELLS_TOPIC, EVENTS_TOPIC

SEPARATION_NM = 5.0
"""Horizontal separation minimum — closer than this (and vertically close) is a conflict."""
SEPARATION_FT = 1000
"""Vertical separation minimum, in feet."""
MIN_AIRBORNE_FT = 500
"""Below this an aircraft is on the ground or on short final — not an airborne-conflict
participant. This is what filters the airport-surface false positives: parked and taxiing
aircraft all report ``alt_baro`` ``"ground"`` (``altitude_ft`` reads that as ``None`` — no
numeric altitude — which the gate treats as not-airborne), so any two on a ramp would
otherwise read as a near-miss. A modest floor also drops the takeoff/landing sequencing
clutter in approach corridors."""
STALE_SECONDS = 15.0
"""Drop a cell position older than this before checking — an aircraft that stopped
updating is no longer 'there' to conflict with (poll cadence is ~10 s)."""

_EARTH_RADIUS_NM = 3440.065


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two positions, in nautical miles."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi, d_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * _EARTH_RADIUS_NM * math.asin(math.sqrt(a))


def _pair(icao_a: str, icao_b: str) -> str:
    """A stable, order-independent key for a pair of aircraft."""
    return "|".join(sorted((icao_a, icao_b)))


def _in_conflict(lat: float, lon: float, altitude: int | None, other: Record) -> bool:
    """True if this position is within both the horizontal and vertical minima.

    Both altitudes are known by construction — ``detect_conflicts`` only keeps
    airborne aircraft, so a stored position always carries feet — but the ``None``
    guard stays as a defensive belt: without a vertical figure the pair is never
    flagged."""
    if _haversine_nm(lat, lon, other[SRC_LAT], other[SRC_LON]) >= SEPARATION_NM:
        return False
    other_altitude = other.get(SRC_ALTITUDE)
    if altitude is None or other_altitude is None:
        return False
    return abs(altitude - other_altitude) < SEPARATION_FT


def _fresh(positions: dict[str, Record], polled_at) -> dict[str, Record]:
    """Drop cell positions older than ``STALE_SECONDS`` relative to this poll."""
    return {icao: p for icao, p in positions.items()
            if (polled_at - p[POLLED_AT]).total_seconds() <= STALE_SECONDS}


def _conflict_event(polled_at, aircraft: Record, other_icao: str, other: Record,
                    lat: float, lon: float, altitude: int | None) -> Event:
    separation = _haversine_nm(lat, lon, other[SRC_LAT], other[SRC_LON])
    detail = f"within {separation:.1f} nm of {other_icao}"
    if altitude is not None and other.get(SRC_ALTITUDE) is not None:
        detail += f", {abs(altitude - other[SRC_ALTITUDE])} ft vertical"
    event = Event({AT: polled_at, EVENT_TYPE: "conflict", HEX: aircraft[HEX], REQUESTED_REGION: aircraft[REQUESTED_REGION],
                   DETAIL: detail, SRC_LAT: lat, SRC_LON: lon})
    if altitude is not None:
        event[SRC_ALTITUDE] = altitude
    return event


async def detect_conflicts(state: State, aircraft: Record, polled_at) -> AsyncIterator[Message | State]:
    """Check one incoming aircraft against its cell's recent positions.

    Only *airborne* aircraft take part: one on the ground or broadcasting no altitude
    (``altitude_ft`` is ``None``), or on short final (below ``MIN_AIRBORNE_FT``), is
    neither checked nor kept — that is what stops parked and taxiing aircraft at an
    airport from reading as a swarm of false near-misses. For the rest, yields a ``conflict``
    event per newly-violating pair (once, at onset — a sustained near-miss is
    remembered in ``ACTIVE_PAIRS`` and not re-announced), clears pairs that have
    separated, upserts this aircraft's position, and yields the new ``State`` as the
    commit boundary. Pure and I/O-free.
    """
    icao = aircraft[HEX]
    lat, lon, altitude = aircraft[SRC_LAT], aircraft[SRC_LON], altitude_ft(aircraft.get(SRC_ALTITUDE))
    positions = _fresh(dict(state.get(POSITIONS) or {}), polled_at)
    active = set(state.get(ACTIVE_PAIRS) or set())

    if altitude is None or altitude < MIN_AIRBORNE_FT:
        # Not airborne — drop any stale position it left behind and commit unchanged;
        # don't compare it against others or keep it in the cell's self-join.
        positions.pop(icao, None)
        yield State({POSITIONS: positions, ACTIVE_PAIRS: active})
        return

    for other_icao, other in positions.items():
        if other_icao == icao:
            continue
        pair = _pair(icao, other_icao)
        if _in_conflict(lat, lon, altitude, other):
            if pair not in active:
                active.add(pair)
                yield Message(key=pair, topic=EVENTS_TOPIC,
                              value=_conflict_event(polled_at, aircraft, other_icao, other, lat, lon, altitude))
        else:
            active.discard(pair)  # separated → re-entry may fire again

    # Store the coerced numeric altitude — every aircraft in the cell is now airborne
    # (it passed the gate above), so the pairwise check always has feet to compare.
    positions[icao] = Record({SRC_LAT: lat, SRC_LON: lon, SRC_ALTITUDE: altitude, POLLED_AT: polled_at})
    yield State({POSITIONS: positions, ACTIVE_PAIRS: active})


@transformer(input_topics=[CELLS_TOPIC])
async def conflict(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    async for item in detect_conflicts(state, msg.value, msg.value[POLLED_AT]):
        yield item


stage = conflict
"""The stage the dispatcher runs (``python -m examples.adsb_flight_tracker conflict``)."""

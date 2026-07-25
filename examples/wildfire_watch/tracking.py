"""The spatiotemporal clustering core — pure, I/O-free, the logic tier's playground.

FIRMS gives us **375 m hotspot pixels**, not fires. A single blaze lights up a dozen adjacent
pixels on one pass, both satellites see it on different orbits hours apart, and NRT slices keep
delivering *older* pixels for up to ~3 h. Turning that into something a human recognizes as
"a fire" means clustering points into persistent objects with a lifecycle:

* **ignition** — a detection that links to no existing fire founds one;
* **growth** — a detection within :data:`LINK_KM` of a fire's footprint joins it, extending the
  footprint and re-centring the centroid;
* **merge** — a detection that links to *several* fires reveals they were one blaze all along,
  so they fold into a single survivor;
* **extinction** — a fire nothing has been seen of for :data:`EXTINGUISH_AFTER` of **event
  time** is declared out and leaves the state.

That is a **session window** — entities born, grown, and killed by timeout — in space as well
as time. GDELT clusters text into stories by cosine similarity; this is the same shape with
geography as the metric and a satellite's acquisition instant as the clock.

**Everything here is deterministic.** A fire's id comes from its founding detection, merge
survivors are chosen by earliest ``first_seen`` with a lexicographic id tie-break, and the
bounds are ``min``/``max`` folds rather than last-write-wins — so replaying the same detections
in the same order rebuilds byte-identical state, which is what makes the exactly-once story
mean something. Out-of-orderness is handled by the folds, never by buffering.

The only import is the ``F_*`` state-key vocabulary from :mod:`.attributes` (plain strings, one
home for the whole schema — the odds ``L_*`` placement). No framework types, no I/O, and no
clock appear in any signature here: entries hold real ``datetime`` objects and :mod:`.tracker`
encodes them at the state boundary, so ``tests/logic_test.py`` drives all of this directly.
"""
import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from .attributes import (
    F_COUNT,
    F_FIRST_SEEN,
    F_FRP_MAX,
    F_FRP_SUM,
    F_LAST_SEEN,
    F_LAT,
    F_LON,
    F_MAX_LAT,
    F_MAX_LON,
    F_MIN_LAT,
    F_MIN_LON,
    F_SATELLITES,
)

LINK_KM = 2.0
"""How far outside its current footprint a fire reaches to claim a new detection, in km.

≈5 VIIRS pixels. Big enough to bridge the gaps a coarse off-nadir pass leaves inside one fire
(the pixel itself grows past 750 m toward the swath edge) and to connect the same fire seen by
two satellites whose grids don't align; small enough that two genuinely separate fires a few km
apart stay separate. It is the single most consequential tuning knob in the example: raise it
and distinct fires fuse, lower it and one fire fragments into a cloud of short-lived objects."""

EXTINGUISH_AFTER = timedelta(hours=12)
"""How long a fire may go unseen, in **event time**, before a sweep declares it extinguished.

Sized from the observation geometry, not from taste: two satellites give a mid-latitude revisit
gap of typically 4–6 h (worst ~8 h), plus up to ~3 h of NRT delivery latency. 12 h clears that
with margin while still retiring a fire the same day it stops burning. It can still fire a
*false* extinction across an unlucky night — and that is deliberately allowed to happen,
because it **self-heals**: the next detection simply founds a new fire (with a new id). The
README documents that rather than hiding it behind a longer timeout that would leave dead fires
on the map for days."""

_KM_PER_DEGREE_LAT = 111.32
"""Great-circle km per degree of latitude — constant enough (±0.3 %) for a 2 km link test."""

_MIN_COS_LAT = 0.01
"""Floor on ``cos(latitude)`` when converting km to degrees of longitude, so the conversion
stays finite within ~0.6° of the poles. Longitude degrees collapse there; without the floor a
polar fire's link box would span the globe. No VIIRS fire detection lives at 89.5°, but a
divide-by-almost-zero is not the way to find that out."""


@dataclass(frozen=True, slots=True)
class Detection:
    """One hotspot pixel, reduced to what the clustering fold needs.

    Built by :mod:`.tracker` from a ``wildfire-detections`` record; a plain value object here so
    the whole module stays framework-free. ``frp`` is ``None`` when the source omitted it —
    note that a genuine ``0.0`` occurs in real FIRMS data, so absence is never inferred from
    falsiness.
    """
    detection_id: str
    lat: float
    lon: float
    acquired_at: datetime
    satellite: str
    frp: float | None


def link_spans(mid_lat: float, link_km: float = LINK_KM) -> tuple[float, float]:
    """``(Δlatitude, Δlongitude)`` in degrees for ``link_km`` at ``mid_lat`` — pure.

    Latitude degrees are uniform; longitude degrees shrink with ``cos(latitude)``, so the same
    2 km reaches ~0.018° of longitude at the equator but ~0.036° at 60°N. Getting this wrong is
    the classic geo bug: a fixed degree threshold silently doubles the link distance by the
    time you reach Scandinavia — and boreal fires are exactly what a wildfire demo wants to
    track."""
    d_lat = link_km / _KM_PER_DEGREE_LAT
    d_lon = link_km / (_KM_PER_DEGREE_LAT * max(math.cos(math.radians(mid_lat)), _MIN_COS_LAT))
    return d_lat, d_lon


def detection_links(fire: dict, lat: float, lon: float, link_km: float = LINK_KM) -> bool:
    """Whether a detection at ``(lat, lon)`` belongs to ``fire`` — pure.

    Tests containment in the fire's detection bounding box **expanded by** ``link_km`` on every
    side. A box rather than a centroid distance on purpose: a real fire front is long and thin,
    and a centroid test would stop claiming pixels at the far end of a 20 km burn scar while a
    grown box keeps following it."""
    d_lat, d_lon = link_spans((fire[F_MIN_LAT] + fire[F_MAX_LAT]) / 2.0, link_km)
    return (fire[F_MIN_LAT] - d_lat <= lat <= fire[F_MAX_LAT] + d_lat
            and fire[F_MIN_LON] - d_lon <= lon <= fire[F_MAX_LON] + d_lon)


def found_fire(detection: Detection) -> tuple[str, dict]:
    """A brand-new fire from its first detection: ``(fire_id, entry)`` — pure.

    The id is ``F-`` + the founding detection's id, so it is a **deterministic function of the
    input stream**: replay the same detections and the same fires get the same names, with no
    counter or random source in the loop. The footprint starts as the single pixel's point."""
    entry = {
        F_LAT: detection.lat,
        F_LON: detection.lon,
        F_MIN_LAT: detection.lat,
        F_MAX_LAT: detection.lat,
        F_MIN_LON: detection.lon,
        F_MAX_LON: detection.lon,
        F_FIRST_SEEN: detection.acquired_at,
        F_LAST_SEEN: detection.acquired_at,
        F_COUNT: 1,
        F_SATELLITES: [detection.satellite],
    }
    if detection.frp is not None:
        entry[F_FRP_SUM] = detection.frp
        entry[F_FRP_MAX] = detection.frp
    return f"F-{detection.detection_id}", entry


def absorb(fire: dict, detection: Detection) -> dict:
    """``fire`` grown by one detection — a new dict, pure.

    The centroid is a **count-weighted running mean**, so it converges on the fire's centre of
    observed activity without keeping every point. Bounds are ``min``/``max`` folds in both
    directions: a late NRT slice delivering an *older* pixel correctly pushes ``first_seen``
    back without ever regressing ``last_seen`` — the property that stops out-of-order data from
    resurrecting or prematurely killing a fire. FRP folds only when present, so a fire seen
    only through FRP-less rows carries no fabricated 0."""
    count = fire[F_COUNT]
    grown = {
        **fire,
        F_LAT: (fire[F_LAT] * count + detection.lat) / (count + 1),
        F_LON: (fire[F_LON] * count + detection.lon) / (count + 1),
        F_MIN_LAT: min(fire[F_MIN_LAT], detection.lat),
        F_MAX_LAT: max(fire[F_MAX_LAT], detection.lat),
        F_MIN_LON: min(fire[F_MIN_LON], detection.lon),
        F_MAX_LON: max(fire[F_MAX_LON], detection.lon),
        F_FIRST_SEEN: min(fire[F_FIRST_SEEN], detection.acquired_at),
        F_LAST_SEEN: max(fire[F_LAST_SEEN], detection.acquired_at),
        F_COUNT: count + 1,
        F_SATELLITES: sorted({*fire[F_SATELLITES], detection.satellite}),
    }
    if detection.frp is not None:
        grown[F_FRP_SUM] = fire.get(F_FRP_SUM, 0.0) + detection.frp
        grown[F_FRP_MAX] = max(fire.get(F_FRP_MAX, detection.frp), detection.frp)
    return grown


def merge_fires(entries: dict[str, dict]) -> tuple[str, dict, list[str]]:
    """Fold several fires into one: ``(survivor_id, merged_entry, absorbed_ids)`` — pure.

    Called when a single detection links to more than one fire, which means they were always
    one blaze and the satellites had merely caught it as separate hotspot groups. The
    **survivor is the earliest ``first_seen``** (the fire that has the better claim to being
    the original), tie-broken lexicographically by id so the choice never depends on dict
    ordering. Counts, FRP, satellites, footprint, and both time bounds all fold; the centroid is
    re-derived as the count-weighted mean of the parts.

    ``entries`` must be non-empty. With a single entry it is the identity fold (no absorbed
    ids), which keeps the caller free of a special case."""
    assert entries, "merge_fires needs at least one fire"
    survivor_id = min(entries, key=lambda fid: (entries[fid][F_FIRST_SEEN], fid))
    total = sum(entry[F_COUNT] for entry in entries.values())
    merged = {
        F_LAT: sum(e[F_LAT] * e[F_COUNT] for e in entries.values()) / total,
        F_LON: sum(e[F_LON] * e[F_COUNT] for e in entries.values()) / total,
        F_MIN_LAT: min(e[F_MIN_LAT] for e in entries.values()),
        F_MAX_LAT: max(e[F_MAX_LAT] for e in entries.values()),
        F_MIN_LON: min(e[F_MIN_LON] for e in entries.values()),
        F_MAX_LON: max(e[F_MAX_LON] for e in entries.values()),
        F_FIRST_SEEN: min(e[F_FIRST_SEEN] for e in entries.values()),
        F_LAST_SEEN: max(e[F_LAST_SEEN] for e in entries.values()),
        F_COUNT: total,
        F_SATELLITES: sorted({s for e in entries.values() for s in e[F_SATELLITES]}),
    }
    if (sums := [e[F_FRP_SUM] for e in entries.values() if F_FRP_SUM in e]):
        merged[F_FRP_SUM] = sum(sums)
    if (maxes := [e[F_FRP_MAX] for e in entries.values() if F_FRP_MAX in e]):
        merged[F_FRP_MAX] = max(maxes)
    return survivor_id, merged, sorted(fid for fid in entries if fid != survivor_id)


def expired(fire: dict, sweep_at: datetime, ttl: timedelta = EXTINGUISH_AFTER) -> bool:
    """Whether ``fire`` has gone unseen past ``ttl`` as of ``sweep_at`` — pure.

    Both instants are event time (the fire's last acquisition, the poll's server ``Date``), so
    this never reads a clock and the logic tier can drive extinction by simply choosing a later
    sweep."""
    return sweep_at - fire[F_LAST_SEEN] > ttl

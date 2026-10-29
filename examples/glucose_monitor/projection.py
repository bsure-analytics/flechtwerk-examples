"""The pure half of Glucose Monitor — LibreLinkUp timestamps, reading extraction, the seen-set.

Nothing here touches the framework, the network or a clock, so the logic tier drives it straight
off the fixture (``tests/fixtures/``) and ``readings`` stays a thin shell around
:func:`readings` and :func:`fresh`.

**Two series in one response.** A ``graph`` response carries the connection's *latest*
measurement (``connection.glucoseMeasurement`` — about one a minute from a Libre 3) and the
``graphData`` history (the last ~12 hours on a coarser grid). They are different series — the
history is not a copy of the live readings — so each item is tagged with its ``kind`` and deduped
within its own kind.

**Spread through, compute only what is missing.** A reading is the item as delivered plus what
no item says outright: the UTC instant parsed from ``FactoryTimestamp`` and the phone's UTC
offset (``Timestamp`` is the phone's local clock, so the difference is the offset). Every other
field — ``ValueInMgPerDl``, ``TrendArrow``, ``MeasurementColor``, ``isHigh``, fields Abbott adds
tomorrow — passes through untouched.

**The seen-set stands in for a cursor the source doesn't have.** LibreLinkUp has no "since"
parameter: every poll returns the whole 12-hour window again, and the window sometimes fills in a
point *behind* the newest one (LibreLinkUp lags in reporting recent history), so a high-water
mark would lose it. :func:`fresh` keeps every emitted timestamp of the last :data:`HORIZON` per
kind, measured back from the newest reading — event time, never a wall clock.
"""
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

HORIZON = timedelta(hours=24)
"""How far behind a patient's newest reading the seen-set remembers. Twice the ~12-hour
``graphData`` window, so a point is forgotten only long after the window has dropped it. One day
of a Libre 3 is ~1 440 live and a few hundred history stamps, ~20 KB of state — far under the
~1 MB changelog-record ceiling, so no hard cap is needed."""

_TIMESTAMP = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{2}):(\d{2}) ([AP]M)")


def parse_timestamp(text: str) -> datetime:
    """LibreLinkUp's US-style ``M/D/YYYY h:mm:ss AM`` as a naive ``datetime``.

    Parsed by hand rather than with ``strptime``'s ``%p``, whose AM/PM strings follow the process
    locale. Naive on purpose: the same format carries a UTC clock (``FactoryTimestamp``) and a
    local one (``Timestamp``), and only the caller knows which it holds."""
    match = _TIMESTAMP.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"not a LibreLinkUp timestamp: {text!r}")
    month, day, year, hour, minute, second, half = match.groups()
    hour24 = int(hour) % 12 + (12 if half == "PM" else 0)
    return datetime(int(year), int(month), int(day), hour24, int(minute), int(second))


def utc_offset_minutes(factory: datetime, local: datetime) -> int:
    """The phone's UTC offset: local clock minus UTC clock, rounded to a quarter hour (every
    real offset is a multiple of 15 minutes, and the two clocks may disagree by a few seconds)."""
    return round((local - factory).total_seconds() / 900) * 15


@dataclass(frozen=True)
class Reading:
    """One glucose item with what :func:`readings` computed for it."""

    kind: str
    """``live`` or ``history``."""
    measured_at: datetime
    """Aware UTC, from ``FactoryTimestamp``."""
    utc_offset_min: int | None
    """The phone's UTC offset, or ``None`` when the item has no ``Timestamp``."""
    item: dict[str, Any]
    """The item exactly as delivered."""

    @property
    def stamp(self) -> int:
        """Epoch seconds — the reading's identity within its kind."""
        return int(self.measured_at.timestamp())


def readings(data: dict[str, Any]) -> list[Reading]:
    """Every reading a ``graph`` response's ``data`` carries, oldest first.

    An item without a ``FactoryTimestamp`` cannot be placed in time; it is logged and skipped
    rather than crashing the stage on a record that will never parse."""
    items: list[tuple[str, dict[str, Any]]] = []
    if measurement := (data.get("connection") or {}).get("glucoseMeasurement"):
        items.append(("live", measurement))
    items.extend(("history", item) for item in data.get("graphData") or [])

    out: list[Reading] = []
    for kind, item in items:
        factory = item.get("FactoryTimestamp")
        if not factory:
            log.warning("skipping a %s item without FactoryTimestamp: %r", kind, item)
            continue
        utc = parse_timestamp(factory)
        local = item.get("Timestamp")
        offset = utc_offset_minutes(utc, parse_timestamp(local)) if local else None
        out.append(Reading(kind, utc.replace(tzinfo=timezone.utc), offset, item))
    return sorted(out, key=lambda reading: reading.measured_at)


def fresh(candidates: list[Reading], seen: dict[str, list[int]], *,
          horizon: timedelta = HORIZON) -> tuple[list[Reading], dict[str, list[int]]]:
    """The candidates not emitted before, and the seen-set after emitting them.

    The floor is the newest stamp known — seen or arriving — minus ``horizon``. A candidate
    below it is dropped: it is older than anything the ``graphData`` window can still hold, so it
    was decided long ago. Duplicates inside one response collapse too."""
    known = {kind: set(stamps) for kind, stamps in seen.items()}
    stamps = [reading.stamp for reading in candidates]
    stamps.extend(max(kind_stamps) for kind_stamps in known.values() if kind_stamps)
    if not stamps:
        return [], {}
    floor = max(stamps) - int(horizon.total_seconds())

    out: list[Reading] = []
    for reading in candidates:
        kind_stamps = known.setdefault(reading.kind, set())
        if reading.stamp < floor or reading.stamp in kind_stamps:
            continue
        kind_stamps.add(reading.stamp)
        out.append(reading)
    return out, prune_seen(known, floor)


def prune_seen(known: dict[str, set[int]], floor: int) -> dict[str, list[int]]:
    """The seen-set as stored: per kind, the sorted stamps at or above ``floor``; a kind with
    none left is dropped."""
    pruned = {kind: sorted(stamp for stamp in stamps if stamp >= floor)
              for kind, stamps in known.items()}
    return {kind: stamps for kind, stamps in pruned.items() if stamps}

"""The bundled outlet table — the domain → name/country lookup for coverage spread.

``outlets.csv`` is objective metadata only (domain, name, country); :func:`outlet_messages`
projects it into ``gdelt-outlets`` records. ``setup.py`` seeds them onto the compacted
``gdelt-outlets`` **config topic** (a one-shot producer — the table is static bundled data,
so it needs no polling stage), and ``GdeltStories`` joins that topic GlobalKTable-style to
annotate each story's coverage spread (how many distinct countries' outlets carry it). Any
producer can write ``gdelt-outlets`` directly (Kafbat included) — this is just the
convenient, idempotent seed.

**Objective metadata only.** A Ground-News-style *leaning*/bias column would plug in right
here — deliberately **not shipped** (see the README): the point is the streaming layer, and
editorial-bias ratings carry baggage this demo won't take on.
"""
import csv
import io
from collections.abc import Iterator
from pathlib import Path

from flechtwerk import Event, Message

from .schema import OUTLET_COUNTRY, OUTLET_DOMAIN, OUTLET_NAME

OUTLETS_TOPIC = "gdelt-outlets"
"""Compacted config topic, one record per outlet (keyed by domain). Seeded by ``setup.py``,
consumed by ``GdeltStories`` as a lookup table."""

OUTLETS_CSV = Path(__file__).parent / "outlets.csv"


def outlet_messages(csv_text: str) -> Iterator[Message]:
    """Project the bundled ``domain,name,country`` CSV into ``gdelt-outlets`` records.

    Pure and I/O-free (text in, ``Message``s out) so the logic tier drives it, and ``setup.py``
    reuses it to seed the topic. One record per row, keyed by the (lowercased) domain;
    blank/domain-less rows are skipped.
    """
    for row in csv.DictReader(io.StringIO(csv_text)):
        domain = (row.get("domain") or "").strip().lower()
        if not domain:
            continue
        value = Event({OUTLET_DOMAIN: domain})
        if name := (row.get("name") or "").strip():
            value[OUTLET_NAME] = name
        if country := (row.get("country") or "").strip():
            value[OUTLET_COUNTRY] = country
        yield Message(key=domain, topic=OUTLETS_TOPIC, value=value)

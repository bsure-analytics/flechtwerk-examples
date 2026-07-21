"""Outlet loader — an ``Extractor`` that publishes a bundled outlet table to a config topic.

Mirrors ADS-B's ``CountryLoader``: a small loader that provisions a lookup table other
stages read. It reads the bundled ``outlets.csv`` (domain → name, country) and publishes one
record per outlet to the compacted ``gdelt-outlets`` **config topic**, which ``GdeltStories``
joins against (GlobalKTable-style) to annotate each story's coverage spread — how many
distinct countries' outlets carry it.

The loader is driven by a one-record ``gdelt-outlet-load`` trigger config (seeded by
``setup.py``): each poll re-reads the CSV, and a content digest kept in ``State`` gates it —
an unchanged CSV re-poll is a no-op, exactly like ``CountryLoader``'s freshness gate, so the
outlets are (re)published only when the bundle actually changes. Any producer can write
``gdelt-outlets`` directly (Kafbat included); this is just the convenient, idempotent seed.

**Objective metadata only** (domain, name, country). A Ground-News-style *leaning*/bias
column would plug in right here — deliberately **not shipped** (see the README): the point
is the streaming layer, and editorial-bias ratings carry baggage this demo won't take on.

The CSV → records projection lives in the pure function :func:`outlet_messages`; the stage
is the thin shell that reads the file, gates on the digest, and delegates.
"""
import csv
import hashlib
import io
import logging
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

from flechtwerk import Config, Event, Extractor, Message, State

from .schema import (
    OUTLET_COUNTRY,
    OUTLET_DOMAIN,
    OUTLET_LOADED_DIGEST,
    OUTLET_NAME,
)

log = logging.getLogger(__name__)

OUTLETS_TOPIC = "gdelt-outlets"
"""Compacted config topic, one record per outlet (keyed by domain). Produced here,
consumed by ``GdeltStories`` as a lookup table."""
OUTLET_LOAD_TOPIC = "gdelt-outlet-load"
"""One-record trigger config topic that drives the loader's poll (seeded by ``setup.py``)."""

OUTLETS_CSV = Path(__file__).parent / "outlets.csv"


def outlet_messages(csv_text: str) -> Iterator[Message]:
    """Project the bundled ``domain,name,country`` CSV into ``gdelt-outlets`` records.

    Pure and I/O-free (text in, ``Message``s out) so the logic tier drives it. One record
    per row, keyed by the (lowercased) domain; blank/domain-less rows are skipped.
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


class OutletLoader(Extractor):
    """Publishes the bundled outlet table to ``gdelt-outlets``, once per CSV version.

    Subclassing only to hold the (injectable) CSV path/text; it owns no network resource.
    """

    config_topics = [OUTLET_LOAD_TOPIC]

    def __init__(self, *, csv_path: Path = OUTLETS_CSV, csv_text: str | None = None,
                 outlets_topic: str = OUTLETS_TOPIC) -> None:
        super().__init__()
        self._csv_path = csv_path
        self._csv_text = csv_text
        self._outlets_topic = outlets_topic

    def _read(self) -> str:
        return self._csv_text if self._csv_text is not None else self._csv_path.read_text()

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Publish every outlet, then record the CSV digest as the cursor.

        A digest matching the stored one means the bundle is unchanged since the last
        publish → a no-op poll (yields nothing, so the next poll re-enters with the same
        state). Otherwise every outlet record is emitted and the digest advances — messages
        first, ``State`` last, one transaction.
        """
        text = self._read()
        digest = hashlib.sha256(text.encode()).hexdigest()
        if state.get(OUTLET_LOADED_DIGEST) == digest:
            return
        count = 0
        for message in outlet_messages(text):
            count += 1
            yield Message(key=message.key, topic=self._outlets_topic, value=message.value)
        log.info("published %d outlets to %s", count, self._outlets_topic)
        yield State({OUTLET_LOADED_DIGEST: digest})


stage = OutletLoader()
"""The stage the dispatcher runs (``python -m examples.gdelt_news_stories outlets``)."""

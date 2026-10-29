"""Glucose Monitor — a FreeStyle Libre 3's readings as a stream, via LibreLinkUp.

Two host processes over LibreLinkUp, the follower side of Abbott's FreeStyle Libre cloud:

* ``ingest`` — an Extractor, and nothing but a backup. For each account on the compacted
  ``glucose-accounts`` config topic (a LibreLinkUp follower account, its email and password
  **encrypted** on the record), it signs in only when its session — kept **encrypted** as its
  state — has expired or was rejected, lists the patients the account follows and emits each
  one's ``graph`` response — the latest measurement and the last ~12 hours of history — exactly
  as delivered, minus the envelope's credential, to ``glucose-raw``. Every minute, with no
  cursor: the source has none.
* ``readings`` — a Transformer, the first stage to read a response. It emits one
  ``glucose-readings`` record per reading it has not emitted before — the live one and every
  history point — deduped by a 24-hour, event-time-pruned seen-set per patient. ClickHouse's
  Kafka engine lands the readings for the dashboard (time in range, GMI, a daily profile).

Why this example exists: it is the repo's **personal-data** case — a source whose only interface
is a sign-in, whose window forgets everything older than 12 hours (so the raw backup is the only
history there will ever be), and whose snapshot repeats itself on every poll (so the stream is
made by a seen-set, not by the source). It is also the one example whose source is an
**unofficial, reverse-engineered API** — see the README's caveats — and the one whose readings
are health data: keep the stack local. Not a medical device; never dose from it.
"""
from .attributes import ACCOUNTS_TOPIC, RAW_TOPIC, READINGS_TOPIC
from .projection import fresh, parse_timestamp, readings, utc_offset_minutes

__all__ = [
    "ACCOUNTS_TOPIC",
    "RAW_TOPIC",
    "READINGS_TOPIC",
    "fresh",
    "parse_timestamp",
    "readings",
    "utc_offset_minutes",
]

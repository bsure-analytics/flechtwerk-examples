"""TMDB Catalog — a local copy of The Movie Database, kept current from its daily change feeds.

Two host processes over TMDB's API v3:

* ``changes`` — an Extractor. For each feed on the compacted ``tmdb-feeds`` config topic
  (``movie``, ``tv``, ``person`` — each record carrying the access token **encrypted**), it lists
  one calendar day of TMDB's change feed at a time, from an epoch carried on the record (default:
  the last 90 days) up to today, and emits one ``tmdb-changes`` record per changed id. The cursor
  is the last *closed* day; the open day is deduped against a seen-set and re-listed in full once
  it closes.
* ``catalog`` — a Transformer. It fetches each changed entity's details, projects them to a flat
  row, and emits a new **version** only when a content hash says the content moved — one row on
  ``tmdb-movies`` / ``tmdb-tv`` / ``tmdb-people`` plus one row per credit on ``tmdb-credits``.
  ClickHouse's Kafka engine lands the rows for ad-hoc analysis.

Why this example exists: it teaches two shapes the others don't — **reference data as a change
stream** (slowly changing dimensions behind a content-hash version gate, with a calendar-day
cursor sized to a 500-page cap and a seen-set for the open day) and the framework's **keyring
facility in a real ops flow** (a keyring file, ``request-tmdb token`` encrypting the credential
onto the config topic, stages decrypting on read, the secret counters on the metrics endpoint).
It is the repo's first example to carry its credential encrypted on a config topic.

This product uses TMDB and the TMDB APIs but is not endorsed, certified, or otherwise approved by
TMDB. Non-commercial use — see ``README.md``.
"""
from .attributes import (
    CHANGES_TOPIC,
    CREDITS_TOPIC,
    FEEDS_TOPIC,
    MOVIES_TOPIC,
    PEOPLE_TOPIC,
    TV_TOPIC,
)
from .projection import (
    content_hash,
    day_windows,
    parse_changes_page,
    project_movie,
    project_person,
    project_tv,
    prune_seen,
)

__all__ = [
    "CHANGES_TOPIC",
    "CREDITS_TOPIC",
    "FEEDS_TOPIC",
    "MOVIES_TOPIC",
    "PEOPLE_TOPIC",
    "TV_TOPIC",
    "content_hash",
    "day_windows",
    "parse_changes_page",
    "project_movie",
    "project_person",
    "project_tv",
    "prune_seen",
]

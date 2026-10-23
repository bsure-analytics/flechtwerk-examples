"""Typed attributes, topic names, and state keys for the TMDB Catalog example.

Two schemas meet here, owned by different parties. The **catalog records** (a movie, a TV
series, a person) are TMDB's own JSON: the projection spreads everything TMDB sends and adds
only the fields we *compute* — so, deliberately, only the computed ones are declared below
(``MEDIA_TYPE``, ``VERSION``, ``CONTENT_HASH``, the flattened name lists, …). Declaring every
TMDB field would re-declare a schema we don't own and break on the first field TMDB adds (it
already sends an undocumented ``softcore``). The **credit records** are ours — one row per
person-on-title, built from scratch — so every one of their fields is declared.

**The credential rides on every feed config record.** The config store merges every config topic
a stage declares into one namespace, and the extractor polls every entry of it: a second
"credentials" record would be polled as if it were a feed. So the token is an ``ENCRYPTED(STR)``
attribute of each feed's record on ``tmdb-feeds`` — ciphertext (``flenc:jwe:…``) on the wire,
plaintext only inside a stage that holds the keyring.

**Every clock is TMDB's or the framework's, never a wall clock read mid-logic.** ``CHANGED_ON``
is TMDB's own UTC calendar day; ``DISCOVERED_AT`` / ``VERSION_AT`` are the server ``Date`` of the
response that produced the record.
"""
from typing import Final

from flechtwerk.attribute import Attribute, DATE, DATETIME, DICT, FLOAT, INT, LIST, RECORD, STR
from flechtwerk.secrets import ENCRYPTED

# --- Topics (the wire contract) ---

FEEDS_TOPIC: Final = "tmdb-feeds"
"""Compacted config topic, one record per feed (``movie`` | ``tv`` | ``person``), keyed by the
feed name — which doubles as TMDB's path segment. Written by ``request-tmdb`` (the token goes in
encrypted), never seeded by setup."""
CHANGES_RAW_TOPIC: Final = "tmdb-changes-raw"
"""The extractor's backup: one record per change-feed page **exactly as TMDB delivered it**,
nested under ``RESPONSE`` with the request's provenance under ``METADATA`` (the ``adsb-raw``
shape). Keyed by the feed name, so a feed's pages stay in order on one ``split`` task."""
CHANGES_TOPIC: Final = "tmdb-changes"
"""Partitioned stream of change notifications — one record per (id, day) the daily change feed
listed — keyed ``<feed>:<id>`` (``movie:550``) so one entity's changes land on one catalog task."""
MOVIES_TOPIC: Final = "tmdb-movies"
"""One flat JSON row per movie *version*, keyed ``movie:<id>``."""
TV_TOPIC: Final = "tmdb-tv"
"""One flat JSON row per TV-series version, keyed ``tv:<id>``."""
PEOPLE_TOPIC: Final = "tmdb-people"
"""One flat JSON row per person version, keyed ``person:<id>``."""
CREDITS_TOPIC: Final = "tmdb-credits"
"""One flat JSON row per credit (cast or crew) of a title version, keyed by the *title*
(``movie:550``) so a title's credits co-partition with its versions."""

FEEDS: Final = ("movie", "tv", "person")
"""The three feeds. The config keys double as TMDB path segments (``/3/movie/changes``)."""

# --- Config record (tmdb-feeds; wire key = feed name) ---

FEED: Final = Attribute("feed", STR)
"""The feed name — duplicated into the value so a stage reads it without decoding the key."""
ACCESS_TOKEN: Final = Attribute("access_token", ENCRYPTED(STR))
"""TMDB's *API Read Access Token* (a bearer JWT). ``ENCRYPTED`` makes a typed write produce
ciphertext and a typed read decrypt it with the process keyring — see ``request.py`` for the
write, the stages for the read."""
SINCE: Final = Attribute("since", DATE, optional=True)
"""The epoch: the first calendar day to list for a feed that has never listed. Optional — the
default is the last ``DEFAULT_LOOKBACK_DAYS`` days. It only *seeds* the cursor; editing it later
does not rewind."""

# --- Raw page record (tmdb-changes-raw; wire key = feed name) ---

METADATA: Final = Attribute("metadata", RECORD)
"""The request's provenance, nested: ``FEED``, ``CHANGED_ON`` (the day listed), ``PAGE``,
``LISTING`` and ``DISCOVERED_AT`` (the response's ``Date``). Everything here is ours — what we
asked for and when — never read out of the body."""
RESPONSE: Final = Attribute("response", RECORD)
"""The change-feed page as delivered, ``Record.wrap``-ed at the HTTP edge and never inspected
by the extractor — the uncontrolled schema keeps its own namespace."""
PAGE: Final = Attribute("page", INT)
"""The page number requested (TMDB pages start at 1)."""

# --- Change record (tmdb-changes; wire key = "<feed>:<id>") ---

TMDB_ID: Final = Attribute("id", INT)
"""TMDB's numeric id of the entity within its feed."""
CHANGED_ON: Final = Attribute("changed_on", DATE)
"""The UTC calendar day the change feed listed the entity under (TMDB's day, not ours)."""
LISTING: Final = Attribute("listing", STR)
"""``open`` while the day was still accruing, ``closed`` for the one full listing made after the
day ended. A closed listing is the authoritative copy of that day."""
DISCOVERED_AT: Final = Attribute("discovered_at", DATETIME)
"""The server ``Date`` of the first page of the listing that produced this record."""

# --- Extractor state (one record per feed) ---

CURSOR: Final = Attribute("cursor", DATE, optional=True)
"""The last **closed** day fully listed — monotonic. Absent until a day has closed. The
extractor's only state: it records *how far it has listed*, never what the pages said."""

# --- Split state (one record per feed) ---

SEEN: Final = Attribute("seen", DICT(LIST(INT)))
"""``{iso_date: [ids]}`` for the **open** day only: what ``split`` already emitted for it, so the
hourly re-listing of today yields just the news. Pruned to the open day and hard-capped (see
``projection.prune_seen``)."""

# --- Catalog records (spread from TMDB; these are the computed fields) ---

MEDIA_TYPE: Final = Attribute("media_type", STR)
"""``movie`` | ``tv`` | ``person`` — the feed the row came from."""
VERSION: Final = Attribute("version", INT)
"""The entity's version counter: 1 for the first row we ever emitted, +1 per content change."""
VERSION_AT: Final = Attribute("version_at", DATETIME)
"""The server ``Date`` of the detail response this version was built from."""
CONTENT_HASH: Final = Attribute("content_hash", STR)
"""16 hex of the digest of the projected content, computed **before** the version fields are
added — the version gate: an unchanged hash emits nothing."""

GENRES: Final = Attribute("genres", LIST(STR))
KEYWORDS: Final = Attribute("keywords", LIST(STR))
PRODUCTION_COMPANIES: Final = Attribute("production_companies", LIST(STR))
PRODUCTION_COUNTRIES: Final = Attribute("production_countries", LIST(STR))
SPOKEN_LANGUAGES: Final = Attribute("spoken_languages", LIST(STR))
NETWORKS: Final = Attribute("networks", LIST(STR))
CREATED_BY: Final = Attribute("created_by", LIST(STR))
"""The flattened name lists: TMDB sends ``[{"id": 18, "name": "Drama"}, …]``; the catalog keeps
the names, so a downstream OLAP store reads a plain string array with no flatten step."""
COLLECTION: Final = Attribute("collection", STR, optional=True)
"""The name of the collection a movie belongs to (``belongs_to_collection.name``)."""
CERTIFICATIONS: Final = Attribute("certifications", DICT(STR))
"""``{iso_3166_1: certification}`` of a movie — read downstream as ``payload.certifications.US``."""
CONTENT_RATINGS: Final = Attribute("content_ratings", DICT(STR))
"""``{iso_3166_1: rating}`` of a TV series."""
IMDB_ID: Final = Attribute("imdb_id", STR, optional=True)
WIKIDATA_ID: Final = Attribute("wikidata_id", STR, optional=True)
TVDB_ID: Final = Attribute("tvdb_id", INT, optional=True)

# --- Credit record (tmdb-credits; ours, so fully declared) ---

MEDIA_ID: Final = Attribute("media_id", INT)
"""TMDB's id of the title the credit belongs to; ``MEDIA_TYPE`` says which feed it is in."""
CREDIT_ID: Final = Attribute("credit_id", STR)
KIND: Final = Attribute("kind", STR)
"""``cast`` | ``crew``."""
PERSON_ID: Final = Attribute("person_id", INT)
NAME: Final = Attribute("name", STR)
GENDER: Final = Attribute("gender", INT)
KNOWN_FOR_DEPARTMENT: Final = Attribute("known_for_department", STR, optional=True)
PERSON_POPULARITY: Final = Attribute("person_popularity", FLOAT)
CHARACTER: Final = Attribute("character", STR, optional=True)
BILLING_ORDER: Final = Attribute("billing_order", INT, optional=True)
"""TMDB's ``order`` — renamed because ``order`` is an SQL keyword downstream."""
DEPARTMENT: Final = Attribute("department", STR, optional=True)
JOB: Final = Attribute("job", STR, optional=True)
EPISODE_COUNT: Final = Attribute("episode_count", INT, optional=True)

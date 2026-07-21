"""Schema for the GDELT pipeline: column-name tuples + typed attributes.

Two halves, both following the framework's rules:

1. **Column-name tuples** for the three GDELT 2.0 tables, transcribed from the
   published codebooks (Event 2.0, Mentions 2.0, GKG 2.1 — links in the README).
   The files are headerless TSV, so the parser (:mod:`.parsers`) zips these names
   positionally onto each row; **do not guess positions**. Encoding the whole list
   keeps every computed column landing on the right name even as we read only a few.

2. **Typed ``Attribute`` handles** — declared *only* for fields a stage computes
   with (the framework's typed-attributes rule). Every other column rides through
   as a string inside the nested ``ROW`` record under its own wire (column) name,
   exactly as ADS-B spreads the adsb.lol feed: a raw row can never collide with our
   derived fields, and a column we don't model today needs no code change to reach
   ClickHouse. The heads below name the columns we read; the polymorphic sub-fields
   (``V2Tone``'s comma tuple, the ``;``-separated entity/location lists) are strings
   here and parsed at the compute site by :mod:`.parsers`, never declared per-part.
"""
from datetime import datetime
from typing import Final

from flechtwerk.attribute import Attribute, DATETIME, DICT, FLOAT, INT, LIST, RECORD, SET, STR

# --- Column-name tuples (headerless TSV, positional) ---

EVENTS_COLUMNS: Final[tuple[str, ...]] = (
    "GlobalEventID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
)
"""GDELT 2.0 Event table — 61 columns."""

MENTIONS_COLUMNS: Final[tuple[str, ...]] = (
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID",
    "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset", "InRawText",
    "Confidence", "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo",
    "Extras",
)
"""GDELT 2.0 Mentions table — 16 columns."""

GKG_COLUMNS: Final[tuple[str, ...]] = (
    "GKGRecordId", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
    "DocumentIdentifier", "V1Counts", "V21Counts", "V1Themes", "V2EnhancedThemes",
    "V1Locations", "V2EnhancedLocations", "V1Persons", "V2EnhancedPersons",
    "V1Organizations", "V2EnhancedOrganizations", "V2Tone", "V21EnhancedDates",
    "V2GCAM", "V21SharingImage", "V21RelatedImages", "V21SocialImageEmbeds",
    "V21SocialVideoEmbeds", "V21Quotations", "V21AllNames", "V21Amounts",
    "V21TranslationInfo", "V2ExtrasXML",
)
"""GDELT 2.1 GKG table — 27 columns."""

COLUMNS_BY_TABLE: Final[dict[str, tuple[str, ...]]] = {
    "events": EVENTS_COLUMNS,
    "mentions": MENTIONS_COLUMNS,
    "gkg": GKG_COLUMNS,
}

# --- Raw message shape (ingest output): the row + provenance, each nested ---

ROW: Final = Attribute("row", RECORD)
"""The parsed GDELT row, column-name → string value — the uncontrolled upstream in its
own namespace (ADS-B's spread-through convention), so it can never collide with ours."""
METADATA: Final = Attribute("metadata", RECORD)
"""Poll provenance, nested: ``FILE_TS``, ``FEED``, ``TABLE``, ``ROW_NUMBER``, ``FETCHED_AT``."""
FILE_TS: Final = Attribute("file_ts", DATETIME)
"""The 15-minute file timestamp this row came from — the authoritative event time (all
rows in one file share it, so windows are exact). Never trust the row's own ``SQLDATE``."""
FEED: Final = Attribute("feed", STR)
"""``english`` | ``translation`` — which GDELT feed produced the row."""
TABLE: Final = Attribute("table", STR)
"""``events`` | ``mentions`` | ``gkg`` — which table the row belongs to."""
ROW_NUMBER: Final = Attribute("row_number", INT)
"""1-based line number within the file — provenance for tracing a row back."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""Wall-clock time ingest downloaded the file — feed-latency provenance."""

# --- Event columns we compute with (read off ROW; wire = codebook column name) ---

GLOBAL_EVENT_ID: Final = Attribute("GlobalEventID", STR)
"""The event identity — the ``gdelt-events-raw`` / ``gdelt-mentions-raw`` message key and
the join's state key. A string end to end (it is numeric on the wire, but a key is a key)."""
EVENT_ROOT_CODE: Final = Attribute("EventRootCode", STR, optional=True)
"""CAMEO root action code (e.g. ``14`` = protest) — the coverage record's event summary."""
DATE_ADDED: Final = Attribute("DATEADDED", STR, optional=True)
"""When GDELT added the event, ``YYYYMMDDHHMMSS``. Cross-checked against the file timestamp;
the row's ``SQLDATE`` is deliberately NOT modelled (machine-coded, observed a year stale)."""
AVG_TONE: Final = Attribute("AvgTone", STR, optional=True)
"""Average document tone for the event, a signed number as a string — parsed at the site."""
ACTION_GEO_FULLNAME: Final = Attribute("ActionGeo_FullName", STR, optional=True)
"""Human place name of where the event happened — the coverage record's location."""
ACTION_GEO_COUNTRY: Final = Attribute("ActionGeo_CountryCode", STR, optional=True)
"""FIPS country code of the action location."""
ACTION_GEO_LAT: Final = Attribute("ActionGeo_Lat", STR, optional=True)
"""Action latitude as a string ('' when GDELT geocoded nothing) — the world tone map."""
ACTION_GEO_LONG: Final = Attribute("ActionGeo_Long", STR, optional=True)
"""Action longitude as a string ('' when absent)."""
SOURCE_URL: Final = Attribute("SOURCEURL", STR, optional=True)
"""The event's source article URL."""

# --- Mention columns we compute with ---

MENTION_SOURCE_NAME: Final = Attribute("MentionSourceName", STR, optional=True)
"""The outlet that carried the mention (a domain, e.g. ``bbc.co.uk``) — a distinct-source count."""
MENTION_IDENTIFIER: Final = Attribute("MentionIdentifier", STR, optional=True)
"""The mention's article URL."""
MENTION_TIME_DATE: Final = Attribute("MentionTimeDate", STR, optional=True)
"""When the mention was found, ``YYYYMMDDHHMMSS`` — first/last-mention window bounds."""
MENTION_DOC_TONE: Final = Attribute("MentionDocTone", STR, optional=True)
"""Tone of the mentioning document, a signed number as a string."""

# --- GKG columns we compute with ---

DOCUMENT_IDENTIFIER: Final = Attribute("DocumentIdentifier", STR)
"""The article URL — the ``gdelt-gkg-raw`` message key and the clustering dedup key."""
SOURCE_COMMON_NAME: Final = Attribute("SourceCommonName", STR, optional=True)
"""The outlet's common name / domain (e.g. ``lemonde.fr``) — the coverage-spread lookup key."""
GKG_DATE: Final = Attribute("DATE", STR, optional=True)
"""GKG record timestamp, ``YYYYMMDDHHMMSS`` (redundant with the file ts; kept for provenance)."""
V2_TONE: Final = Attribute("V2Tone", STR, optional=True)
"""The 7-element comma tuple (tone,pos,neg,polarity,activity,selfgroup,wordcount) — parsed
at the site by :func:`.parsers.parse_tone`, never declared per-element."""
V1_THEMES: Final = Attribute("V1Themes", STR, optional=True)
"""``;``-separated GKG theme tags — the plain (offset-free) theme list."""
V2_ENHANCED_PERSONS: Final = Attribute("V2EnhancedPersons", STR, optional=True)
"""``;``-separated ``name,charoffset`` person mentions — offsets stripped at the site."""
V2_ENHANCED_ORGANIZATIONS: Final = Attribute("V2EnhancedOrganizations", STR, optional=True)
"""``;``-separated ``name,charoffset`` organization mentions — offsets stripped at the site."""
V2_ENHANCED_LOCATIONS: Final = Attribute("V2EnhancedLocations", STR, optional=True)
"""``;``-separated ``type#name#cc#adm1#lat#lon#featureid`` location blocks — parsed at the site."""

# --- Config topic: one record per feed to poll (wire key = feed name) ---

FEED_NAME: Final = Attribute("feed", STR)
"""``english`` | ``translation`` — the feed a ``gdelt-feeds`` config record names (its key too)."""

# --- Config topic: outlet metadata (wire key = domain) ---

OUTLET_DOMAIN: Final = Attribute("domain", STR)
"""The outlet's domain (the ``gdelt-outlets`` config key), matched against a GKG source name."""
OUTLET_NAME: Final = Attribute("name", STR, optional=True)
"""The outlet's human name (e.g. ``Le Monde``)."""
OUTLET_COUNTRY: Final = Attribute("country", STR, optional=True)
"""The outlet's home country (ISO-2 or a short label) — the coverage-spread dimension.
A leaning/bias column would plug in right here (deliberately not shipped — see the README)."""

# --- Event-coverage output (GdeltEventCoverage → gdelt-event-coverage) ---

MENTION_COUNT: Final = Attribute("mention_count", INT)
"""How many mentions this event has accumulated (breaking-news velocity, per file window)."""
DISTINCT_SOURCES: Final = Attribute("distinct_sources", INT)
"""Distinct outlet names mentioning the event — coverage breadth."""
FIRST_MENTION_AT: Final = Attribute("first_mention_at", DATETIME, optional=True)
"""Earliest mention timestamp seen for the event."""
LAST_MENTION_AT: Final = Attribute("last_mention_at", DATETIME, optional=True)
"""Latest mention timestamp seen for the event."""
EVENT_SEEN: Final = Attribute("event_seen", INT)
"""``1`` once the event row itself has arrived, ``0`` while only orphan mentions are buffered."""
UPDATED_AT: Final = Attribute("updated_at", DATETIME)
"""When the coverage record was last emitted (the file ts of the triggering row)."""

# --- Event-coverage state (per GlobalEventID) ---

SOURCES: Final = Attribute("sources", SET(STR))
"""The set of distinct outlet names mentioning the event — the distinct-source count's backing."""
ORPHANED_AT: Final = Attribute("orphaned_at", DATETIME, optional=True)
"""When the first orphan mention (a mention whose event row hasn't arrived) was buffered —
the TTL clock; the bucket is tombstoned once it exceeds ``ORPHAN_TTL`` unresolved."""

# --- Stories output (GdeltStories → gdelt-stories) ---

STORY_ID: Final = Attribute("story_id", STR)
"""Stable cluster id (hash of the first article's URL + file ts) — the story message key."""
ARTICLE_COUNT: Final = Attribute("article_count", INT)
"""How many distinct articles the cluster holds (breaking-news velocity per story)."""
SOURCE_DOMAINS: Final = Attribute("source_domains", LIST(STR))
"""Distinct outlet domains covering the story."""
COUNTRIES: Final = Attribute("countries", LIST(STR))
"""Distinct outlet home countries covering the story (annotated from ``gdelt-outlets``;
unknown domains contribute nothing) — the coverage-spread dimension."""
COUNTRY_COUNT: Final = Attribute("country_count", INT)
"""How many distinct countries' outlets cover the story (``1`` = a single-country story)."""
TOP_ENTITIES: Final = Attribute("top_entities", LIST(STR))
"""The cluster's representative persons ∪ orgs ∪ themes (its similarity feature set)."""
FIRST_SEEN: Final = Attribute("first_seen", DATETIME)
"""File ts of the story's first article."""
LAST_SEEN: Final = Attribute("last_seen", DATETIME)
"""File ts of the story's most recent article (the TTL clock for eviction)."""
AVG_STORY_TONE: Final = Attribute("avg_tone", FLOAT, optional=True)
"""Running mean document tone across the story's articles."""
SAMPLE_TITLE: Final = Attribute("sample_url", STR, optional=True)
"""A representative article URL for the story (the first one seen)."""

# --- Stories state (single bucket: all live clusters) ---

CLUSTERS: Final = Attribute("clusters", DICT(RECORD))
"""story_id → the cluster record (feature set, counts, domains, tone stats, first/last seen).
All clusters live in ONE state bucket because clustering needs cross-article visibility;
that is why the clustering input is single-partition (see ``stories.py``)."""
SEEN_URLS: Final = Attribute("seen_urls", DICT(DATETIME))
"""article URL → last-seen file ts, for dedup with TTL (a re-crawled URL is skipped;
entries older than ``SEEN_TTL`` are pruned so the map can't grow without bound)."""

# Per-cluster record fields (nested inside CLUSTERS; their own namespace, distinct wire
# names from the output record's — sets here, lists there). FIRST_SEEN / LAST_SEEN /
# ARTICLE_COUNT / SAMPLE_TITLE are shared with the output (same codec) and reused.
CLUSTER_FEATURES: Final = Attribute("features", SET(STR))
"""The cluster's representative feature set (persons ∪ orgs ∪ top themes) for similarity."""
CLUSTER_DOMAINS: Final = Attribute("domains", SET(STR))
"""Distinct outlet domains that have contributed an article to the cluster."""
CLUSTER_COUNTRIES: Final = Attribute("member_countries", SET(STR))
"""Distinct outlet home countries (annotated) contributing to the cluster."""
CLUSTER_TONE_SUM: Final = Attribute("tone_sum", FLOAT, optional=True)
"""Running sum of article tone; divided by ``ARTICLE_COUNT`` for the story's mean tone."""


def parse_gdelt_datetime(value: str | None) -> datetime | None:
    """Parse a GDELT ``YYYYMMDDHHMMSS`` (or ``YYYYMMDD``) stamp to an aware UTC datetime.

    Pure and defensive: ``None``/empty/malformed → ``None`` (a stage falls back to the
    file timestamp), never a raise. Lives here (not in :mod:`.parsers`) because it reads
    a value addressed by a schema attribute; the row-shredding parsers stay import-free.
    """
    from datetime import timezone

    if not value:
        return None
    digits = value.strip()
    try:
        if len(digits) == 14:
            return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        if len(digits) == 8:
            return datetime.strptime(digits, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None

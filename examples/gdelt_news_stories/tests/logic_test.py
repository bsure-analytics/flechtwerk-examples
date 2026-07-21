"""Tier 1 — pure logic. No framework machinery, no fakes, no I/O beyond reading
the committed fixtures. Covers the row-shredding and sub-syntax parsers and the
GDELT timestamp helper, plus the data-quirk regressions the README calls out.
"""
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flechtwerk import Event, IncomingMessage, Message, State
from flechtwerk.attribute import Record

from examples.gdelt_news_stories.coverage import COVERAGE_TOPIC, ORPHAN_TTL, join_coverage
from examples.gdelt_news_stories.ingest import (
    EVENTS_RAW_TOPIC,
    GKG_RAW_TOPIC,
    MENTIONS_RAW_TOPIC,
    build_page,
    parse_pointer,
    table_of,
)
from examples.gdelt_news_stories.parsers import (
    decode_table,
    parse_entities,
    parse_locations,
    parse_table,
    parse_tone,
    row_to_dict,
)
from examples.gdelt_news_stories.outlets import OUTLETS_TOPIC, outlet_messages
from examples.gdelt_news_stories.sink import COVERAGE_TABLE, STORIES_TABLE, dedup_token, to_rows
from examples.gdelt_news_stories.stories import STORIES_TOPIC
from examples.gdelt_news_stories.stories import (
    MIN_FEATURES,
    STORY_TTL,
    article_features,
    assign,
    cap,
    country_from_tld,
    prune,
    similarity,
    story_record,
)
import examples.gdelt_news_stories.stories as stories_mod
from examples.gdelt_news_stories.schema import (
    ARTICLE_COUNT,
    AVG_STORY_TONE,
    CLUSTER_COUNTRIES,
    CLUSTER_DOMAINS,
    CLUSTER_FEATURES,
    CLUSTER_TONE_SUM,
    COUNTRIES,
    COUNTRY_COUNT,
    DISTINCT_SOURCES,
    DOCUMENT_IDENTIFIER,
    EVENT_ROOT_CODE,
    EVENT_SEEN,
    EVENTS_COLUMNS,
    FEED,
    FILE_TS,
    FIRST_MENTION_AT,
    FIRST_SEEN,
    GKG_COLUMNS,
    GLOBAL_EVENT_ID,
    LAST_MENTION_AT,
    LAST_SEEN,
    MENTION_COUNT,
    MENTIONS_COLUMNS,
    METADATA,
    ROW,
    ROW_NUMBER,
    SAMPLE_TITLE,
    SOURCE_DOMAINS,
    STORY_ID,
    TABLE,
    TOP_ENTITIES,
    UPDATED_AT,
    ACTION_GEO_LAT,
    AVG_TONE,
    parse_gdelt_datetime,
)

FIXTURES = Path(__file__).parent / "fixtures"

FILE_TS_TS = datetime(2026, 7, 21, 8, 30, tzinfo=timezone.utc)
FETCHED_AT_TS = datetime(2026, 7, 21, 8, 31, tzinfo=timezone.utc)


def _unzip(name: str) -> bytes:
    with zipfile.ZipFile(FIXTURES / name) as zf:
        return zf.read(zf.namelist()[0])


# --- row_to_dict: positional zip, drop empties, tolerate short/long rows ---

def test_row_to_dict_zips_columns_and_drops_empty_cells() -> None:
    columns = ("a", "b", "c")
    assert row_to_dict(columns, "1\t\t3") == {"a": "1", "c": "3"}  # empty middle cell dropped


def test_row_to_dict_pads_short_and_folds_long_rows() -> None:
    columns = ("a", "b", "c")
    assert row_to_dict(columns, "1") == {"a": "1"}                 # short → the rest absent
    # A literal tab inside the trailing free-text column must not shift every field left;
    # the overflow folds back into the last column so nothing is lost.
    assert row_to_dict(columns, "1\t2\t3\textra") == {"a": "1", "b": "2", "c": "3\textra"}


# --- parse_tone: the V2Tone comma tuple ---

def test_parse_tone_reads_the_seven_components() -> None:
    tone = parse_tone("-2.5,3.1,5.6,8.7,20.0,1.2,180")
    assert tone == {"tone": -2.5, "positive": 3.1, "negative": 5.6, "polarity": 8.7,
                    "activity_density": 20.0, "self_group_density": 1.2, "word_count": 180.0}


def test_parse_tone_is_defensive() -> None:
    assert parse_tone(None) is None
    assert parse_tone("") is None
    assert parse_tone("junk") is None            # leading tone not numeric → skipped, not raised
    assert parse_tone("-1.0,oops")["tone"] == -1.0  # keeps the clean prefix, stops at the garbage


# --- parse_entities: V1 and V2-enhanced lists ---

def test_parse_entities_strips_offsets_lowercases_and_dedupes() -> None:
    # V2-enhanced: "Name,charoffset;..." — offsets stripped, order-preserving distinct.
    assert parse_entities("Angela Merkel,1345;NATO,20;Angela Merkel,9001") == ["angela merkel", "nato"]
    # V1 plain (no offsets) passes through, lowercased.
    assert parse_entities("United Nations;World Bank") == ["united nations", "world bank"]
    assert parse_entities(None) == [] and parse_entities("") == []


def test_parse_entities_keeps_a_name_that_contains_a_comma() -> None:
    # Only a trailing all-digit suffix is treated as an offset; a comma inside a name stays.
    assert parse_entities("Company, Inc,42") == ["company, inc"]


# --- parse_locations: the #-subfield blocks ---

def test_parse_locations_reads_a_block_and_skips_malformed() -> None:
    value = "4#Paris, France#FR#FR11#FR11#48.85#2.35#-1428125#320;bad#block"
    locations = parse_locations(value)
    assert len(locations) == 1  # the "bad#block" (too few subfields) is skipped
    assert locations[0]["name"] == "Paris, France"
    assert (locations[0]["lat"], locations[0]["lon"]) == (48.85, 2.35)
    assert parse_locations(None) == []


# --- schema.parse_gdelt_datetime ---

def test_parse_gdelt_datetime_handles_both_widths_and_junk() -> None:
    assert parse_gdelt_datetime("20260721083000") == datetime(2026, 7, 21, 8, 30, tzinfo=timezone.utc)
    assert parse_gdelt_datetime("20260721") == datetime(2026, 7, 21, tzinfo=timezone.utc)
    assert parse_gdelt_datetime(None) is None
    assert parse_gdelt_datetime("") is None
    assert parse_gdelt_datetime("nonsense") is None


# --- fixtures: column counts + the SQLDATE≠DATEADDED quirk ---

def test_fixture_tables_have_the_codebook_column_counts() -> None:
    for name, columns in (
        ("20260721083000.export.CSV.zip", EVENTS_COLUMNS),
        ("20260721083000.mentions.CSV.zip", MENTIONS_COLUMNS),
        ("20260721083000.gkg.csv.zip", GKG_COLUMNS),
    ):
        first = decode_table(_unzip(name)).splitlines()[0]
        assert len(first.split("\t")) == len(columns), name


# --- ingest: parse_pointer / table_of / build_page ---

def test_parse_pointer_reads_size_md5_and_basename() -> None:
    text = ("60092 47552eb8d0e11d1000c4d66f8ede7143 http://data.gdeltproject.org/gdeltv2/x.export.CSV.zip\n"
            "78379 20de6b1bc27f416af5bb4489521dc17c http://data.gdeltproject.org/gdeltv2/x.mentions.CSV.zip\n")
    entries = parse_pointer(text)
    assert entries[0] == (60092, "47552eb8d0e11d1000c4d66f8ede7143", "x.export.CSV.zip")
    assert table_of(entries[0][2]) == "events" and table_of(entries[1][2]) == "mentions"
    assert table_of("something.else.zip") is None


def test_build_page_wraps_rows_with_row_and_metadata_and_keys_them() -> None:
    tables = {
        "events": _unzip("20260721083000.export.CSV.zip"),
        "mentions": _unzip("20260721083000.mentions.CSV.zip"),
        "gkg": _unzip("20260721083000.gkg.csv.zip"),
    }
    messages = list(build_page(tables, feed="english", file_ts=FILE_TS_TS, fetched_at=FETCHED_AT_TS))

    by_topic: dict[str, list] = {}
    for m in messages:
        by_topic.setdefault(m.topic, []).append(m)
    # every parsed row becomes one message (all fixture rows carry their key column)
    assert len(by_topic[EVENTS_RAW_TOPIC]) == len(parse_table(tables["events"], EVENTS_COLUMNS))
    assert len(by_topic[MENTIONS_RAW_TOPIC]) == len(parse_table(tables["mentions"], MENTIONS_COLUMNS))
    assert len(by_topic[GKG_RAW_TOPIC]) == 300

    event = by_topic[EVENTS_RAW_TOPIC][0]
    assert event.key == event.value[ROW][GLOBAL_EVENT_ID]              # keyed by GlobalEventID
    assert event.value[METADATA][FILE_TS] == FILE_TS_TS               # authoritative event time
    assert event.value[METADATA][FEED] == "english"
    assert event.value[METADATA][TABLE] == "events"
    assert event.value[METADATA][ROW_NUMBER] == 1
    gkg = by_topic[GKG_RAW_TOPIC][0]
    assert gkg.key == gkg.value[ROW][DOCUMENT_IDENTIFIER]             # gkg keyed by article URL


def test_build_page_skips_a_keyless_row() -> None:
    # A row missing its key column is malformed data — skipped defensively, not crashed.
    events = b"\tSKIP\n1314546129\tKEEP\n"  # first row has no GlobalEventID (empty first cell)
    messages = list(build_page({"events": events}, feed="english", file_ts=FILE_TS_TS, fetched_at=FETCHED_AT_TS))
    assert [m.key for m in messages] == ["1314546129"]


# --- coverage: the co-partitioned Events ⋈ Mentions join (join_coverage) ---

T0 = datetime(2026, 7, 21, 8, 30, tzinfo=timezone.utc)


def _raw_msg(table: str, row: dict, file_ts: datetime, key: str) -> IncomingMessage:
    return IncomingMessage(key=key, offset=0, partition=0, timestamp=None, topic=table,
                           value=Event({ROW: Record.wrap(row), METADATA: Record({TABLE: table, FILE_TS: file_ts})}))


def _event_msg(event_id: str, file_ts: datetime = T0, **row) -> IncomingMessage:
    return _raw_msg("events", {"GlobalEventID": event_id, **row}, file_ts, event_id)


def _mention_msg(event_id: str, file_ts: datetime = T0, **row) -> IncomingMessage:
    return _raw_msg("mentions", {"GlobalEventID": event_id, **row}, file_ts, event_id)


async def _join(msgs: list[IncomingMessage]) -> tuple[list[list[Message]], State]:
    """Drive join_coverage sequentially, threading the emitted state as the real runner would."""
    state = State()
    emitted: list[list[Message]] = []
    for msg in msgs:
        items = [item async for item in join_coverage(state, msg)]
        emitted.append([i for i in items if isinstance(i, Message)])
        states = [i for i in items if isinstance(i, State)]
        if states:
            state = states[-1]
    return emitted, state


async def test_event_first_then_mention_reconciles_in_order() -> None:
    emitted, state = await _join([
        _event_msg("42", EventRootCode="14", ActionGeo_FullName="Paris, France", AvgTone="-3.2"),
        _mention_msg("42", MentionSourceName="lemonde.fr"),
    ])
    assert emitted[0][0].topic == COVERAGE_TOPIC
    first = emitted[0][0].value
    assert first[EVENT_SEEN] == 1 and first[EVENT_ROOT_CODE] == "14"
    assert first[MENTION_COUNT] == 0
    latest = emitted[1][0].value
    assert latest[MENTION_COUNT] == 1 and latest[DISTINCT_SOURCES] == 1 and latest[EVENT_SEEN] == 1


async def test_mention_before_event_is_buffered_as_an_orphan_then_reconciled() -> None:
    # The teaching point: a mention can arrive before its event row. It is buffered
    # (event_seen=0) and reconciled the moment the event lands, keeping its aggregates.
    emitted, state = await _join([
        _mention_msg("42", MentionSourceName="bbc.co.uk"),
        _mention_msg("42", MentionSourceName="reuters.com"),
        _event_msg("42", EventRootCode="14"),
    ])
    orphan = emitted[0][0].value
    assert orphan[EVENT_SEEN] == 0 and orphan[MENTION_COUNT] == 1  # buffered, event not yet seen
    reconciled = emitted[2][0].value
    assert reconciled[EVENT_SEEN] == 1                              # event landed → resolved
    assert reconciled[MENTION_COUNT] == 2 and reconciled[DISTINCT_SOURCES] == 2  # aggregates kept


async def test_distinct_sources_and_mention_window() -> None:
    emitted, _ = await _join([
        _mention_msg("42", T0, MentionSourceName="bbc.co.uk", MentionTimeDate="20260721083000"),
        _mention_msg("42", T0, MentionSourceName="bbc.co.uk", MentionTimeDate="20260721081500"),  # same source, earlier
    ])
    latest = emitted[1][0].value
    assert latest[MENTION_COUNT] == 2 and latest[DISTINCT_SOURCES] == 1        # deduped source
    assert latest[FIRST_MENTION_AT] == datetime(2026, 7, 21, 8, 15, tzinfo=timezone.utc)
    assert latest[LAST_MENTION_AT] == datetime(2026, 7, 21, 8, 30, tzinfo=timezone.utc)


async def test_orphan_past_ttl_is_tombstoned_by_a_straggler() -> None:
    # An orphan whose event never came, touched by a straggler > ORPHAN_TTL later, is
    # tombstoned via a falsy State (the key is deleted) and the straggler dropped.
    late = T0 + ORPHAN_TTL + timedelta(hours=1)
    emitted, state = await _join([
        _mention_msg("42", T0, MentionSourceName="bbc.co.uk"),
        _mention_msg("42", late, MentionSourceName="late.example"),
    ])
    assert emitted[1] == []       # straggler produced no coverage record…
    assert not state              # …and the buffer was tombstoned (empty/falsy State)


async def test_event_after_ttl_rebuilds_fresh() -> None:
    # If the event finally arrives after the orphan expired, the stale mention aggregates
    # are discarded and coverage is rebuilt from the event alone.
    late = T0 + ORPHAN_TTL + timedelta(hours=1)
    emitted, _ = await _join([
        _mention_msg("42", T0, MentionSourceName="bbc.co.uk"),
        _event_msg("42", late, EventRootCode="14"),
    ])
    rebuilt = emitted[1][0].value
    assert rebuilt[EVENT_SEEN] == 1 and rebuilt[MENTION_COUNT] == 0  # stale mentions dropped


# --- stories: online clustering (assign / prune / story_record / article_features) ---

def _assign(clusters, seen, url, features, *, domain="x.com", country=None, tone=None, file_ts=T0):
    return assign(clusters, seen, url=url, features=set(features), domain=domain,
                  country=country, tone=tone, file_ts=file_ts)


def test_country_from_tld_derives_cctld_and_skips_gtld() -> None:
    assert country_from_tld("bbc.co.uk") == "GB"       # ccTLD → derived, no data needed
    assert country_from_tld("spiegel.de") == "DE"
    assert country_from_tld("abc.net.au") == "AU"
    assert country_from_tld("nytimes.com") is None     # gTLD → not derivable (needs an override)
    assert country_from_tld("startup.io") is None      # vanity ccTLD → deliberately excluded
    assert country_from_tld("noTldHere") is None


def test_similarity_is_the_overlap_coefficient() -> None:
    assert similarity({"a", "b", "c"}, {"a", "b"}) == 1.0        # subset → 2/min(3,2)=1.0
    assert similarity({"a", "b"}, {"c", "d"}) == 0.0
    assert similarity(set(), {"a"}) == 0.0


def test_same_story_articles_cluster_and_unrelated_ones_do_not() -> None:
    clusters, seen = {}, {}
    shared = {"keir starmer", "andy burnham", "john healey", "office for budget responsibility"}
    a = _assign(clusters, seen, "http://a", shared, domain="yorkpress.co.uk")
    b = _assign(clusters, seen, "http://b", shared | {"rachel reeves"}, domain="london-now.co.uk")
    assert a == b                                                # same story
    assert len(clusters) == 1 and clusters[a].get(ARTICLE_COUNT) == 2
    assert clusters[a].get(CLUSTER_DOMAINS) == {"yorkpress.co.uk", "london-now.co.uk"}
    c = _assign(clusters, seen, "http://c", {"amie bunnik", "croatian national tourist board"}, domain="smh.com.au")
    assert c is not None and c != a and len(clusters) == 2       # unrelated → its own cluster


def test_duplicate_url_is_deduplicated() -> None:
    clusters, seen = {}, {}
    features = {"keir starmer", "andy burnham", "john healey"}
    first = _assign(clusters, seen, "http://a", features)
    again = _assign(clusters, seen, "http://a", features)        # same URL re-crawled
    assert again is None and clusters[first].get(ARTICLE_COUNT) == 1  # not counted twice


def test_thin_article_is_marked_seen_but_not_clustered() -> None:
    clusters, seen = {}, {}
    assert _assign(clusters, seen, "http://a", {"only-one"}) is None   # < MIN_FEATURES
    assert MIN_FEATURES == 2 and clusters == {} and len(seen) == 1      # seen (by URL hash)


def test_ttl_eviction_prunes_idle_clusters_and_seen_urls() -> None:
    clusters, seen = {}, {}
    _assign(clusters, seen, "http://a", {"keir starmer", "andy burnham"}, file_ts=T0)
    assert clusters and seen
    prune(clusters, seen, T0 + STORY_TTL + timedelta(hours=1))    # a slice well past the TTL
    assert clusters == {} and seen == {}                          # both evicted


def test_cap_bounds_the_single_bucket_keeping_the_most_recent(monkeypatch) -> None:
    # The single state bucket holds all clusters + seen URLs in one changelog record, which
    # Kafka caps at ~1 MB — so cap() evicts the least-recently-seen beyond the hard limits.
    monkeypatch.setattr(stories_mod, "MAX_CLUSTERS", 2)
    monkeypatch.setattr(stories_mod, "MAX_SEEN_URLS", 2)
    clusters = {f"s{i}": Record({LAST_SEEN: T0 + timedelta(minutes=i)}) for i in range(5)}
    seen = {f"u{i}": T0 + timedelta(minutes=i) for i in range(5)}
    cap(clusters, seen)
    assert set(clusters) == {"s3", "s4"} and set(seen) == {"u3", "u4"}  # newest kept


def test_story_record_projects_coverage_spread_and_mean_tone() -> None:
    cluster = Record({
        CLUSTER_FEATURES: {"keir starmer", "andy burnham"},
        ARTICLE_COUNT: 3,
        CLUSTER_DOMAINS: {"bbc.co.uk", "lemonde.fr", "dw.com"},
        CLUSTER_COUNTRIES: {"GB", "FR", "DE"},
        CLUSTER_TONE_SUM: -6.0,
        FIRST_SEEN: T0,
        LAST_SEEN: T0 + timedelta(hours=1),
        SAMPLE_TITLE: "http://a",
    })
    record = story_record("story42", cluster)
    assert record[ARTICLE_COUNT] == 3
    assert record[SOURCE_DOMAINS] == ["bbc.co.uk", "dw.com", "lemonde.fr"]  # sorted
    assert record[COUNTRIES] == ["DE", "FR", "GB"] and record[COUNTRY_COUNT] == 3  # coverage spread
    assert record[AVG_STORY_TONE] == -2.0                                   # -6.0 / 3
    assert record[SAMPLE_TITLE] == "http://a"


def test_article_features_is_persons_orgs_and_filters_noise() -> None:
    row = Record.wrap({
        "V2EnhancedPersons": "Keir Starmer,10;Andy Burnham,42;a court,99",  # "a court" is junk
        "V2EnhancedOrganizations": "Labour Party,7",
        "V1Themes": "TAX_FNCACT;EPU_POLICY",  # broad themes — excluded from clustering
    })
    features = article_features(row)
    assert features == {"keir starmer", "andy burnham", "labour party"}
    assert "a court" not in features    # leading-article junk dropped
    assert "tax_fncact" not in features  # themes are not a clustering signal


def test_clustering_over_the_fixture_forms_multiple_sane_clusters() -> None:
    rows = parse_table(_unzip("20260721083000.gkg.csv.zip"), GKG_COLUMNS)
    clusters, seen = {}, {}
    for row in rows:
        record = Record.wrap(row)
        if not (url := row.get("DocumentIdentifier")):
            continue
        _assign(clusters, seen, url, article_features(record),
                domain=(row.get("SourceCommonName") or "").lower())
    multi = [c for c in clusters.values() if (c.get(ARTICLE_COUNT) or 0) >= 2]
    assert 1 < len(clusters) < len(rows)   # neither one blob nor all singletons
    assert len(multi) >= 5                 # several genuine multi-article stories formed


# --- sink: routing + row projection (to_rows / dedup_token) ---

def _incoming(topic: str, value: Event, *, partition: int = 0, offset: int = 0) -> IncomingMessage:
    return IncomingMessage(key="k", offset=offset, partition=partition, timestamp=None, topic=topic, value=value)


def test_dedup_token_is_the_kafka_coordinate() -> None:
    msg = _incoming(STORIES_TOPIC, Event({STORY_ID: "s"}), partition=3, offset=99)
    assert dedup_token(msg) == f"{STORIES_TOPIC}:3:99"


def test_to_rows_routes_and_projects_a_story() -> None:
    value = Event({STORY_ID: "story42", ARTICLE_COUNT: 3, COUNTRY_COUNT: 2,
                   SOURCE_DOMAINS: ["bbc.co.uk", "dw.com"], COUNTRIES: ["DE", "GB"],
                   TOP_ENTITIES: ["keir starmer"], FIRST_SEEN: T0, LAST_SEEN: T0,
                   AVG_STORY_TONE: -2.0, SAMPLE_TITLE: "http://a"})
    table, rows = to_rows(_incoming(STORIES_TOPIC, value))
    assert table == STORIES_TABLE and len(rows) == 1
    row = rows[0]
    assert row["story_id"] == "story42" and row["article_count"] == 3 and row["country_count"] == 2
    assert row["countries"] == ["DE", "GB"] and row["avg_tone"] == -2.0
    assert row["payload"]["story_id"] == "story42"          # whole message in the JSON catch-all


def test_to_rows_routes_and_coerces_a_coverage_record() -> None:
    value = Event({GLOBAL_EVENT_ID: "42", EVENT_SEEN: 1, MENTION_COUNT: 5, DISTINCT_SOURCES: 3,
                   UPDATED_AT: T0, EVENT_ROOT_CODE: "14", ACTION_GEO_LAT: "48.85", AVG_TONE: "-3.2"})
    table, rows = to_rows(_incoming(COVERAGE_TOPIC, value))
    assert table == COVERAGE_TABLE
    row = rows[0]
    assert row["global_event_id"] == "42" and row["mention_count"] == 5
    assert row["event_root_code"] == "14"
    assert row["action_lat"] == 48.85 and row["avg_tone"] == -3.2   # strings coerced to float
    assert "action_lon" not in row                                  # absent optional → omitted (NULL)


# --- outlets: the bundled CSV → config records ---

def test_outlet_messages_keys_by_domain_and_skips_blank_rows() -> None:
    text = "domain,name,country\nBBC.co.uk,BBC,GB\n,No Domain,US\nlemonde.fr,Le Monde,FR\n"
    messages = list(outlet_messages(text))
    assert [m.key for m in messages] == ["bbc.co.uk", "lemonde.fr"]  # lowercased; blank-domain row skipped
    assert all(m.topic == OUTLETS_TOPIC for m in messages)
    assert messages[0].value.raw["country"] == "GB"


def test_bundled_outlets_csv_is_well_formed() -> None:
    # The shipped seed table parses and every row has a domain (the config key).
    text = (Path(__file__).parent.parent / "outlets.csv").read_text()
    messages = list(outlet_messages(text))
    assert len(messages) >= 50                              # a decent DE/EU/US/world spread
    assert len({m.key for m in messages}) == len(messages)  # domains unique → clean compaction


def test_fixture_carries_the_sqldate_lags_dateadded_regression_row() -> None:
    # The README's quirk: SQLDATE is machine-coded and can be a year stale, so event time
    # MUST come from the file timestamp / DATEADDED, never SQLDATE. These two rows prove it.
    rows = {r["GlobalEventID"]: r for r in parse_table(_unzip("20260721083000.export.CSV.zip"), EVENTS_COLUMNS)}
    for event_id in ("1314546129", "1314546130"):
        row = rows[event_id]
        assert row["SQLDATE"].startswith("2025")       # a year stale…
        assert row["DATEADDED"].startswith("20260721")  # …while DATEADDED is current
        # and the year the pipeline would trust (from DATEADDED) differs from SQLDATE's year
        assert parse_gdelt_datetime(row["DATEADDED"]).year != int(row["SQLDATE"][:4])

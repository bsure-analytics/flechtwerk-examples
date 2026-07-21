-- ClickHouse sink for the GDELT news-stories pipeline.
--
-- Two tables, both written by the Flechtwerk sink stage (sink.py) — the honest
-- at-least-once counterpart to ADS-B's Kafka-engine shortcut. Each is a
-- ReplacingMergeTree keyed by the entity id with a version column, so successive
-- UPDATES to the same story / event replace rather than accumulate: query with
-- FINAL for the live picture. non_replicated_deduplication_window turns on
-- insert_deduplication_token dedup on a plain (non-replicated) MergeTree family
-- table, so a reprocessed insert carrying the same topic:partition:offset token is
-- dropped — the two mechanisms together make the sink's at-least-once writes
-- converge to exactly the same rows.
--
-- SCHEMALESS INGEST (ADS-B style): a curated set of fields is promoted into typed
-- columns for the dashboards, and the WHOLE message rides in a `payload JSON`
-- catch-all, so any field we don't promote today is queryable as `payload.<field>`
-- with no DDL change. ClickHouse's JSON type stores each path as its own columnar
-- sub-column with its own inferred type.

-- === Stories: gdelt-stories -> current state per story (clusters of articles) ===
CREATE TABLE IF NOT EXISTS flechtwerk.gdelt_stories
(
    story_id String,
    article_count UInt32,
    country_count UInt16,
    avg_tone Nullable(Float64),
    source_domains Array(String),
    countries Array(String),
    top_entities Array(String),
    sample_url String,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY story_id
SETTINGS non_replicated_deduplication_window = 1000;

-- === Event coverage: gdelt-event-coverage -> current state per GlobalEventID ===
CREATE TABLE IF NOT EXISTS flechtwerk.gdelt_event_coverage
(
    global_event_id String,
    event_seen UInt8,
    mention_count UInt32,
    distinct_sources UInt32,
    event_root_code String DEFAULT '',
    action_geo_fullname String DEFAULT '',
    action_geo_country String DEFAULT '',
    action_lat Nullable(Float64),
    action_lon Nullable(Float64),
    avg_tone Nullable(Float64),
    source_url String DEFAULT '',
    first_mention_at Nullable(DateTime64(3, 'UTC')),
    last_mention_at Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY global_event_id
SETTINGS non_replicated_deduplication_window = 1000;

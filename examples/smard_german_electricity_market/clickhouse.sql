-- ClickHouse sink for the SMARD German electricity-market example.
--
-- Like ADS-B and GTFS, this takes the Kafka-engine SHORTCUT: ClickHouse's own Kafka
-- table engine consumes the output streams directly and materialized views land the
-- rows (no Flechtwerk sink stage — that pattern is already taught by `clickhouse_sink`
-- and the GDELT sink). The stack configures the Kafka engine to read COMMITTED
-- (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are never
-- ingested — required because the upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / GDELT): each message is read whole into one JSON column
-- (`kafka_format = 'JSONAsObject'`); the materialized views PROMOTE the columns the
-- Grafana board reads into typed columns and keep the whole message in a `payload JSON`
-- catch-all, so a field we don't promote today (e.g. per-source `generation.<source>`)
-- is still queryable as `payload.<field>` with no DDL change. A missing JSON subcolumn
-- reads as a type default, so the `WHERE kind = 'observation'` views tolerate the
-- `settled` markers that share the observations topic (their absent fields never throw).
--
-- REVISIONS ARE THE POINT. `smard_observations` is a ReplacingMergeTree versioned by
-- `fetched_at`, so a corrected value UPSERTS over the one it restates — query FINAL for
-- the current best value per (series, interval). `smard_revisions` is the append-only
-- audit log of every restatement (the corrections feed). `smard_mix` is versioned by
-- `updated_at`, and a settle marker's record always carries the newest one, so FINAL
-- yields the settled value once an interval finalizes and the latest preliminary before.

-- ============================ observations + revisions ============================

-- The queue: one JSON message per row on smard-observations (observations AND settled
-- markers), read whole.
CREATE TABLE IF NOT EXISTS flechtwerk.smard_observations_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'smard-observations',
    kafka_group_name = 'smard-observations-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === Current best value per (series, interval): revisions upsert (query FINAL) ===
CREATE TABLE IF NOT EXISTS flechtwerk.smard_observations
(
    series_key String,
    name LowCardinality(String),
    role LowCardinality(String),
    source LowCardinality(String),
    ts DateTime64(3, 'UTC'),
    value Float64,
    revised UInt8,
    fetched_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (series_key, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.smard_observations_mv
TO flechtwerk.smard_observations
AS SELECT
    message.series_key::String AS series_key,
    message.name::String AS name,
    message.role::String AS role,
    message.source::String AS source,
    parseDateTime64BestEffort(message.interval_ts::String, 3) AS ts,
    message.value::Float64 AS value,
    message.revised::UInt8 AS revised,
    parseDateTime64BestEffort(message.fetched_at::String, 3) AS fetched_at,
    message AS payload
FROM flechtwerk.smard_observations_queue
WHERE message.kind::String = 'observation';

-- === Corrections feed: append-only audit of every restatement (7-day TTL) ===
CREATE TABLE IF NOT EXISTS flechtwerk.smard_revisions
(
    fetched_at DateTime64(3, 'UTC'),
    series_key String,
    name LowCardinality(String),
    ts DateTime64(3, 'UTC'),
    previous_value Float64,
    value Float64
)
ENGINE = MergeTree
ORDER BY (fetched_at, series_key)
TTL toDateTime(fetched_at) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.smard_revisions_mv
TO flechtwerk.smard_revisions
AS SELECT
    parseDateTime64BestEffort(message.fetched_at::String, 3) AS fetched_at,
    message.series_key::String AS series_key,
    message.name::String AS name,
    parseDateTime64BestEffort(message.interval_ts::String, 3) AS ts,
    message.previous_value::Float64 AS previous_value,
    message.value::Float64 AS value
FROM flechtwerk.smard_observations_queue
WHERE message.kind::String = 'observation' AND message.revised::UInt8 = 1;

-- ================================= generation mix =================================

-- The queue: one JSON message per row on smard-mix, read whole.
CREATE TABLE IF NOT EXISTS flechtwerk.smard_mix_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'smard-mix',
    kafka_group_name = 'smard-mix-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === Mix per interval: preliminary rows upsert until the final one wins (query FINAL) ===
-- Optionals are Nullable: an absent aggregate lands as NULL, never a fabricated 0
-- (a `::Nullable(Float64)` cast returns NULL for a missing JSON subcolumn).
CREATE TABLE IF NOT EXISTS flechtwerk.smard_mix
(
    ts DateTime64(3, 'UTC'),
    total_generation_mwh Nullable(Float64),
    renewables_share Nullable(Float64),
    co2_g_per_kwh Nullable(Float64),
    load_mwh Nullable(Float64),
    residual_load_mwh Nullable(Float64),
    price_eur_mwh Nullable(Float64),
    n_sources UInt8,
    is_final UInt8,
    updated_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ts;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.smard_mix_mv
TO flechtwerk.smard_mix
AS SELECT
    parseDateTime64BestEffort(message.interval_ts::String, 3) AS ts,
    message.total_generation_mwh::Nullable(Float64) AS total_generation_mwh,
    message.renewables_share::Nullable(Float64) AS renewables_share,
    message.co2_g_per_kwh::Nullable(Float64) AS co2_g_per_kwh,
    message.load_mwh::Nullable(Float64) AS load_mwh,
    message.residual_load_mwh::Nullable(Float64) AS residual_load_mwh,
    message.price_eur_mwh::Nullable(Float64) AS price_eur_mwh,
    message.n_sources::UInt8 AS n_sources,
    message.is_final::UInt8 AS is_final,
    parseDateTime64BestEffort(message.updated_at::String, 3) AS updated_at,
    message AS payload
FROM flechtwerk.smard_mix_queue;

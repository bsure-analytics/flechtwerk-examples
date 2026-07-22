-- ClickHouse sink for the GTFS German rail delays example.
--
-- Like the ADS-B example, this takes the Kafka-engine SHORTCUT: ClickHouse's own
-- Kafka table engine consumes the `gtfs-train-delays` stream directly and
-- materialized views land the rows (no Flechtwerk sink stage — that pattern is
-- already taught by `clickhouse_sink` and the GDELT sink). The stack configures
-- the Kafka engine to read COMMITTED (clickhouse/config/kafka.xml), so aborted
-- pages from a crash or handover are never ingested — required because the
-- upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / GDELT): each message is read whole into one JSON
-- column (`kafka_format = 'JSONAsObject'`); the materialized views PROMOTE only the
-- columns the Grafana board reads into typed columns and keep the whole message in a
-- `payload JSON` catch-all, so a field we don't promote today (stops_done, skipped,
-- terminus_delay_s, …) is still queryable as `payload.<field>` with no DDL change.
--
-- ONE queue, TWO views: the live board reads current state per train (a
-- ReplacingMergeTree keyed by trip_id, versioned by feed_ts, queried FINAL); the
-- network-delay timeseries reads an append-only history (a MergeTree, TTL 1 day).

-- The queue: one JSON message per row on gtfs-train-delays, read whole.
CREATE TABLE IF NOT EXISTS flechtwerk.gtfs_train_delays_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'gtfs-train-delays',
    kafka_group_name = 'gtfs-train-delays-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === Live board: current state per train (latest feed_ts wins) ===
CREATE TABLE IF NOT EXISTS flechtwerk.gtfs_train_delays
(
    trip_id String,
    feed_ts DateTime64(3, 'UTC'),
    line LowCardinality(String),
    route_type UInt16,
    destination String,
    delay_s Int32,
    status LowCardinality(String),
    next_stop String,
    lat Float64,
    lon Float64,
    payload JSON
)
ENGINE = ReplacingMergeTree(feed_ts)
ORDER BY trip_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.gtfs_train_delays_mv
TO flechtwerk.gtfs_train_delays
AS SELECT
    message.trip_id::String AS trip_id,
    parseDateTime64BestEffort(message.feed_ts::String, 3) AS feed_ts,
    message.line::String AS line,
    message.route_type::UInt16 AS route_type,
    message.destination::String AS destination,
    message.delay_s::Int32 AS delay_s,
    message.status::String AS status,
    message.next_stop::String AS next_stop,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    message AS payload
FROM flechtwerk.gtfs_train_delays_queue;

-- === History: network delay over time (append-only, pruned after a day) ===
CREATE TABLE IF NOT EXISTS flechtwerk.gtfs_delay_history
(
    feed_ts DateTime64(3, 'UTC'),
    trip_id String,
    line LowCardinality(String),
    delay_s Int32
)
ENGINE = MergeTree
ORDER BY (feed_ts, trip_id)
TTL toDateTime(feed_ts) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.gtfs_delay_history_mv
TO flechtwerk.gtfs_delay_history
AS SELECT
    parseDateTime64BestEffort(message.feed_ts::String, 3) AS feed_ts,
    message.trip_id::String AS trip_id,
    message.line::String AS line,
    message.delay_s::Int32 AS delay_s
FROM flechtwerk.gtfs_train_delays_queue;

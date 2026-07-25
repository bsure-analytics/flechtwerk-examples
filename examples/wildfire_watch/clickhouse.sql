-- ClickHouse sink for the Wildfire Watch example.
--
-- Like ADS-B / SMARD / GTFS / Odds, this takes the Kafka-engine SHORTCUT: ClickHouse's own
-- Kafka table engine consumes the output streams directly and materialized views land the
-- rows (no Flechtwerk sink stage — that pattern is already taught by `clickhouse_sink` and
-- the GDELT sink). The stack configures the Kafka engine to read COMMITTED
-- (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are never
-- ingested — required because the upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / SMARD / Odds): each message is read whole into one JSON column
-- (`kafka_format = 'JSONAsObject'`); the materialized views PROMOTE the columns the Grafana
-- board reads into typed columns and keep the whole message in a `payload JSON` catch-all, so
-- a field we don't promote today (`version`, `merged_into` on an unexpected kind, …) is still
-- queryable as `payload.<field>` with no DDL change. Optional measurements are
-- `Nullable(Float64)`: an absent JSON subcolumn cast to `::Nullable(Float64)` returns NULL,
-- never a fabricated 0 — which matters more here than usual, because a genuine `frp` of 0.0
-- does occur in real FIRMS data, so NULL and 0.0 have to stay distinguishable. Optional
-- timestamps use `parseDateTime64BestEffortOrNull`, since the plain parser rejects the empty
-- string an absent subcolumn yields.
--
-- ONE MIXED-KIND TOPIC, FILTERED VIEWS (the SMARD idiom): `wildfire-detections` carries both
-- hotspot pixels and the per-poll `sweep` markers, so each view selects its own kind with
-- `WHERE message.kind::String = '…'` and a missing subcolumn on the other kind never throws.
--
-- ONE TOPIC, TWO TABLES (the GTFS idiom): `wildfire-status` feeds both an append-only history
-- (`wildfire_status`, the FRP timeline) and a current-state table (`wildfire_active`, a
-- ReplacingMergeTree versioned by `as_of`, queried FINAL) — the same rows, read two ways.
--
-- WHY `wildfire_sweeps` EXISTS. The sweep marker is the example's whole point: it beats even when
-- a region is quiet, which is what lets a dashboard tell "nothing is burning" from "the poller
-- has stopped". A freshness panel built on detections alone would call a peaceful region stale,
-- so the heartbeat gets its own tiny table. `wildfire_events` keeps NO TTL — a fire's life story is
-- sparse and precious (the odds-signals rationale); everything else expires.

-- ========================== detections (hotspot pixels) ==========================

-- The queue: one JSON message per row on wildfire-detections (detections AND sweep markers).
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_detections_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'wildfire-detections',
    kafka_group_name = 'wildfire-detections-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === Every 375 m pixel that contained fire (the raw dots under the clusters) ===
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_detections
(
    region LowCardinality(String),
    detection_id String,
    lat Float64,
    lon Float64,
    acquired_at DateTime64(3, 'UTC'),
    satellite LowCardinality(String),
    instrument LowCardinality(String),
    confidence LowCardinality(String),
    daynight LowCardinality(String),
    frp Nullable(Float64),
    bright_ti4 Nullable(Float64),
    bright_ti5 Nullable(Float64),
    scan Float64,
    track Float64,
    fetched_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (region, acquired_at)
TTL toDateTime(acquired_at) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.wildfire_detections_mv
TO flechtwerk.wildfire_detections
AS SELECT
    message.region::String AS region,
    message.detection_id::String AS detection_id,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    parseDateTime64BestEffort(message.acquired_at::String, 3) AS acquired_at,
    message.satellite::String AS satellite,
    message.instrument::String AS instrument,
    message.confidence::String AS confidence,
    message.daynight::String AS daynight,
    message.frp::Nullable(Float64) AS frp,
    message.bright_ti4::Nullable(Float64) AS bright_ti4,
    message.bright_ti5::Nullable(Float64) AS bright_ti5,
    message.scan::Float64 AS scan,
    message.track::Float64 AS track,
    parseDateTime64BestEffort(message.fetched_at::String, 3) AS fetched_at,
    message AS payload
FROM flechtwerk.wildfire_detections_queue
WHERE message.kind::String = 'detection';

-- === The poll heartbeat: one row per region per poll, even a quiet one ===
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_sweeps
(
    region LowCardinality(String),
    sweep_at DateTime64(3, 'UTC'),
    new_detections UInt32
)
ENGINE = MergeTree
ORDER BY (region, sweep_at)
TTL toDateTime(sweep_at) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.wildfire_sweeps_mv
TO flechtwerk.wildfire_sweeps
AS SELECT
    message.region::String AS region,
    parseDateTime64BestEffort(message.sweep_at::String, 3) AS sweep_at,
    message.new_detections::UInt32 AS new_detections
FROM flechtwerk.wildfire_detections_queue
WHERE message.kind::String = 'sweep';

-- ===================== status (history + current state, one topic) =====================

CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_status_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'wildfire-status',
    kafka_group_name = 'wildfire-status-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === History: every snapshot, sweep-paced — the FRP timeline reads this ===
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_status
(
    region LowCardinality(String),
    fire_id String,
    status LowCardinality(String),
    lat Float64,
    lon Float64,
    detections UInt32,
    frp_sum Nullable(Float64),
    frp_max Nullable(Float64),
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    as_of DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (region, fire_id, as_of)
TTL toDateTime(as_of) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.wildfire_status_mv
TO flechtwerk.wildfire_status
AS SELECT
    message.region::String AS region,
    message.fire_id::String AS fire_id,
    message.status::String AS status,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    message.detections::UInt32 AS detections,
    message.frp_sum::Nullable(Float64) AS frp_sum,
    message.frp_max::Nullable(Float64) AS frp_max,
    parseDateTime64BestEffort(message.first_seen::String, 3) AS first_seen,
    parseDateTime64BestEffort(message.last_seen::String, 3) AS last_seen,
    parseDateTime64BestEffort(message.as_of::String, 3) AS as_of,
    message AS payload
FROM flechtwerk.wildfire_status_queue;

-- === Current state per fire: latest as_of wins (query FINAL) — the map reads this ===
-- The final `extinguished` snapshot is what drops a fire off the map, which is why the tracker
-- emits it *after* removing the fire from its state: `... FINAL WHERE status = 'active'`.
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_active
(
    region LowCardinality(String),
    fire_id String,
    status LowCardinality(String),
    lat Float64,
    lon Float64,
    detections UInt32,
    frp_sum Nullable(Float64),
    frp_max Nullable(Float64),
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    as_of DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(as_of)
ORDER BY (region, fire_id)
TTL toDateTime(as_of) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.wildfire_active_mv
TO flechtwerk.wildfire_active
AS SELECT
    message.region::String AS region,
    message.fire_id::String AS fire_id,
    message.status::String AS status,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    message.detections::UInt32 AS detections,
    message.frp_sum::Nullable(Float64) AS frp_sum,
    message.frp_max::Nullable(Float64) AS frp_max,
    parseDateTime64BestEffort(message.first_seen::String, 3) AS first_seen,
    parseDateTime64BestEffort(message.last_seen::String, 3) AS last_seen,
    parseDateTime64BestEffort(message.as_of::String, 3) AS as_of
FROM flechtwerk.wildfire_status_queue;

-- ============================ events (the fire log, kept) ============================

CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_events_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'wildfire-events',
    kafka_group_name = 'wildfire-events-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- Ordered by time (a life-story log is read newest-first across regions) and WITHOUT a TTL.
-- Summary fields are Nullable because only `extinguished` carries them, and `merged_into` only
-- appears on `merged` — absent stays NULL rather than becoming 0 or ''.
CREATE TABLE IF NOT EXISTS flechtwerk.wildfire_events
(
    occurred_at DateTime64(3, 'UTC'),
    kind LowCardinality(String),
    region LowCardinality(String),
    fire_id String,
    lat Float64,
    lon Float64,
    detections Nullable(UInt32),
    frp_max Nullable(Float64),
    first_seen Nullable(DateTime64(3, 'UTC')),
    last_seen Nullable(DateTime64(3, 'UTC')),
    merged_into Nullable(String),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (occurred_at, region);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.wildfire_events_mv
TO flechtwerk.wildfire_events
AS SELECT
    parseDateTime64BestEffort(message.occurred_at::String, 3) AS occurred_at,
    message.kind::String AS kind,
    message.region::String AS region,
    message.fire_id::String AS fire_id,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    message.detections::Nullable(UInt32) AS detections,
    message.frp_max::Nullable(Float64) AS frp_max,
    parseDateTime64BestEffortOrNull(message.first_seen::String, 3) AS first_seen,
    parseDateTime64BestEffortOrNull(message.last_seen::String, 3) AS last_seen,
    nullIf(message.merged_into::String, '') AS merged_into,
    message AS payload
FROM flechtwerk.wildfire_events_queue;

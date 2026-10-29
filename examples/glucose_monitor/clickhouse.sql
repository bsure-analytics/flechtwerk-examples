-- ClickHouse sink for the Glucose Monitor example.
--
-- Like ADS-B / SMARD / GTFS / Odds / Wildfire / TMDB, this takes the Kafka-engine SHORTCUT:
-- ClickHouse's own Kafka table engine consumes `glucose-readings` directly and a materialized view
-- lands the rows (no Flechtwerk sink stage). The stack configures the Kafka engine to read
-- COMMITTED (clickhouse/config/kafka.xml), so readings from an aborted transaction are never
-- ingested — required because `readings` is a transactional (EOS) producer, and the reason the
-- table can be an append-only MergeTree: the stage emits each reading exactly once.
--
-- SCHEMALESS INGEST (as ADS-B / SMARD / Odds / Wildfire / TMDB): each message is read whole into
-- one JSON column (`kafka_format = 'JSONAsObject'`); the view PROMOTES the columns the Grafana
-- board reads into typed columns and keeps the whole message in a `payload JSON` catch-all, so
-- LibreLinkUp's other fields (`Value` in the user's own unit, `GlucoseUnits`, `type`, a field
-- Abbott adds tomorrow) stay queryable as `payload.<field>` with no DDL change.
--
-- CASTS. `mg_dl` is `Float64`, never an integer type: LibreLinkUp sends whole numbers, and a
-- cast to an integer would silently truncate the day it doesn't. `trend_arrow` and
-- `utc_offset_min` are `Nullable`: a history point carries no trend arrow, and an absent
-- subcolumn cast to `Nullable(…)` is NULL — never a fabricated 0, which would read as "falling
-- fast" and as UTC respectively.
--
-- TWO KINDS IN ONE TABLE. `live` is the latest measurement at each poll (about one a minute from a
-- Libre 3, with a trend arrow); `history` is LibreLinkUp's coarser `graphData` series. They are
-- different series, so every query picks one: the dashboard's statistics read `history` — gap-free
-- across any ingest outage shorter than LibreLinkUp's 12-hour window, and on a regular grid, so
-- every point weighs the same — and the chart draws both.
--
-- NO TTL. LibreLinkUp forgets everything older than 12 hours; this table is the only long history
-- the pipeline makes, and the raw `glucose-raw` topic its only source. It is health data — keep
-- the stack local.

CREATE TABLE IF NOT EXISTS flechtwerk.glucose_readings_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'glucose-readings',
    kafka_group_name = 'glucose-readings-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.glucose_readings
(
    patient_id LowCardinality(String),
    kind LowCardinality(String),
    measured_at DateTime64(3, 'UTC'),
    utc_offset_min Nullable(Int16),
    mg_dl Float64,
    trend_arrow Nullable(UInt8),
    is_high Bool,
    is_low Bool,
    fetched_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (patient_id, kind, measured_at);

-- `__time` for FRET Studio (see the README): when the sensor measured.
ALTER TABLE flechtwerk.glucose_readings ADD COLUMN IF NOT EXISTS __time DateTime64(3, 'UTC') ALIAS measured_at;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.glucose_readings_mv
TO flechtwerk.glucose_readings
AS SELECT
    message.patient_id::String AS patient_id,
    message.kind::String AS kind,
    parseDateTime64BestEffort(message.measured_at::String, 3) AS measured_at,
    message.utc_offset_min::Nullable(Int16) AS utc_offset_min,
    message.ValueInMgPerDl::Float64 AS mg_dl,
    message.TrendArrow::Nullable(UInt8) AS trend_arrow,
    message.isHigh::Bool AS is_high,
    message.isLow::Bool AS is_low,
    parseDateTime64BestEffort(message.fetched_at::String, 3) AS fetched_at,
    message AS payload
FROM flechtwerk.glucose_readings_queue;

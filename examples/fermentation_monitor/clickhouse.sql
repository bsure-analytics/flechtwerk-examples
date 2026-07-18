-- ClickHouse sink for the fermentation monitor: the gravity curve and the alerts.
-- Both Kafka engines read committed (clickhouse/config/kafka.xml).

-- The reading carries the hydrometer payload verbatim (the bridge spreads it),
-- so angle and battery come through for free alongside gravity/temperature.
CREATE TABLE IF NOT EXISTS flechtwerk.fermentation_readings_queue
(
    batch String,
    gravity Float64,
    temperature Float64,
    angle Float64,
    battery Float64,
    at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'fermentation.readings',
    kafka_group_name = 'clickhouse-fermentation-readings',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.fermentation_readings
(
    batch LowCardinality(String),
    gravity Float64,
    temperature Float64,
    angle Float64,
    battery Float64,
    at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (batch, at);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.fermentation_readings_mv
TO flechtwerk.fermentation_readings
AS SELECT batch, gravity, temperature, angle, battery, parseDateTime64BestEffort(at, 3) AS at
FROM flechtwerk.fermentation_readings_queue;

CREATE TABLE IF NOT EXISTS flechtwerk.fermentation_alerts_queue
(
    batch String,
    kind String,
    gravity Float64,
    at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'fermentation.alerts',
    kafka_group_name = 'clickhouse-fermentation-alerts',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.fermentation_alerts
(
    batch LowCardinality(String),
    kind LowCardinality(String),
    gravity Float64,
    at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (batch, at);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.fermentation_alerts_mv
TO flechtwerk.fermentation_alerts
AS SELECT batch, kind, gravity, parseDateTime64BestEffort(at, 3) AS at
FROM flechtwerk.fermentation_alerts_queue;

-- ClickHouse sink for the chaos harness verifier.
--
-- The Kafka engine consumes chaos-output; the stack configures it to read
-- COMMITTED (clickhouse/config/kafka.xml), so aborted transactions from a
-- SIGKILLed transformer are never ingested. The verifier's one query over
-- chaos_output is therefore a true zero-duplicates / zero-gaps check.
CREATE TABLE IF NOT EXISTS flechtwerk.chaos_output_queue
(
    n Int64,
    seq Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'chaos-output',
    kafka_group_name = 'chaos-output-clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.chaos_output
(
    n Int64,
    seq Int64
)
ENGINE = MergeTree
ORDER BY n;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.chaos_output_mv
TO flechtwerk.chaos_output
AS SELECT n, seq FROM flechtwerk.chaos_output_queue;

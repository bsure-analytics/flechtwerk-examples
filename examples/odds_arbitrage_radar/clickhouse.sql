-- ClickHouse sink for the Odds Arbitrage Radar example.
--
-- Like ADS-B / SMARD / GTFS, this takes the Kafka-engine SHORTCUT: ClickHouse's own Kafka
-- table engine consumes the output streams directly and materialized views land the rows
-- (no Flechtwerk sink stage — that pattern is already taught by `clickhouse_sink` and the
-- GDELT sink). The stack configures the Kafka engine to read COMMITTED
-- (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are never
-- ingested — required because the upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / SMARD): each message is read whole into one JSON column
-- (`kafka_format = 'JSONAsObject'`); the materialized views PROMOTE the columns the Grafana
-- board reads into typed columns and keep the whole message in a `payload JSON` catch-all,
-- so a field we don't promote today is still queryable as `payload.<field>` with no DDL
-- change. Optional prices/sizes are `Nullable(Float64)`: an absent JSON subcolumn cast to
-- `::Nullable(Float64)` returns NULL, never a fabricated 0 (the SMARD rule) — a one-sided
-- book reads as NULL, not as a $0 ask.
--
-- THREE STREAMS. `odds_quotes` is the append-only quote history from both venues (per venue,
-- per poll) — the divergence view. `odds_margins` is the continuous "distance to free money"
-- per pair and direction, every poll. `odds_signals` is the sparse subset that was fresh and
-- net-positive after fees — an actual (paper) arb; kept without a TTL because it is rare and
-- precious. Quotes and margins expire after 30 days (the demo is a live board, not an
-- archive).

-- ================================= quotes (both venues) =================================

CREATE TABLE IF NOT EXISTS flechtwerk.odds_quotes_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'odds-quotes',
    kafka_group_name = 'odds-quotes-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.odds_quotes
(
    pair_key String,
    venue LowCardinality(String),
    title String,
    status LowCardinality(String),
    yes_bid Nullable(Float64),
    yes_ask Nullable(Float64),
    no_bid Nullable(Float64),
    no_ask Nullable(Float64),
    yes_ask_size Nullable(Float64),
    no_ask_size Nullable(Float64),
    fee_rate Float64,
    fetched_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (pair_key, venue, fetched_at)
TTL toDateTime(fetched_at) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.odds_quotes_mv
TO flechtwerk.odds_quotes
AS SELECT
    message.pair_key::String AS pair_key,
    message.venue::String AS venue,
    message.title::String AS title,
    message.status::String AS status,
    message.yes_bid::Nullable(Float64) AS yes_bid,
    message.yes_ask::Nullable(Float64) AS yes_ask,
    message.no_bid::Nullable(Float64) AS no_bid,
    message.no_ask::Nullable(Float64) AS no_ask,
    message.yes_ask_size::Nullable(Float64) AS yes_ask_size,
    message.no_ask_size::Nullable(Float64) AS no_ask_size,
    message.fee_rate::Float64 AS fee_rate,
    parseDateTime64BestEffort(message.fetched_at::String, 3) AS fetched_at,
    message AS payload
FROM flechtwerk.odds_quotes_queue;

-- ================================ margins (continuous) ================================

CREATE TABLE IF NOT EXISTS flechtwerk.odds_margins_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'odds-margins',
    kafka_group_name = 'odds-margins-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.odds_margins
(
    pair_key String,
    title String,
    direction LowCardinality(String),
    yes_ask Float64,
    no_ask Float64,
    gross_edge Float64,
    fees Float64,
    net_edge Float64,
    executable_size Nullable(Float64),
    fresh UInt8,
    computed_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (pair_key, direction, computed_at)
TTL toDateTime(computed_at) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.odds_margins_mv
TO flechtwerk.odds_margins
AS SELECT
    message.pair_key::String AS pair_key,
    message.title::String AS title,
    message.direction::String AS direction,
    message.yes_ask::Float64 AS yes_ask,
    message.no_ask::Float64 AS no_ask,
    message.gross_edge::Float64 AS gross_edge,
    message.fees::Float64 AS fees,
    message.net_edge::Float64 AS net_edge,
    message.executable_size::Nullable(Float64) AS executable_size,
    message.fresh::UInt8 AS fresh,
    parseDateTime64BestEffort(message.computed_at::String, 3) AS computed_at,
    message AS payload
FROM flechtwerk.odds_margins_queue;

-- ================================= signals (rare, kept) =================================

CREATE TABLE IF NOT EXISTS flechtwerk.odds_signals_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'odds-signals',
    kafka_group_name = 'odds-signals-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- Same shape as odds_margins, ordered by time (a signal feed is read newest-first across all
-- pairs) and WITHOUT a TTL — a real arb is the whole point; don't expire the evidence.
CREATE TABLE IF NOT EXISTS flechtwerk.odds_signals
(
    pair_key String,
    title String,
    direction LowCardinality(String),
    yes_ask Float64,
    no_ask Float64,
    gross_edge Float64,
    fees Float64,
    net_edge Float64,
    executable_size Nullable(Float64),
    fresh UInt8,
    computed_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (computed_at, pair_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.odds_signals_mv
TO flechtwerk.odds_signals
AS SELECT
    message.pair_key::String AS pair_key,
    message.title::String AS title,
    message.direction::String AS direction,
    message.yes_ask::Float64 AS yes_ask,
    message.no_ask::Float64 AS no_ask,
    message.gross_edge::Float64 AS gross_edge,
    message.fees::Float64 AS fees,
    message.net_edge::Float64 AS net_edge,
    message.executable_size::Nullable(Float64) AS executable_size,
    message.fresh::UInt8 AS fresh,
    parseDateTime64BestEffort(message.computed_at::String, 3) AS computed_at,
    message AS payload
FROM flechtwerk.odds_signals_queue;

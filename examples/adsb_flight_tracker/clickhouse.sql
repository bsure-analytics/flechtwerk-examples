-- ClickHouse sink for the ADS-B flight tracker (three-stage pipeline).
--
-- This example takes the SHORTCUT: ClickHouse's own Kafka table engine consumes
-- the enriched adsb.aircraft stream and the derived adsb.events stream directly,
-- and materialized views land the rows. Example 2 ("ClickHouse sink stage") does
-- the opposite — a Flechtwerk sink stage — precisely to contrast with this
-- shortcut and to make the at-least-once write semantics explicit.
--
-- The stack configures the Kafka engine to read COMMITTED
-- (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are
-- never ingested. What the engine still can't do -- and why the sink stage
-- exists -- is enrichment, routing, fan-out, or unit-testable projection logic;
-- the enrich stage (enrich.py) does all four before the data reaches here.
--
-- SCHEMALESS INGEST (Druid-style, but typed). Each Kafka message is read whole
-- into a single ClickHouse `JSON` column (`kafka_format = 'JSONAsObject'`), and the
-- materialized view PROMOTES only the fields the Grafana dashboards read into typed
-- columns, keeping the entire message in a `payload JSON` catch-all. Every other
-- field the feed sends (mlat, rssi, nav_*, squawk, track, seen, r, t, ... and
-- anything adsb.lol adds next month) is queryable as `payload.<field>` with NO DDL
-- change — this is the SQL-side mirror of the pipeline's "declare an attribute only
-- for what you compute with". Unlike Druid's degenerate-to-string, ClickHouse's JSON
-- type stores each path as its own columnar sub-column with its own inferred type.
--
-- Column names are adsb.lol's own wire names (flight, alt_baro, gs, t, r) — the
-- enrich stage spreads the feed through untouched. alt_baro is polymorphic on the
-- wire (a number OR the string "ground") and reaches here FAITHFULLY: the promoted
-- `alt_baro` column is a `Dynamic`, so it keeps the exact value — "ground" means "on
-- the surface" (at whatever field elevation), NOT 0 ft above sea level, and that
-- distinction is preserved rather than fabricated away. A panel that needs feet
-- coerces at query time (`toInt32OrNull(toString(alt_baro))`). The dashboards alias
-- the cryptic wire names at query time (`trimBoth(flight) AS callsign`).

-- === Enriched aircraft: adsb.aircraft -> current state per aircraft ===

-- The queue: one JSON message per row on adsb.aircraft, read whole. Nothing declared,
-- nothing dropped — the materialized view picks the fields it promotes.
CREATE TABLE IF NOT EXISTS flechtwerk.adsb_aircraft_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'adsb.aircraft',
    kafka_group_name = 'clickhouse-adsb',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- Current state per aircraft: latest row per ICAO, departures (is_deleted=1)
-- physically removed on merge. Only the engine keys (hex / polled_at / is_deleted)
-- and the columns the dashboards read are typed; everything else rides in `payload`.
-- Query it with FINAL for the live picture.
CREATE TABLE IF NOT EXISTS flechtwerk.adsb_aircraft
(
    hex String,
    polled_at DateTime64(3, 'UTC'),
    is_deleted UInt8,
    flight String,
    alt_baro Dynamic,
    gs Float64,
    lat Float64,
    lon Float64,
    emergency UInt8,
    vertical_rate Float64,
    aircraft_type_name String,
    type_wiki String,
    airline String,
    airline_wiki String,
    over_country LowCardinality(String),
    region LowCardinality(String),
    payload JSON
)
ENGINE = ReplacingMergeTree(polled_at, is_deleted)
ORDER BY hex;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.adsb_aircraft_mv
TO flechtwerk.adsb_aircraft
AS SELECT
    message.hex::String AS hex,
    parseDateTime64BestEffort(message.polled_at::String, 3) AS polled_at,
    message.is_deleted::UInt8 AS is_deleted,
    message.flight::String AS flight,
    -- alt_baro is feet as a number OR the string "ground" ("on the surface", NOT 0 ft
    -- MSL — a distinction that matters). `Dynamic` keeps the exact wire value; a panel
    -- that needs feet coerces at query time (toInt32OrNull(toString(alt_baro))).
    message.alt_baro AS alt_baro,
    message.gs::Float64 AS gs,
    message.lat::Float64 AS lat,
    message.lon::Float64 AS lon,
    message.emergency::UInt8 AS emergency,
    message.vertical_rate::Float64 AS vertical_rate,
    message.aircraft_type_name::String AS aircraft_type_name,
    message.type_wiki::String AS type_wiki,
    message.airline::String AS airline,
    message.airline_wiki::String AS airline_wiki,
    message.over_country::String AS over_country,
    message.region::String AS region,
    message AS payload
FROM flechtwerk.adsb_aircraft_queue;

-- === Derived events: adsb.events -> an append-only aviation-events log ===

-- Emergencies, rapid descents, going-dark, and near-miss conflicts. A log, not a
-- state table: the "stop viewing, start deriving" payoff that adsb.lol can't show.
CREATE TABLE IF NOT EXISTS flechtwerk.adsb_events_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'adsb.events',
    kafka_group_name = 'clickhouse-adsb-events',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- Same schemaless treatment: promote what the aviation-events dashboard reads;
-- an event's position (lat/lon/alt_baro) rides in `payload` until a panel needs it.
CREATE TABLE IF NOT EXISTS flechtwerk.adsb_events
(
    at DateTime64(3, 'UTC'),
    event_type LowCardinality(String),
    hex String,
    flight String,
    region LowCardinality(String),
    detail String,
    payload JSON
)
ENGINE = MergeTree
ORDER BY (at, hex);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.adsb_events_mv
TO flechtwerk.adsb_events
AS SELECT
    parseDateTime64BestEffort(message.at::String, 3) AS at,
    message.event_type::String AS event_type,
    message.hex::String AS hex,
    message.flight::String AS flight,
    message.region::String AS region,
    message.detail::String AS detail,
    message AS payload
FROM flechtwerk.adsb_events_queue;

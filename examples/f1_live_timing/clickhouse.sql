-- ClickHouse sink for the F1 Live Timing example.
--
-- Like ADS-B / SMARD / GTFS / Odds / Wildfire, this takes the Kafka-engine SHORTCUT: ClickHouse's
-- own Kafka table engine consumes the output streams directly and materialized views land the
-- rows (no Flechtwerk sink stage — that pattern is already taught by `clickhouse_sink` and the
-- GDELT sink). The stack configures the Kafka engine to read COMMITTED
-- (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are never ingested —
-- required, because the upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / SMARD / Odds / Wildfire): each message is read whole into one JSON
-- column (`kafka_format = 'JSONAsObject'`); the materialized views PROMOTE the columns the
-- dashboards read into typed columns and keep the whole message in a `payload JSON` catch-all, so
-- a field not promoted today (`line`, `reference`, `speed_i2`, …) is still queryable as
-- `payload.<field>` with no DDL change. Optional measurements are `Nullable`: an absent JSON
-- subcolumn cast to `::Nullable(T)` returns NULL, never a fabricated 0 — which matters everywhere
-- here, because a gap of 0.0 means "level with the leader" and a lap time of 0 means nothing at all.
--
-- THREE QUEUES, NOT FOUR. `f1-timing` — the tape itself — is deliberately NOT sunk. It is Kafka-
-- durable with unlimited retention, it is the *input* to the board rather than a result, and it is
-- rebuildable into ClickHouse at any time by re-running the timing stage under a fresh
-- application id. Sinking it would double the storage of the example's largest stream to store
-- somebody else's undocumented JSON.
--
-- MIXED-KIND TOPICS, FILTERED VIEWS (the SMARD idiom): each queue carries several record shapes,
-- so every view selects its own with `WHERE message.kind::String = '…'` and a subcolumn missing on
-- the other kinds never throws. `f1-events` alone feeds eight tables this way.
--
-- WHY `f1_heartbeats` EXISTS. The same reason `wildfire_sweeps` does: a freshness panel built on
-- standings alone cannot tell "the session is under a red flag and nothing is moving" from "the
-- ingest stage died". The tape's Heartbeat feed beats every ~15 s regardless of what is happening
-- on track, so a tiny table of those beats can.
--
-- NO TTLs ANYWHERE. The season is the product: an ingested race is meant to stay replayable, the
-- Kafka topics are retained forever for the same reason (see setup.py), and `poe clean` is the
-- reset. This is the one example whose value grows with age.

-- ============================ status: the continuous streams ============================

-- The queue: one JSON message per row on f1-status (standings, weather, clock, heartbeat).
CREATE TABLE IF NOT EXISTS flechtwerk.f1_status_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'f1-status',
    kafka_group_name = 'f1-status-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === The leaderboard, as a history of snapshots — THE table of this example ===
-- Partitioned by session because that is the unit everything is queried by and dropped by, and
-- ordered so the one query that matters is a prefix scan: "per driver, the latest row at or before
-- the cursor" (argMax over event_time, grouped by racing_number, filtered to one session).
CREATE TABLE IF NOT EXISTS flechtwerk.f1_standings
(
    session_key UInt32,
    session String,
    racing_number LowCardinality(String),
    tla LowCardinality(String),
    team LowCardinality(String),
    position Nullable(UInt8),
    gap_raw String,
    gap_s Nullable(Float64),
    gap_laps Nullable(UInt16),
    interval_raw String,
    interval_s Nullable(Float64),
    interval_laps Nullable(UInt16),
    catching Nullable(Bool),
    last_lap_ms Nullable(UInt32),
    last_lap_personal_best Nullable(Bool),
    last_lap_overall_best Nullable(Bool),
    best_lap_ms Nullable(UInt32),
    sector1_ms Nullable(UInt32),
    sector2_ms Nullable(UInt32),
    sector3_ms Nullable(UInt32),
    speed_i1 Nullable(UInt16),
    speed_i2 Nullable(UInt16),
    speed_fl Nullable(UInt16),
    speed_st Nullable(UInt16),
    in_pit Nullable(Bool),
    pit_out Nullable(Bool),
    retired Nullable(Bool),
    stopped Nullable(Bool),
    pit_count Nullable(UInt8),
    tyre_compound LowCardinality(String),
    tyre_age Nullable(UInt16),
    stint Nullable(UInt8),
    laps_completed Nullable(UInt16),
    track_status LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
PARTITION BY session_key
ORDER BY (session_key, racing_number, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_standings_mv
TO flechtwerk.f1_standings
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.session::String AS session,
    message.racing_number::String AS racing_number,
    message.tla::String AS tla,
    message.team::String AS team,
    message.position::Nullable(UInt8) AS position,
    message.gap_raw::String AS gap_raw,
    message.gap_s::Nullable(Float64) AS gap_s,
    message.gap_laps::Nullable(UInt16) AS gap_laps,
    message.interval_raw::String AS interval_raw,
    message.interval_s::Nullable(Float64) AS interval_s,
    message.interval_laps::Nullable(UInt16) AS interval_laps,
    message.catching::Nullable(Bool) AS catching,
    message.last_lap_ms::Nullable(UInt32) AS last_lap_ms,
    message.last_lap_personal_best::Nullable(Bool) AS last_lap_personal_best,
    message.last_lap_overall_best::Nullable(Bool) AS last_lap_overall_best,
    message.best_lap_ms::Nullable(UInt32) AS best_lap_ms,
    message.sector1_ms::Nullable(UInt32) AS sector1_ms,
    message.sector2_ms::Nullable(UInt32) AS sector2_ms,
    message.sector3_ms::Nullable(UInt32) AS sector3_ms,
    message.speed_i1::Nullable(UInt16) AS speed_i1,
    message.speed_i2::Nullable(UInt16) AS speed_i2,
    message.speed_fl::Nullable(UInt16) AS speed_fl,
    message.speed_st::Nullable(UInt16) AS speed_st,
    message.in_pit::Nullable(Bool) AS in_pit,
    message.pit_out::Nullable(Bool) AS pit_out,
    message.retired::Nullable(Bool) AS retired,
    message.stopped::Nullable(Bool) AS stopped,
    message.pit_count::Nullable(UInt8) AS pit_count,
    message.tyre_compound::String AS tyre_compound,
    message.tyre_age::Nullable(UInt16) AS tyre_age,
    message.stint::Nullable(UInt8) AS stint,
    message.laps_completed::Nullable(UInt16) AS laps_completed,
    message.track_status::String AS track_status,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time,
    message AS payload
FROM flechtwerk.f1_status_queue
WHERE message.kind::String = 'standings';

-- === Weather: ~one row per minute per session ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_weather
(
    session_key UInt32,
    air_temp Nullable(Float64),
    track_temp Nullable(Float64),
    humidity Nullable(Float64),
    pressure Nullable(Float64),
    rainfall Nullable(Float64),
    wind_speed Nullable(Float64),
    wind_direction Nullable(Float64),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_weather_mv
TO flechtwerk.f1_weather
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.air_temp::Nullable(Float64) AS air_temp,
    message.track_temp::Nullable(Float64) AS track_temp,
    message.humidity::Nullable(Float64) AS humidity,
    message.pressure::Nullable(Float64) AS pressure,
    message.rainfall::Nullable(Float64) AS rainfall,
    message.wind_speed::Nullable(Float64) AS wind_speed,
    message.wind_direction::Nullable(Float64) AS wind_direction,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_status_queue
WHERE message.kind::String = 'weather';

-- === The lap counter and the session clock — what the wall's "LAP 42/70" panel reads ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_clock
(
    session_key UInt32,
    lap Nullable(UInt16),
    total_laps Nullable(UInt16),
    remaining_s Nullable(Float64),
    extrapolating Nullable(Bool),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_clock_mv
TO flechtwerk.f1_clock
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.lap::Nullable(UInt16) AS lap,
    message.total_laps::Nullable(UInt16) AS total_laps,
    message.remaining_s::Nullable(Float64) AS remaining_s,
    message.extrapolating::Nullable(Bool) AS extrapolating,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_status_queue
WHERE message.kind::String = 'clock';

-- === Tape freshness: the ~15 s beat that separates "quiet session" from "dead ingest" ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_heartbeats
(
    session_key UInt32,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_heartbeats_mv
TO flechtwerk.f1_heartbeats
AS SELECT
    message.session_key::UInt32 AS session_key,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_status_queue
WHERE message.kind::String = 'heartbeat';

-- ====================== events: the sparse streams and the dimensions ======================

CREATE TABLE IF NOT EXISTS flechtwerk.f1_events_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'f1-events',
    kafka_group_name = 'f1-events-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === The session dimension: the dashboards' picker, and their replay links ===
-- ReplacingMergeTree versioned by event_time, because a session is re-upserted on every status
-- change (Inactive → Started → Finished → Finalised → Ends) and only the latest matters. Query it
-- FINAL. `start_utc` / `end_utc` are already real instants — the board did the timezone
-- arithmetic once, so no panel ever has to.
CREATE TABLE IF NOT EXISTS flechtwerk.f1_sessions
(
    session_key UInt32,
    session String,
    year UInt16,
    meeting String,
    session_name LowCardinality(String),
    session_type LowCardinality(String),
    label String,
    circuit String,
    country LowCardinality(String),
    location String,
    start_local String,
    gmt_offset String,
    start_utc Nullable(DateTime64(3, 'UTC')),
    end_utc Nullable(DateTime64(3, 'UTC')),
    status LowCardinality(String),
    event_time DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY session_key;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_sessions_mv
TO flechtwerk.f1_sessions
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.session::String AS session,
    message.year::UInt16 AS year,
    message.meeting::String AS meeting,
    message.session_name::String AS session_name,
    message.session_type::String AS session_type,
    message.label::String AS label,
    message.circuit::String AS circuit,
    message.country::String AS country,
    message.location::String AS location,
    message.start_local::String AS start_local,
    message.gmt_offset::String AS gmt_offset,
    parseDateTime64BestEffortOrNull(message.start_utc::String, 3) AS start_utc,
    parseDateTime64BestEffortOrNull(message.end_utc::String, 3) AS end_utc,
    message.status::String AS status,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'session';

-- === The driver dimension, per session (numbers and teams change between seasons) ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_drivers
(
    session_key UInt32,
    racing_number LowCardinality(String),
    tla LowCardinality(String),
    full_name String,
    first_name String,
    last_name String,
    team LowCardinality(String),
    team_colour String,
    reference String,
    line Nullable(UInt8),
    event_time DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY (session_key, racing_number);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_drivers_mv
TO flechtwerk.f1_drivers
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.tla::String AS tla,
    message.full_name::String AS full_name,
    message.first_name::String AS first_name,
    message.last_name::String AS last_name,
    message.team::String AS team,
    message.team_colour::String AS team_colour,
    message.reference::String AS reference,
    message.line::Nullable(UInt8) AS line,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'driver';

-- === Completed laps: the broadcast-join output, and the strategy view's whole basis ===
-- `track_status` is the WORST flag state seen during the lap and `clean` is the verdict that
-- makes a pace comparison legitimate: the SCD join, already resolved into a column.
CREATE TABLE IF NOT EXISTS flechtwerk.f1_laps
(
    session_key UInt32,
    racing_number LowCardinality(String),
    tla LowCardinality(String),
    lap UInt16,
    lap_ms Nullable(UInt32),
    sector1_ms Nullable(UInt32),
    sector2_ms Nullable(UInt32),
    sector3_ms Nullable(UInt32),
    position Nullable(UInt8),
    tyre_compound LowCardinality(String),
    tyre_age Nullable(UInt16),
    stint Nullable(UInt8),
    speed_st Nullable(UInt16),
    pitted Nullable(Bool),
    track_status LowCardinality(String),
    clean Nullable(Bool),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, racing_number, lap);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_laps_mv
TO flechtwerk.f1_laps
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.tla::String AS tla,
    message.lap::UInt16 AS lap,
    message.lap_ms::Nullable(UInt32) AS lap_ms,
    message.sector1_ms::Nullable(UInt32) AS sector1_ms,
    message.sector2_ms::Nullable(UInt32) AS sector2_ms,
    message.sector3_ms::Nullable(UInt32) AS sector3_ms,
    message.position::Nullable(UInt8) AS position,
    message.tyre_compound::String AS tyre_compound,
    message.tyre_age::Nullable(UInt16) AS tyre_age,
    message.stint::Nullable(UInt8) AS stint,
    message.speed_st::Nullable(UInt16) AS speed_st,
    message.pitted::Nullable(Bool) AS pitted,
    message.track_status::String AS track_status,
    message.clean::Nullable(Bool) AS clean,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'lap';

-- === Pit stops: stationary time, and the ~20 s of pit lane that dwarfs it ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_pit_stops
(
    session_key UInt32,
    racing_number LowCardinality(String),
    lap Nullable(UInt16),
    stationary_s Nullable(Float64),
    pit_lane_s Nullable(Float64),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, racing_number, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_pit_stops_mv
TO flechtwerk.f1_pit_stops
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.lap::Nullable(UInt16) AS lap,
    message.stationary_s::Nullable(Float64) AS stationary_s,
    message.pit_lane_s::Nullable(Float64) AS pit_lane_s,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'pit';

-- === Flag periods: the annotation layer every time-series panel draws ===
-- A period is emitted TWICE — once when it opens (`ended_at` NULL, so an annotation appears the
-- moment a flag flies) and once when it closes. ReplacingMergeTree keyed on the period's start and
-- versioned by event_time, so the closed row supersedes the open one. Query it FINAL.
CREATE TABLE IF NOT EXISTS flechtwerk.f1_track_status
(
    session_key UInt32,
    code LowCardinality(String),
    label LowCardinality(String),
    severity UInt8,
    started_at DateTime64(3, 'UTC'),
    ended_at Nullable(DateTime64(3, 'UTC')),
    event_time DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY (session_key, started_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_track_status_mv
TO flechtwerk.f1_track_status
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.code::String AS code,
    message.label::String AS label,
    message.severity::UInt8 AS severity,
    parseDateTime64BestEffort(message.started_at::String, 3) AS started_at,
    parseDateTime64BestEffortOrNull(message.ended_at::String, 3) AS ended_at,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'track_period';

-- === Race control: the ticker (flags, penalties, DRS, safety cars) ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_race_control
(
    session_key UInt32,
    utc Nullable(DateTime64(3, 'UTC')),
    category LowCardinality(String),
    flag LowCardinality(String),
    scope LowCardinality(String),
    sector Nullable(UInt8),
    lap Nullable(UInt16),
    racing_number LowCardinality(String),
    message_text String,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_race_control_mv
TO flechtwerk.f1_race_control
AS SELECT
    message.session_key::UInt32 AS session_key,
    parseDateTime64BestEffortOrNull(message.utc::String, 3) AS utc,
    message.category::String AS category,
    message.flag::String AS flag,
    message.scope::String AS scope,
    message.sector::Nullable(UInt8) AS sector,
    message.lap::Nullable(UInt16) AS lap,
    message.racing_number::String AS racing_number,
    message.message::String AS message_text,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'race_control';

-- === Overtakes: carried verbatim for ad-hoc SQL; the feed's `count` semantics are its own ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_overtakes
(
    session_key UInt32,
    racing_number LowCardinality(String),
    overtakes Nullable(UInt16),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_overtakes_mv
TO flechtwerk.f1_overtakes
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.overtakes::Nullable(UInt16) AS overtakes,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'overtake';

-- === Championship standings and the feed's own live prediction, drivers and teams in one table ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_championship
(
    session_key UInt32,
    entity_type LowCardinality(String),
    entity_id String,
    position Nullable(UInt8),
    points Nullable(Float64),
    predicted_position Nullable(UInt8),
    predicted_points Nullable(Float64),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (session_key, entity_type, entity_id, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_championship_mv
TO flechtwerk.f1_championship
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.entity_type::String AS entity_type,
    message.entity_id::String AS entity_id,
    message.position::Nullable(UInt8) AS position,
    message.points::Nullable(Float64) AS points,
    message.predicted_position::Nullable(UInt8) AS predicted_position,
    message.predicted_points::Nullable(Float64) AS predicted_points,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_events_queue
WHERE message.kind::String = 'championship';

-- ============================= telemetry: the two firehoses =============================

CREATE TABLE IF NOT EXISTS flechtwerk.f1_telemetry_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'f1-telemetry',
    kafka_group_name = 'f1-telemetry-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- === Car telemetry, ~4–5 Hz × 22 cars. Throttle and brake are on the UPSTREAM's 0–104 scale ===
-- and stored verbatim: the scale is undocumented and only looks like a percentage (104 occurs more
-- often than 100 at full), so rescaling would either produce 104 % or silently reinterpret history.
CREATE TABLE IF NOT EXISTS flechtwerk.f1_car_telemetry
(
    session_key UInt32,
    racing_number LowCardinality(String),
    rpm Nullable(UInt16),
    speed Nullable(UInt16),
    gear Nullable(UInt8),
    throttle Nullable(UInt8),
    brake Nullable(UInt8),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY session_key
ORDER BY (session_key, racing_number, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_car_telemetry_mv
TO flechtwerk.f1_car_telemetry
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.rpm::Nullable(UInt16) AS rpm,
    message.speed::Nullable(UInt16) AS speed,
    message.gear::Nullable(UInt8) AS gear,
    message.throttle::Nullable(UInt8) AS throttle,
    message.brake::Nullable(UInt8) AS brake,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_telemetry_queue
WHERE message.kind::String = 'car';

-- === Track positions: X/Y/Z in the upstream's own frame, Int32 because X and Y go negative ===
CREATE TABLE IF NOT EXISTS flechtwerk.f1_positions
(
    session_key UInt32,
    racing_number LowCardinality(String),
    x Nullable(Int32),
    y Nullable(Int32),
    z Nullable(Int32),
    status LowCardinality(String),
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY session_key
ORDER BY (session_key, racing_number, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.f1_positions_mv
TO flechtwerk.f1_positions
AS SELECT
    message.session_key::UInt32 AS session_key,
    message.racing_number::String AS racing_number,
    message.x::Nullable(Int32) AS x,
    message.y::Nullable(Int32) AS y,
    message.z::Nullable(Int32) AS z,
    message.status::String AS status,
    parseDateTime64BestEffort(message.event_time::String, 3) AS event_time
FROM flechtwerk.f1_telemetry_queue
WHERE message.kind::String = 'pos';

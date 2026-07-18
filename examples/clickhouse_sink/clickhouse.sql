-- Target table for the ClickHouse sink stage: a positions history.
--
-- non_replicated_deduplication_window is the crucial setting: on a plain
-- (non-replicated) MergeTree, insert_deduplication_token is IGNORED unless this
-- window is > 0. With it, ClickHouse remembers the last N insert tokens per
-- partition and drops a re-insert carrying a token it has already seen — which
-- is exactly what makes the sink's at-least-once writes idempotent.
CREATE TABLE IF NOT EXISTS flechtwerk.adsb_positions
(
    hex String,
    callsign String,
    altitude Nullable(Int32),
    ground_speed Nullable(Float64),
    lat Float64,
    lon Float64,
    region LowCardinality(String),
    polled_at DateTime64(3, 'UTC'),
    source_partition Int32,
    source_offset UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (hex, polled_at)
SETTINGS non_replicated_deduplication_window = 1000;

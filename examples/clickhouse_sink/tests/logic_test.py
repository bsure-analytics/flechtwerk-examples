"""Tier 1 — pure logic. No framework, no ClickHouse.

A sink has little logic, and that is the point: `to_rows` (projection, with
tombstones skipped) and `dedup_token` (a reprocessing-stable identity) are plain
functions, so they are tested by calling them.
"""
import json

from flechtwerk.kafka import parse_message
from flechtwerk.testing import make_record

from examples.clickhouse_sink.sink import INPUT_TOPIC, dedup_token, to_rows

POSITION = {
    "hex": "abc123", "flight": "BAW123", "alt_baro": 30000, "gs": 420.0,
    "lat": 51.5, "lon": -0.4, "region": "london", "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 0,
}
TOMBSTONE = {"hex": "gone99", "region": "london", "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 1}


def _msg(payload: dict, *, partition: int = 0, offset: int = 0):
    return parse_message(make_record(
        topic=INPUT_TOPIC, partition=partition, offset=offset, value=json.dumps(payload)))


def test_position_projects_to_a_row_with_provenance() -> None:
    rows = to_rows(_msg(POSITION, partition=3, offset=7))

    assert len(rows) == 1
    row = rows[0]
    assert row["hex"] == "abc123"
    assert row["callsign"] == "BAW123"
    assert row["altitude"] == 30000
    assert row["region"] == "london"
    # Provenance = the full Kafka coordinate. Offsets repeat across partitions, so
    # partition:offset (not offset alone) is what uniquely identifies a record.
    assert row["source_partition"] == 3
    assert row["source_offset"] == 7


def test_departure_tombstone_produces_no_row() -> None:
    assert to_rows(_msg(TOMBSTONE)) == []


def test_identity_only_event_without_a_position_produces_no_row() -> None:
    # A Mode-S aircraft that broadcast identity but no lat/lon must not become a
    # fabricated (0, 0) fix in a positions history — the row is skipped entirely.
    identity_only = {"hex": "abc123", "region": "london",
                     "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 0}
    assert to_rows(_msg(identity_only)) == []


def test_ground_altitude_lands_as_null_not_a_fabricated_zero() -> None:
    # Example 1 forwards alt_baro faithfully — a surface aircraft carries the string
    # "ground", which is NOT 0 ft MSL. This sink's numeric column has no faithful value
    # to store, so the key is omitted (→ NULL), never a fabricated 0.
    on_ground = {**POSITION, "alt_baro": "ground"}
    assert "altitude" not in to_rows(_msg(on_ground))[0]


def test_omitted_telemetry_is_left_out_of_the_row_not_zeroed() -> None:
    # No altitude / ground_speed in the event → those keys are absent from the row
    # (they land as NULL in the Nullable columns), never a fabricated 0 / 0.0.
    positioned = {"hex": "abc123", "region": "london", "lat": 51.5, "lon": -0.4,
                  "polled_at": "2026-07-17T12:00:00Z", "is_deleted": 0}
    row = to_rows(_msg(positioned))[0]
    assert "altitude" not in row
    assert "ground_speed" not in row
    assert row["lat"] == 51.5 and row["lon"] == -0.4


def test_dedup_token_is_the_reprocessing_stable_identity() -> None:
    record = _msg(POSITION, partition=2, offset=42)

    # Same record reprocessed => same token => ClickHouse drops the re-insert.
    assert dedup_token(record) == "adsb.aircraft:2:42"
    assert dedup_token(_msg(POSITION, partition=2, offset=42)) == dedup_token(record)

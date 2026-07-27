"""Tier 1 — pure logic. No framework machinery, no mocks, no network.

Drives the two pure cores directly: :mod:`~examples.f1_live_timing.tape` (framing, inflation,
anchoring, the watermark merge) and :mod:`~examples.f1_live_timing.board` (patch merging, gap and
duration parsing, lap detection, the flag SCD), plus the ``run_board`` fold driven as a bare async
generator over hand-built ``State``/``IncomingMessage`` — the SMARD ``run_mix`` / wildfire
``run_tracker`` style.

The fixture is a **miniature session built from real archive lines** (see
``fixtures/PROVENANCE.md``): every shape is verbatim, only the values are trimmed. So the framing,
inflation, and anchoring assertions are pinned to bytes the CDN actually served, and the awkward
cases the live tape only offers once per race — a lap run entirely under a virtual safety car, a
feed's collection switching from array to index-keyed dict mid-stream, an unterminated final line —
are all present in 12 KB.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from flechtwerk import Event, IncomingMessage, Message, State

from examples.f1_live_timing import board, tape
from examples.f1_live_timing.attributes import (
    CLOCK,
    D_COMPOUND,
    D_IN_PIT,
    D_LAP_PITTED,
    D_LAP_WORST,
    D_LAPS,
    D_POSITION,
    D_SECTORS,
    D_STINT,
    D_TYRE_AGE,
    DRIVERS,
    ENDED_AT,
    EVENT_TIME,
    EVENTS_TOPIC,
    FINALISED,
    FEED,
    LABEL,
    LAP,
    M_KEY,
    M_LABEL,
    M_START_UTC,
    META,
    OFFSET_MS,
    PAYLOAD,
    SECTOR3_MS,
    SESSION,
    STATUS_TOPIC,
    T_LABEL,
    T_SEVERITY,
    TELEMETRY_TOPIC,
    TIMING_TOPIC,
    TRACK,
)
from examples.f1_live_timing.ingest import TELEMETRY_FEEDS, WISH_LIST
from examples.f1_live_timing.timing import (
    CAR,
    CLOCK_KIND,
    DRIVER,
    HEARTBEAT,
    LAP_KIND,
    PIT,
    POS,
    RACE_CONTROL,
    SESSION_KIND,
    STANDINGS,
    TRACK_PERIOD,
    run_board,
)

FIXTURES = Path(__file__).parent / "fixtures" / "session"
SESSION_PATH = "2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/"

UTC = timezone.utc
# The real anchor of the real 2026 Hungarian GP race tape: the first Heartbeat line sits at
# offset 00:00:13.844 and states 12:09:26.3114877Z, so the recording began 13.844 s earlier.
T0 = datetime(2026, 7, 26, 12, 9, 12, 467000, tzinfo=UTC)
INGESTED_FEEDS = set(WISH_LIST) | set(TELEMETRY_FEEDS)


def raw(feed: str) -> bytes:
    return (FIXTURES / f"{feed}.jsonStream").read_bytes()


def fetch(feed: str, **kwargs) -> tape.Fetch:
    """The whole fixture file for one feed, framed as a finished archive read."""
    return tape.frame(raw(feed), feed=feed, start=0, final=True, **kwargs)


def all_fetches() -> list[tape.Fetch]:
    """Every *ingested* feed of the fixture session, framed whole."""
    return [fetch(path.name.removesuffix(".jsonStream"))
            for path in sorted(FIXTURES.glob("*.jsonStream"))
            if path.name.removesuffix(".jsonStream") in INGESTED_FEEDS]


# --- framing ---

def test_every_fixture_file_starts_with_a_bom_like_the_real_archive() -> None:
    for path in FIXTURES.glob("*.json*"):
        assert path.read_bytes().startswith(tape.BOM), path.name


def test_frame_strips_the_bom_only_at_offset_zero_and_counts_its_bytes() -> None:
    data = raw("TrackStatus")
    first = tape.frame(data, feed="TrackStatus", start=0)
    assert first.lines[0].offset_ms == 0
    # The BOM's three bytes are consumed as part of the first line's span, so `end` lands on
    # the byte after its CRLF — the offset a resuming Range read must ask for.
    assert first.lines[0].end == len(data.split(tape.SEPARATOR)[0]) + len(tape.SEPARATOR)
    assert data[first.lines[0].end:].startswith(b"00:02:00.000")

    # A mid-file chunk must NOT strip anything: the same bytes read from a non-zero offset are
    # ordinary content, and stripping there would shift every later cursor by three.
    resumed = tape.frame(data[first.lines[0].end:], feed="TrackStatus",
                         start=first.lines[0].end, final=True)
    assert resumed.lines[0].offset_ms == tape.parse_offset("00:02:00.000")
    assert resumed.tail == len(data)


def test_parse_offset_reads_the_fixed_twelve_character_prefix() -> None:
    assert tape.parse_offset("00:00:00.000") == 0
    assert tape.parse_offset("01:23:45.678") == 5025678
    assert tape.parse_offset("02:37:08.181") == 9428181      # a real end-of-race offset
    assert tape.parse_offset("99:59:59.999") == 359999999    # hours never overflow in practice
    for bad in ("0:00:00.000", "00:00:00.00", "00:00:00,000", "", "{\"Status\":\"1\"}"):
        assert tape.parse_offset(bad) is None


def test_frame_leaves_a_partial_trailing_line_for_the_next_poll() -> None:
    data = raw("TimingData")
    split = data.index(tape.SEPARATOR) + 2 + 40  # 40 bytes into the second line
    first = tape.frame(data[:split], feed="TimingData", start=0)
    assert len(first.lines) == 1
    assert first.tail == first.lines[0].end          # the cursor never lands mid-line
    assert not first.at_eof

    rest = tape.frame(data[first.tail:], feed="TimingData", start=first.tail, final=True)
    assert rest.lines[0].offset_ms == tape.parse_offset("00:01:20.500")
    assert rest.tail == len(data)
    # Nothing was lost or duplicated across the split.
    assert 1 + len(rest.lines) == len(fetch("TimingData").lines)


def test_frame_treats_an_unterminated_final_line_as_content_only_when_final() -> None:
    data = raw("WeatherData")
    assert not data.endswith(tape.SEPARATOR)         # the fixture's one unterminated tail

    finished = tape.frame(data, feed="WeatherData", start=0, final=True)
    assert len(finished.lines) == 1 and finished.tail == len(data) and finished.at_eof

    # While live the same bytes are a fragment: the file may still grow a CRLF and more JSON.
    growing = tape.frame(data, feed="WeatherData", start=0, at_eof=True)
    assert growing.lines == [] and growing.tail == len(tape.BOM) and growing.at_eof


def test_frame_skips_a_malformed_line_but_still_consumes_its_bytes() -> None:
    # A bad prefix, unparseable JSON, and a blank line, wrapped in two good lines.
    body = (b"\xef\xbb\xbf"
            b'00:00:01.000{"Status":"1"}\r\n'
            b'nonsense-pfx{"Status":"2"}\r\n'
            b'00:00:03.000{"Status":\r\n'
            b"\r\n"
            b'00:00:05.000{"Status":"6"}\r\n')
    framed = tape.frame(body, feed="TrackStatus", start=0, final=True)
    assert [line.offset_ms for line in framed.lines] == [1000, 5000]
    # `tail` reaches the end of the file, so the feed can still complete — the whole point:
    # a deterministic poison pill must not stall the season.
    assert framed.tail == len(body)


def test_frame_skips_an_oversized_line() -> None:
    body = b"\xef\xbb\xbf" + b'00:00:01.000{"x":"' + b"y" * tape.MAX_LINE_BYTES + b'"}\r\n'
    framed = tape.frame(body, feed="TimingData", start=0, final=True)
    assert framed.lines == [] and framed.tail == len(body)


def test_frame_counts_bytes_not_characters() -> None:
    # Non-ASCII in a payload (real: circuit and driver names) must not shift the cursor.
    body = ('﻿00:00:01.000{"Message":"Ζάντβoort — Sûrté"}\r\n'
            '00:00:02.000{"Message":"ok"}\r\n').encode()
    framed = tape.frame(body, feed="RaceControlMessages", start=0, final=True)
    assert [line.offset_ms for line in framed.lines] == [1000, 2000]
    assert framed.tail == len(body)                  # bytes, not len(str)
    assert framed.lines[0].end < framed.lines[1].end


# --- .z inflation ---

def test_inflate_decodes_base64_raw_deflate() -> None:
    line, = fetch("CarData.z").lines
    assert line.payload["Entries"][0]["Cars"]["1"]["Channels"] == {
        "0": 10994, "2": 148, "3": 3, "4": 104, "5": 104}
    # The channel set really is these five — 45 (DRS) does not exist in 2026 data.
    assert set(board.CAR_CHANNELS) == {"0", "2", "3", "4", "5"}


def test_inflate_rejects_a_zlib_wrapped_stream() -> None:
    import zlib
    with pytest.raises(zlib.error):
        tape.inflate(__import__("base64").b64encode(zlib.compress(b"{}")).decode())


def test_a_z_feed_line_that_is_not_a_string_is_skipped() -> None:
    body = b'\xef\xbb\xbf00:00:01.000{"Entries":[]}\r\n'
    assert tape.frame(body, feed="CarData.z", start=0, final=True).lines == []


# --- the t0 anchor ---

def test_anchor_is_taken_from_the_first_heartbeat_and_agrees_with_the_real_tape() -> None:
    t0_ms = tape.anchor(fetch("Heartbeat").lines)
    assert t0_ms is not None
    assert tape.event_time(t0_ms, 0) == T0
    # Round trip: the anchor line's own offset reproduces its own inner Utc, to the millisecond
    # the anchor is stored at — the feed's seven fractional digits do not survive, by design.
    first = fetch("Heartbeat").lines[0]
    assert tape.event_time(t0_ms, first.offset_ms) == datetime(
        2026, 7, 26, 12, 9, 26, 311000, tzinfo=UTC)


def test_anchor_warns_when_the_first_estimates_disagree(caplog) -> None:
    # Real spread over the first five beats of a race is 0.18 s; this one is 5 s out.
    lines = [
        tape.Line(feed="Heartbeat", offset_ms=0, end=1, payload={"Utc": "2026-07-26T12:00:00Z"}),
        tape.Line(feed="Heartbeat", offset_ms=1000, end=2, payload={"Utc": "2026-07-26T12:00:06Z"}),
    ]
    with caplog.at_level("WARNING"):
        assert tape.anchor(lines) == int(datetime(2026, 7, 26, 12, tzinfo=UTC).timestamp() * 1000)
    assert "anchor estimates" in caplog.text


def test_anchor_looks_only_at_the_start_of_the_tape() -> None:
    # At the end of a recording the offsets stop advancing while the heartbeats keep ticking, so
    # a late beat implies a t0 twenty minutes out. Only ANCHOR_SAMPLE lines are consulted.
    lines = [tape.Line(feed="Heartbeat", offset_ms=0, end=1,
                       payload={"Utc": "2026-07-26T12:00:00Z"})]
    lines += [tape.Line(feed="Heartbeat", offset_ms=9_000_000, end=2 + n,
                        payload={"Utc": f"2026-07-26T14:{30 + n}:00Z"})
              for n in range(10)]
    assert tape.anchor(lines) == int(datetime(2026, 7, 26, 12, tzinfo=UTC).timestamp() * 1000)


def test_anchor_falls_back_to_other_feeds_inner_clocks() -> None:
    for feed in ("ExtrapolatedClock",        # {"Utc": …}
                 "RaceControlMessages",      # {"Messages": [{"Utc": …}]} — array or index-dict
                 "CarData.z",                # {"Entries": [{"Utc": …}]}
                 "Position.z"):              # {"Position": [{"Timestamp": …}]}
        t0_ms = tape.anchor(fetch(feed).lines)
        assert t0_ms is not None, feed
        # Each feed is internally consistent; absolute agreement between feeds is only within
        # the broadcast pipeline's few seconds, which is exactly what the module documents.
        assert abs(tape.event_time(t0_ms, 0) - T0) < timedelta(seconds=20), feed


def test_anchor_returns_none_when_no_line_carries_an_instant() -> None:
    assert tape.anchor(fetch("TrackStatus").lines) is None
    assert tape.anchor([]) is None


def test_parse_utc_normalizes_every_shape_the_archive_uses() -> None:
    assert tape.parse_utc("2026-07-26T12:09:26.3114877Z") == datetime(
        2026, 7, 26, 12, 9, 26, 311487, tzinfo=UTC)          # seven fractional digits
    assert tape.parse_utc("2026-07-26T13:22:21.62Z") == datetime(
        2026, 7, 26, 13, 22, 21, 620000, tzinfo=UTC)         # two
    assert tape.parse_utc("2026-07-26T12:20:00") == datetime(
        2026, 7, 26, 12, 20, tzinfo=UTC)                     # race control: no zone, means UTC
    assert tape.parse_utc("not a time") is None


def test_session_offset_ms_inverts_event_time() -> None:
    t0_ms = int(T0.timestamp() * 1000)
    assert tape.session_offset_ms(t0_ms, tape.event_time(t0_ms, 1234567)) == 1234567


# --- the watermark merge ---

def test_a_finished_archive_merges_the_whole_tape_in_offset_order() -> None:
    fetches = all_fetches()
    bound = tape.watermark(tape.frontier(one) for one in fetches)
    assert bound is None                                  # every feed exhausted → no bound
    lines = tape.merge(fetches, bound=bound, limit=10_000)
    assert len(lines) == sum(len(one.lines) for one in fetches)
    assert [(line.offset_ms, line.feed) for line in lines] == sorted(
        (line.offset_ms, line.feed) for one in fetches for line in one.lines)
    # Ties are broken by feed name, deterministically: SessionInfo comes before TrackStatus at
    # the shared offset 0, which is what makes the board's "SessionInfo first" gate reliable.
    assert [line.feed for line in lines[:2]] == ["SessionInfo", "TrackStatus"]
    assert tape.cursors_after(fetches, lines) == {one.feed: one.tail for one in fetches}


def test_a_feed_still_being_read_clamps_the_whole_emission() -> None:
    # TrackStatus read only to its second line: nothing past that offset may be emitted, or the
    # board would tag laps with a flag state it has not seen yet.
    partial_bytes = raw("TrackStatus")
    cut = partial_bytes.index(tape.SEPARATOR, partial_bytes.index(tape.SEPARATOR) + 2) + 2
    partial = tape.frame(partial_bytes[:cut], feed="TrackStatus", start=0)
    others = [one for one in all_fetches() if one.feed != "TrackStatus"]
    fetches = [partial, *others]

    bound = tape.watermark(tape.frontier(one) for one in fetches)
    assert bound == partial.lines[-1].offset_ms
    lines = tape.merge(fetches, bound=bound, limit=10_000)
    assert lines and all(line.offset_ms <= bound for line in lines)
    assert any(line.offset_ms > bound for one in others for line in one.lines)  # held back

    # Cursors advance only past what was emitted; the rest is simply re-read next poll.
    cursors = tape.cursors_after(fetches, lines)
    assert cursors["TrackStatus"] == partial.tail
    timing_lines = [line for line in lines if line.feed == "TimingData"]
    assert cursors["TimingData"] == timing_lines[-1].end
    assert cursors["TimingData"] < fetch("TimingData").tail


def test_an_exhausted_feed_drops_out_of_the_bound_entirely() -> None:
    # ExtrapolatedClock's last line sits early in the tape. If an exhausted feed still bounded
    # the watermark, the rest of the session could never be emitted.
    clock = fetch("ExtrapolatedClock")
    assert clock.at_eof and tape.frontier(clock) is None
    assert tape.watermark([None, None]) is None
    assert tape.watermark([None, 500, None]) == 500


def test_a_quiet_feed_is_unblocked_by_the_live_frontier() -> None:
    # Live phase: a feed read to the current end of file has said "nothing up to now", so its
    # frontier is the wall-derived offset rather than its last record's.
    quiet = tape.frame(raw("TrackStatus"), feed="TrackStatus", start=0, at_eof=True)
    assert tape.frontier(quiet, live_frontier_ms=9_000_000) == 9_000_000
    # ... whereas in the archive phase the same read means "exhausted".
    assert tape.frontier(quiet, live_frontier_ms=None) is None
    # A feed NOT at end-of-file always bounds at its last line, in either phase.
    partial = tape.frame(raw("TrackStatus")[:120], feed="TrackStatus", start=0)
    assert tape.frontier(partial, live_frontier_ms=9_000_000) == partial.lines[-1].offset_ms


def test_a_chunk_with_no_complete_line_blocks_rather_than_guesses() -> None:
    stub = tape.frame(raw("TimingData")[:300], feed="TimingData", start=0)
    assert stub.lines == [] and not stub.at_eof
    assert tape.frontier(stub) == tape.BLOCKED
    assert tape.watermark([tape.BLOCKED, 5000]) == tape.BLOCKED
    assert tape.merge(all_fetches(), bound=tape.BLOCKED, limit=10_000) == []


def test_the_line_budget_caps_a_poll_and_leaves_a_prefix_cursor() -> None:
    fetches = all_fetches()
    lines = tape.merge(fetches, bound=None, limit=7)
    assert len(lines) == 7
    cursors = tape.cursors_after(fetches, lines)
    # Each feed advanced exactly to its last emitted line, and untouched feeds not at all.
    for one in fetches:
        emitted = [line for line in lines if line.feed == one.feed]
        if not emitted:
            assert one.feed not in cursors
        elif len(emitted) == len(one.lines):
            assert cursors[one.feed] == one.tail
        else:
            assert cursors[one.feed] == emitted[-1].end


# --- board: scalar parsing ---

@pytest.mark.parametrize("raw_value,expected", [
    ("1:26.406", 86406), ("1:22.491", 82491), ("23.042", 23042), ("24.849", 24849),
    ("1:22.5", 82500), ("59", 59000), ("", None), ("  ", None), (None, None), ("x", None),
    ("10:05.123", 605123),
])
def test_parse_duration_ms(raw_value, expected) -> None:
    assert board.parse_duration_ms(raw_value) == expected


@pytest.mark.parametrize("raw_value,expected", [
    ("", (None, None)),                # unknown — a car in the garage
    ("LAP 1", (None, None)),           # the leader's lap counter, leaking into the gap column
    ("LAP 24", (None, None)),
    ("+1.234", (1.234, None)),
    ("+83.497", (83.497, None)),
    ("1.234", (1.234, None)),
    ("1L", (None, 1)),                 # lapped
    ("15L", (None, 15)),
    ("1 L", (None, 1)),                # the spaced variant some seasons emit
    ("-0.500", (-0.5, None)),
    (None, (None, None)),
    ("nonsense", (None, None)),
])
def test_parse_gap_keeps_seconds_and_laps_apart(raw_value, expected) -> None:
    assert board.parse_gap(raw_value) == expected


def test_parse_clock_and_gmt_offset() -> None:
    assert board.parse_clock_s("01:59:59") == 7199.0
    assert board.parse_clock_s("00:17:01") == 1021.0
    assert board.parse_clock_s("") is None
    assert board.parse_gmt_offset("02:00:00") == timedelta(hours=2)
    assert board.parse_gmt_offset("-05:00:00") == timedelta(hours=-5)
    assert board.parse_gmt_offset("") is None


def test_parse_int_and_float_never_fabricate_a_zero() -> None:
    assert board.parse_int("7") == 7 and board.parse_int(7) == 7
    assert board.parse_int("") is None and board.parse_int(None) is None
    assert board.parse_int(True) is None            # a bool is an int in Python, never a count
    assert board.parse_float("30.0") == 30.0 and board.parse_float(30) == 30.0
    assert board.parse_float("") is None and board.parse_float("x") is None


def test_indexed_normalizes_both_collection_shapes() -> None:
    assert board.indexed([{"a": 1}, {"b": 2}]) == {0: {"a": 1}, 1: {"b": 2}}
    assert board.indexed({"2": "c", "0": "a"}) == {0: "a", 2: "c"}   # sorted by index
    assert board.indexed({"_deleted": ["18"]}) == {}                 # not a collection entry
    assert board.indexed(None) == {} and board.indexed("x") == {}


def test_lines_of_accepts_both_envelopes() -> None:
    assert board.lines_of({"Lines": {"1": {"a": 1}}}) == {"1": {"a": 1}}
    assert board.lines_of({"1": {"a": 1}}) == {"1": {"a": 1}}        # DriverList's shape
    assert board.lines_of({"Lines": []}) == {} and board.lines_of(None) == {}


def test_severity_is_a_total_order_the_codes_are_not() -> None:
    assert board.severity_of("AllClear") < board.severity_of("VSCEnding")
    assert board.severity_of("VSCEnding") < board.severity_of("VSCDeployed")   # code 7 < code 6
    assert board.severity_of("VSCDeployed") < board.severity_of("Red")
    assert board.severity_of("SomethingNew") == board.UNKNOWN_SEVERITY > 0     # never "clear"


# --- board: folds ---

def test_session_meta_computes_utc_bounds_and_the_display_label() -> None:
    payload = fetch("SessionInfo").lines[0].payload
    meta = board.session_meta(payload, {})
    assert meta[M_KEY] == 11342
    assert meta[M_LABEL] == "Hungarian Grand Prix — Race (2026)"
    # 15:00 local at GMT+02:00 is 13:00 UTC — the arithmetic no dashboard should have to do.
    assert meta[M_START_UTC] == "2026-07-26T13:00:00+00:00"
    assert meta["end_utc"] == "2026-07-26T15:00:00+00:00"
    assert meta["circuit"] == "Hungaroring" and meta["country"] == "Hungary"
    assert meta["status"] == "Inactive"


def test_session_meta_merges_rather_than_replaces() -> None:
    meta = board.session_meta({"SessionStatus": "Finalised"},
                              board.session_meta(fetch("SessionInfo").lines[0].payload, {}))
    assert meta["status"] == "Finalised" and meta[M_KEY] == 11342


def test_fold_driver_list_merges_a_line_patch_into_a_full_entry() -> None:
    drivers = board.fold_driver_list({}, fetch("DriverList").lines[0].payload)
    assert set(drivers) == {"1", "16", "81"}
    assert drivers["1"]["tla"] == "NOR" and drivers["1"]["colour"] == "F47600"
    patched = board.fold_driver_list(drivers, {"1": {"Line": 3}})
    assert patched["1"]["line"] == 3 and patched["1"]["tla"] == "NOR"     # identity survives


def test_fold_timing_app_data_uses_the_latest_stint_and_counts_start_laps() -> None:
    drivers = board.fold_timing_app_data({}, {"Lines": {"16": {"Stints": [
        {"Compound": "MEDIUM", "TotalLaps": 17, "StartLaps": 0},
        {"Compound": "SOFT", "TotalLaps": 4, "StartLaps": 3},
    ]}}})
    assert drivers["16"][D_COMPOUND] == "SOFT"
    assert drivers["16"][D_TYRE_AGE] == 7        # 4 laps on a set that had already done 3
    assert drivers["16"][D_STINT] == 2
    # An index-keyed patch adding a third stint must not lose the first two's numbering.
    patched = board.fold_timing_app_data(drivers, {"Lines": {"16": {"Stints": {
        "2": {"Compound": "HARD", "TotalLaps": 0, "StartLaps": 0}}}}})
    assert patched["16"][D_STINT] == 3 and patched["16"][D_TYRE_AGE] == 0


def test_fold_timing_data_merges_the_keyframe_then_partial_patches() -> None:
    keyframe = fetch("TimingData").lines[0].payload
    drivers, laps = board.fold_timing_data({}, keyframe, severity=0, label="AllClear")
    assert laps == []
    assert drivers["1"][D_POSITION] == 1 and drivers["1"][D_IN_PIT] is True
    assert drivers["1"][D_SECTORS] == [None, None, None]     # empty Values, not zeros

    # A sector patch arrives as an index-keyed dict and must merge by index.
    drivers, _ = board.fold_timing_data(
        drivers, {"Lines": {"1": {"Sectors": {"0": {"Value": "28.844"}}}}},
        severity=0, label="AllClear")
    assert drivers["1"][D_SECTORS] == [28844, None, None]
    assert drivers["1"][D_POSITION] == 1                     # untouched fields survive


def test_a_segments_only_patch_changes_nothing_the_board_promotes() -> None:
    # 88 % of TimingData lines look like this. They must leave the entry byte-identical, which is
    # what makes emit-on-change suppress them.
    drivers, _ = board.fold_timing_data({}, fetch("TimingData").lines[0].payload,
                                        severity=0, label="AllClear")
    for payload in ({"Lines": {"1": {"Sectors": {"0": {"Segments": [{"Status": 2048}]}}}}},
                    {"Lines": {"1": {"Sectors": {"1": {"Segments": {"2": {"Status": 2048}}}}}}}):
        after, laps = board.fold_timing_data(drivers, payload, severity=0, label="AllClear")
        assert after == drivers and laps == []


def test_lap_completion_fires_on_number_of_laps_increasing() -> None:
    drivers, _ = board.fold_timing_data({}, {"Lines": {"1": {"Position": "1"}}},
                                        severity=0, label="AllClear")
    drivers, laps = board.fold_timing_data(drivers, {"Lines": {"1": {
        "NumberOfLaps": 12, "Sectors": {"2": {"Value": "23.042"}},
        "LastLapTime": {"Value": "1:22.491"}}}}, severity=0, label="AllClear")
    assert len(laps) == 1
    assert laps[0]["lap"] == 12 and laps[0]["lap_ms"] == 82491
    assert laps[0]["sectors"][2] == 23042 and laps[0]["clean"] is True
    assert drivers["1"][D_LAPS] == 12

    # Re-sending the same counter (a PersonalFastest flag arriving late) must NOT fire again.
    _, again = board.fold_timing_data(drivers, {"Lines": {"1": {
        "NumberOfLaps": 12, "LastLapTime": {"PersonalFastest": True}}}},
        severity=0, label="AllClear")
    assert again == []


def test_a_lap_under_a_virtual_safety_car_is_tagged_and_not_clean() -> None:
    drivers, _ = board.fold_timing_data({}, {"Lines": {"1": {"Position": "1"}}},
                                        severity=0, label="AllClear")
    # The VSC comes out and goes away again entirely between this driver's own patches — which is
    # exactly the case a per-patch sample would miss, so fold_track bumps every driver.
    _, drivers, changed = board.fold_track({}, drivers, {"Status": "6", "Message": "VSCDeployed"},
                                           datetime(2026, 7, 26, 14, tzinfo=UTC))
    assert changed and drivers["1"][D_LAP_WORST] == [3, "VSCDeployed"]
    _, drivers, _ = board.fold_track({"code": "6", "label": "VSCDeployed"}, drivers,
                                     {"Status": "1", "Message": "AllClear"},
                                     datetime(2026, 7, 26, 14, 1, tzinfo=UTC))
    assert drivers["1"][D_LAP_WORST] == [3, "VSCDeployed"]      # the max survives the all-clear

    drivers, laps = board.fold_timing_data(drivers, {"Lines": {"1": {
        "NumberOfLaps": 5, "LastLapTime": {"Value": "1:45.000"}}}},
        severity=0, label="AllClear")
    assert laps[0]["track_status"] == "VSCDeployed" and laps[0]["clean"] is False
    # ... and the accumulator resets to what is flying NOW, so the next lap can be clean.
    assert drivers["1"][D_LAP_WORST] == [0, "AllClear"]


def test_a_lap_involving_the_pit_lane_is_never_clean() -> None:
    drivers, _ = board.fold_timing_data({}, {"Lines": {"16": {"InPit": True}}},
                                        severity=0, label="AllClear")
    assert drivers["16"][D_LAP_PITTED] is True
    drivers, laps = board.fold_timing_data(drivers, {"Lines": {"16": {
        "InPit": False, "PitOut": True, "NumberOfLaps": 9,
        "LastLapTime": {"Value": "1:40.000"}}}}, severity=0, label="AllClear")
    assert laps[0]["pitted"] is True and laps[0]["clean"] is False
    assert laps[0]["track_status"] == "AllClear"      # green, but still not comparable
    # The out-lap is still a pit lap: PitOut is set, so the accumulator stays true.
    assert drivers["16"][D_LAP_PITTED] is True


def test_fold_track_ignores_a_restated_status() -> None:
    track, drivers, changed = board.fold_track({}, {}, {"Status": "1", "Message": "AllClear"},
                                               datetime(2026, 7, 26, 12, tzinfo=UTC))
    assert changed and track[T_LABEL] == "AllClear" and track[T_SEVERITY] == 0
    _, _, again = board.fold_track(track, drivers, {"Status": "1", "Message": "AllClear"},
                                   datetime(2026, 7, 26, 12, 5, tzinfo=UTC))
    assert not again                                  # no annotation churn


def test_fold_track_names_a_code_that_carries_no_message() -> None:
    track, _, _ = board.fold_track({}, {}, {"Status": "4"},
                                   datetime(2026, 7, 26, 12, tzinfo=UTC))
    assert track[T_LABEL] == "SCDeployed" and track[T_SEVERITY] == 4
    unknown, _, _ = board.fold_track({}, {}, {"Status": "9"},
                                     datetime(2026, 7, 26, 12, tzinfo=UTC))
    assert unknown[T_LABEL] == "Status9" and unknown[T_SEVERITY] == board.UNKNOWN_SEVERITY


def test_fold_clock_keeps_total_laps_that_is_sent_only_once() -> None:
    clock = board.fold_clock({}, {"CurrentLap": 1, "TotalLaps": 70})
    clock = board.fold_clock(clock, {"CurrentLap": 2})
    assert clock == {"lap": 2, "total_laps": 70}
    clock = board.fold_clock(clock, {"Utc": "…", "Remaining": "01:59:59", "Extrapolating": True})
    assert clock["remaining_s"] == 7199.0 and clock["extrapolating"] is True
    assert clock["total_laps"] == 70


def test_race_control_accepts_the_array_and_the_index_keyed_shapes() -> None:
    array_line, dict_line = fetch("RaceControlMessages").lines
    assert isinstance(array_line.payload["Messages"], list)
    assert isinstance(dict_line.payload["Messages"], dict)
    assert board.race_control_entries(array_line.payload)[0]["Flag"] == "GREEN"
    assert board.race_control_entries(dict_line.payload)[0]["Sector"] == 19
    assert board.race_control_entries({"Messages": {}}) == []


def test_series_entries_flattens_both_shapes_of_a_per_driver_collection() -> None:
    array_line, dict_line = fetch("PitStopSeries").lines
    assert board.series_entries(array_line.payload, "PitTimes")[0][0] == "16"
    assert board.series_entries(dict_line.payload, "PitTimes")[0][1]["PitStop"]["Lap"] == "4"
    assert board.series_entries({"PitTimes": {}}, "PitTimes") == []


def test_championship_rows_split_drivers_from_teams() -> None:
    rows = board.championship_rows(fetch("ChampionshipPrediction").lines[0].payload)
    assert {row["entity_type"] for row in rows} == {"driver", "team"}
    driver, = [row for row in rows if row["entity_type"] == "driver"]
    assert driver["entity_id"] == "1" and driver["points"] == 103.0
    assert driver["predicted_points"] == 128.0
    team, = [row for row in rows if row["entity_type"] == "team"]
    assert team["entity_id"] == "McLaren Mercedes"     # a constructor name, not a TeamName


def test_car_and_position_samples_explode_per_car_and_keep_their_own_clocks() -> None:
    samples = board.car_samples(fetch("CarData.z").lines[0].payload)
    assert len(samples) == 4                          # 3 cars + 1 in the second entry
    utc, number, channels = samples[0]
    assert number == "1" and channels == {"rpm": 10994, "speed": 148, "gear": 3,
                                          "throttle": 104, "brake": 104}
    assert utc == "2026-07-26T12:09:26.4670000Z"      # the SAMPLE's clock, not the line's
    assert samples[3][0] == "2026-07-26T12:09:26.6870000Z"

    positions = board.position_samples(fetch("Position.z").lines[0].payload)
    assert len(positions) == 3
    assert positions[0][2] == {"x": -8, "y": 9101, "z": 2388, "status": "OnTrack"}
    assert positions[2][2]["status"] == "OffTrack"


def test_weather_row_coerces_every_string() -> None:
    row = board.weather_row(fetch("WeatherData").lines[0].payload)
    assert row == {"air_temp": 30.0, "track_temp": 51.3, "humidity": 25.6, "pressure": 980.6,
                   "rainfall": 0.0, "wind_speed": 1.9, "wind_direction": 145.0}
    assert board.weather_row({"AirTemp": ""}) == {}    # absent, not zero


# --- the run_board fold, driven directly ---

def _incoming(feed: str, payload, *, offset_ms: int = 0,
              session: str = SESSION_PATH) -> IncomingMessage:
    record = Event({SESSION: session, FEED: feed, OFFSET_MS: offset_ms,
                    EVENT_TIME: T0 + timedelta(milliseconds=offset_ms)})
    record.raw[PAYLOAD] = payload
    return IncomingMessage(key=session, offset=0, partition=0, timestamp=None,
                           topic=TIMING_TOPIC, value=record)


async def _drive(lines) -> tuple[State, list[Message]]:
    """Run every line through ``run_board``, threading the state — the whole stage, purely."""
    state, produced = State(), []
    for line in lines:
        async for item in run_board(state, _incoming(line.feed, line.payload,
                                                     offset_ms=line.offset_ms)):
            if isinstance(item, State):
                state = item
            else:
                produced.append(item)
    return state, produced


def _records(produced: list[Message], topic: str, kind: str) -> list[dict]:
    return [json.loads(json.dumps(message.value.raw)) for message in produced
            if message.topic == topic and message.value.raw.get("kind") == kind]


async def test_nothing_is_emitted_before_session_info() -> None:
    state = State()
    produced = [item async for item in run_board(
        state, _incoming("Heartbeat", {"Utc": "2026-07-26T12:09:26Z"}))]
    assert produced == []          # not even a State: there is nothing to remember


async def test_the_fixture_session_produces_the_expected_streams() -> None:
    fetches = all_fetches()
    lines = tape.merge(fetches, bound=None, limit=10_000)
    state, produced = await _drive(lines)

    # Every produced record carries both identities and a Message.timestamp.
    for message in produced:
        assert message.key == SESSION_PATH
        assert message.value.raw["session_key"] == 11342
        assert message.timestamp is not None

    assert len(_records(produced, EVENTS_TOPIC, SESSION_KIND)) >= 1
    assert {row["racing_number"] for row in _records(produced, EVENTS_TOPIC, DRIVER)} == {
        "1", "16", "81"}
    assert len(_records(produced, STATUS_TOPIC, HEARTBEAT)) == 6
    assert len(_records(produced, STATUS_TOPIC, "weather")) == 1
    assert len(_records(produced, TELEMETRY_TOPIC, CAR)) == 4
    assert len(_records(produced, TELEMETRY_TOPIC, POS)) == 3
    assert [row["message"] for row in _records(produced, EVENTS_TOPIC, RACE_CONTROL)] == [
        "GREEN LIGHT - PIT EXIT OPEN", "YELLOW IN TRACK SECTOR 19"]

    # The tape ends with SessionStatus "Ends" → the bucket is tombstoned.
    assert state == State()


async def test_the_leaderboard_only_emits_when_something_actually_changed() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    _, produced = await _drive(lines)
    standings = _records(produced, STATUS_TOPIC, STANDINGS)
    timing_lines = [line for line in lines if line.feed == "TimingData"]
    # Fewer snapshots than TimingData lines × drivers: the segment-only patches emitted nothing.
    assert 0 < len(standings) < len(timing_lines) * 3
    assert all(row["track_status"] for row in standings)


async def test_laps_carry_the_flag_they_ran_under_and_the_clean_verdict() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    _, produced = await _drive(lines)
    laps = _records(produced, EVENTS_TOPIC, LAP_KIND)
    by_driver_lap = {(row["racing_number"], row["lap"]): row for row in laps}

    green = by_driver_lap[("1", 2)]
    assert green["lap_ms"] == 82491 and green["sector1_ms"] == 28844
    assert green[SECTOR3_MS.name] == 23042 and green["track_status"] == "AllClear"

    vsc = by_driver_lap[("1", 3)]
    assert vsc["track_status"] == "VSCDeployed" and vsc["clean"] is False
    assert vsc["lap_ms"] == 105000                     # 25 s slower, as a VSC lap is

    # Lap 5 ran wholly under a green track and outside the pits — the one clean lap.
    assert by_driver_lap[("1", 5)]["clean"] is True
    assert by_driver_lap[("1", 5)]["track_status"] == "AllClear"


async def test_track_periods_open_and_close_in_pairs() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    _, produced = await _drive(lines)
    periods = _records(produced, EVENTS_TOPIC, TRACK_PERIOD)
    opened = [row for row in periods if ENDED_AT.name not in row]
    closed = [row for row in periods if ENDED_AT.name in row]
    assert [row[LABEL.name] for row in opened] == [
        "AllClear", "Yellow", "VSCDeployed", "VSCEnding", "AllClear"]
    assert len(closed) == len(opened)                  # the last one closes on Finalised
    for row in closed:
        assert row["ended_at"] > row["started_at"]
    vsc, = [row for row in closed if row[LABEL.name] == "VSCDeployed"]
    assert vsc["code"] == "6" and vsc["severity"] == 3


async def test_pit_events_use_the_stops_own_absolute_timestamp() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    _, produced = await _drive(lines)
    pits = _records(produced, EVENTS_TOPIC, PIT)
    assert [(row["racing_number"], row[LAP.name]) for row in pits] == [("16", 3), ("16", 4)]
    assert pits[0]["stationary_s"] == 2.4 and pits[0]["pit_lane_s"] == 21.931
    # The stop states its own UTC; that beats the tape offset for this record.
    assert pits[0]["event_time"] == "2026-07-26T12:11:53.355000Z"


async def test_the_clock_row_carries_the_race_distance_after_the_first_record() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    _, produced = await _drive(lines)
    clocks = _records(produced, STATUS_TOPIC, CLOCK_KIND)
    assert clocks[-1]["lap"] == 5 and clocks[-1]["total_laps"] == 5
    assert clocks[-1]["remaining_s"] == 1021.0 and clocks[-1]["extrapolating"] is True


async def test_finalised_emits_the_official_classification_then_ends_tombstones() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    # Everything up to (but not including) the "Ends" status.
    ends = next(index for index, line in enumerate(lines)
                if line.feed == "SessionStatus" and line.payload.get("Status") == "Ends")
    state, produced = await _drive(lines[:ends])
    assert state.get(META, {})[M_KEY] == 11342
    assert state.get(FINALISED) is True
    final = _records(produced, STATUS_TOPIC, STANDINGS)[-3:]
    assert {row["racing_number"] for row in final} == {"1", "16", "81"}
    retired, = [row for row in final if row["racing_number"] == "81"]
    assert retired["retired"] is True and retired["stopped"] is True

    # ... and only then does the bucket go.
    state, _ = await _drive(lines)
    assert state == State()


async def test_a_record_arriving_after_the_tombstone_emits_nothing() -> None:
    produced = [item async for item in run_board(
        State(), _incoming("TimingData", {"Lines": {"1": {"Position": "1"}}}))]
    assert produced == []


async def test_the_board_state_stays_small() -> None:
    lines = tape.merge(all_fetches(), bound=None, limit=10_000)
    ends = next(index for index, line in enumerate(lines)
                if line.feed == "SessionStatus" and line.payload.get("Status") == "Ends")
    state, _ = await _drive(lines[:ends])
    # One changelog record per session, well under the broker's ~1 MB ceiling. Three drivers
    # here; a full grid is 22, and the per-driver entry is what scales.
    encoded = json.dumps(state.raw)
    assert len(encoded) < 4_000
    assert set(state.raw) == {META.name, DRIVERS.name, TRACK.name, CLOCK.name, FINALISED.name}
    assert set(state[DRIVERS]) == {"1", "16", "81"}
    assert state[TRACK][T_LABEL] == "AllClear"


def test_the_fixture_covers_the_shapes_the_tests_rely_on() -> None:
    """A guard on the fixture itself: regenerate it and these must still hold."""
    total = sum(path.stat().st_size for path in FIXTURES.iterdir())
    assert total < 100_000, "fixtures must stay small — see PROVENANCE.md"
    feeds = {path.name.removesuffix(".jsonStream") for path in FIXTURES.glob("*.jsonStream")}
    assert INGESTED_FEEDS <= feeds | {"CarData.z", "Position.z"}
    # Two feeds the session publishes but the wish list does not want — proof that a
    # published-but-unwanted feed is simply never read.
    assert {"TimingStats", "TyreStintSeries"} <= feeds
    assert not {"TimingStats", "TyreStintSeries"} & INGESTED_FEEDS
    index = json.loads((FIXTURES / "Index.json").read_bytes().decode("utf-8-sig"))
    assert set(index["Feeds"]) == feeds
    assert json.loads(
        (FIXTURES / "ArchiveStatus.json").read_bytes().decode("utf-8-sig")) == {"Status": "Complete"}

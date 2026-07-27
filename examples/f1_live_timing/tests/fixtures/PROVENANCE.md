# Test fixtures — provenance

`session/` is a **miniature session** assembled from **real** lines of the 2026 Hungarian Grand
Prix race tape, captured **2026-07-27** from

```
https://livetiming.formula1.com/static/2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/
```

Every **shape** is verbatim — the framing, the BOM, the offset prefix, every payload key, the
array→index-dict shifts, the raw-deflate encoding of the `.z` feeds. Only the **values** are
trimmed, and only in the ways listed below. The whole session is **12 KB** across 18 feeds and 53
lines; the real race is 21 MB across 33 feeds and 93 051 lines.

Re-capture any feed with:

```bash
BASE=https://livetiming.formula1.com/static/2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race
curl -H 'Accept-Encoding: identity' "$BASE/Index.json"                    # the feed list
curl -H 'Accept-Encoding: identity' "$BASE/ArchiveStatus.json"            # {"Status":"Complete"}
curl -H 'Accept-Encoding: identity' -r 0-4095 "$BASE/TrackStatus.jsonStream"
```

## What was trimmed, and why

* **Three drivers instead of 22** — `1` NOR (McLaren), `16` LEC (Ferrari), `81` PIA (McLaren),
  with their real numbers, TLAs, team names and livery hex codes from the session's own
  `DriverList`. A 22-car grid would make the `TimingData` keyframe alone 20 KB and prove nothing
  extra.
* **Five laps instead of 70**, and offsets compressed into ~3¾ minutes of tape. The *relative*
  ordering of the feeds is preserved (`SessionInfo` and `TrackStatus` at offset 0, `DriverList`
  and the `TimingData` keyframe together at `00:00:08.610`, exactly as the real tape has them).
* **Every inner clock is made consistent with the fixture's own `t0`.** The real tape's first
  `Heartbeat` line — `00:00:13.844{"Utc":"2026-07-26T12:09:26.3114877Z"}` — is kept **verbatim**,
  which is what pins the fixture's anchor to the real race's
  `t0 = 2026-07-26T12:09:12.467Z`. The other feeds' inner instants were then adjusted to agree
  with that anchor, so the fallback anchors (`ExtrapolatedClock`, `RaceControlMessages`,
  `CarData.z`, `Position.z`) all land within a second of it. In the real tape they differ by up to
  three seconds — that is genuine broadcast-pipeline bias, documented in `tape.anchor`, and it is
  *not* what the fallback tests are for.
* **Heartbeat jitter is real.** The five beats imply anchors spanning 0.183 s, which is the actual
  spread of the real race's first five beats — enough to exercise `ANCHOR_SPREAD` without
  tripping it.

## What the fixture deliberately contains

Each of these exists because a test would otherwise have nothing to bite on:

| In the Fixture | Why |
|---|---|
| a UTF-8 **BOM** on every file | it counts toward the byte offsets and must be stripped only at offset 0 |
| `WeatherData.jsonStream` ending **without a trailing CRLF** | an unterminated final line is content in a finished file and a fragment in a growing one |
| `RaceControlMessages` as an **array**, then an **index-keyed dict** | the shape shift the real feed makes a few lines in |
| `TimingData` `Sectors` as a **list** (keyframe) and as an **index-keyed dict** (patches) | the same shift, per field, mid-stream |
| `Segments` as a **list** in one patch and a **dict** in the next | it happens; both must be ignored identically (they change nothing the board promotes) |
| a `TimingData` keyframe with `InPit: true` and empty `Value`s | the pre-race state, which is why the first counted lap is never `clean` |
| a lap completed **under a VSC** (car 1, lap 3) | the flag SCD's whole reason for existing |
| a lap completed wholly **green and out of the pits** (car 1, lap 5) | the only `clean` lap, so `clean` is tested in both directions |
| a **pit in / pit out** pair for car 16, with `NumberOfPitStops` | pit state, and the stint change that follows it |
| a **retirement** for car 81 (`Retired` + `Stopped`) | the status column, and a driver whose lines simply stop |
| `PitStopSeries` with an **array** line and an **index-dict** line | the per-driver-collection shape, and each stop's own absolute `Timestamp` |
| `SessionStatus` reaching **`Finalised` then `Ends`** | the classification is emitted on the first, the bucket tombstoned on the second |
| `CarData.z` with channels `0,2,3,4,5` and throttle/brake values of **104** | the real 2026 channel set — there is **no channel 45 (DRS)** — on the upstream's own 0–104 scale |
| `Position.z` with a car `OffTrack` and negative `X` | why the column is `Int32` and not unsigned |
| `TimingStats` and `TyreStintSeries` present in `Index.json` but **not** in the wish list | proof that a published-but-unwanted feed is never fetched |
| `ArchiveStatus.json` = `Complete` | the completion probe; note the real `.jsonStream` twin says `Generating` forever |

`Index.json` lists exactly the 18 feeds present, so the wish-list intersection is exercised in
both directions: four wished feeds are absent (`SessionData`-style gaps), two published feeds are
unwanted.

## Not in the fixture, on purpose

A malformed line, an oversized line, and a non-UTF-8 line are all synthesized inline in
`logic_test.py` rather than committed. They have never been observed in the archive, and putting
a deliberately corrupt line in a file described as "real lines" would undermine the point of the
rest of it.

"""Typed attributes, topic names, and state keys for the F1 Live Timing example.

Two schemas meet here and they are treated differently on purpose.

**The tape topic carries a foreign schema, so it declares only its envelope.** A
``f1-timing`` record wraps one line of one live-timing feed, and those feeds are
heterogeneous, undocumented, and free to change shape mid-session (the
``RaceControlMessages`` array→dict shift, ``Sectors`` as a list in a keyframe and an
index-keyed dict in a patch). Declaring 33 feeds' worth of fields would be inventing a
schema we do not own and cannot keep; so the envelope — :data:`SESSION`, :data:`FEED`,
:data:`OFFSET_MS`, :data:`EVENT_TIME` — is typed, and the decoded payload rides whole
under the raw key :data:`PAYLOAD`, nested rather than spread (a ``Status`` or a ``Utc``
from some feed would otherwise collide with an envelope key).

**The board's output schema is ours, so every field of it is declared.** ``f1-status``,
``f1-events``, and ``f1-telemetry`` are *constructed* by :mod:`.timing` out of folded
state — nothing is passed through untouched except a handful of race-control fields whose
seven keys we do promote by name. That is the wildfire/odds/SMARD side of the house rule:
declare what you compute with, spread what you don't own.

**Every clock is upstream-owned, and there are three of them.** The tape's native clock is
a *recording offset* (:data:`OFFSET_MS`, milliseconds since the recorder started, which is
~50 min before lights-out); :data:`EVENT_TIME` is that offset placed in absolute UTC
against the ``t0`` anchor the ingest stage derives from the ``Heartbeat`` feed; and a few
payloads carry their own absolute instant (a ``CarData`` sample's ``Utc``, a pit stop's
``Timestamp``), which wins for the record it belongs to. No stage ever reads a wall clock
on the archive path, so every code path stays drivable from the logic tier.

Codecs are exact-type: ``FLOAT`` rejects ``int`` (the feed ships ``"30.0"`` as a *string*
and ``PitStopTime`` as ``"2.1"``, so both go through ``float()``), and ``INT`` rejects
``bool``. Lap and sector times are stored as **integer milliseconds** — the feed's
``"1:26.406"`` is for humans; a duration has to be summable.
"""
from typing import Final

from flechtwerk.attribute import ANY, Attribute, BOOL, DATETIME, DICT, FLOAT, INT, LIST, STR

# --- Topics (the wire contract, shared by both stages) ---

SESSIONS_TOPIC: Final = "f1-sessions"
"""Compacted config topic, one record per session to ingest (keyed by the session **path**)
plus, optionally, one ``follow`` record per season (keyed ``season-<year>``). Seeded by
nobody — a user asks for sessions with ``uv run poe request-f1`` (or any producer, Kafbat
included). The one topic an extractor both *reads as config* and *writes to*: the follow
target self-produces session records for newly-listed sessions (see :mod:`.ingest`)."""

TIMING_TOPIC: Final = "f1-timing"
"""**The tape.** One record per line of one feed, event-timed and decoded, keyed by the
session path so a session's whole stream co-partitions onto the one board task that owns
it. This is the example's centre of gravity: an append-only recording of a live feed,
replayed byte-exactly, from which everything downstream is derived. Retained forever
(``retention.ms=-1``) — see :mod:`.setup` for why time-based retention would eat a
backfill alive."""

STATUS_TOPIC: Final = "f1-status"
"""Continuous stream: per-driver ``standings`` snapshots (the leaderboard), plus
``weather``, ``clock``, and ``heartbeat`` rows. High-volume — a race emits tens of
thousands of standings rows, which is the point: the leaderboard *is* keyed state, and
these are its materializations."""

EVENTS_TOPIC: Final = "f1-events"
"""Sparse stream: ``lap``, ``pit``, ``track_period``, ``race_control``, ``overtake``,
``championship``, and the two dimension upserts ``session`` / ``driver``. A session's story
rather than its state — kept without a TTL (the ``odds-signals`` rationale)."""

TELEMETRY_TOPIC: Final = "f1-telemetry"
"""High-rate stream: ``car`` (rpm/speed/gear/throttle/brake, ~4–5 Hz × 22 cars) and ``pos``
(X/Y/Z track coordinates) samples, exploded one row per car per sample and event-timed by
the sample's **own** inner instant. Gated at the *ingest* side — with ``telemetry: false``
on the session config the two ``.z`` feeds are never fetched, halving the download."""

# --- Config record (f1-sessions; wire key = session path, or "season-<year>") ---

KIND: Final = Attribute("kind", STR)
"""The record's self-description, so one topic can carry more than one shape and every
materialized view stays explicit (the SMARD idiom). ``session`` | ``follow`` on
``f1-sessions``; ``standings`` | ``weather`` | ``clock`` | ``heartbeat`` on ``f1-status``;
``lap`` | ``pit`` | ``track_period`` | ``race_control`` | ``overtake`` | ``championship``
| ``session`` | ``driver`` on ``f1-events``; ``car`` | ``pos`` on ``f1-telemetry``."""

PATH: Final = Attribute("path", STR)
"""The session's archive path, e.g.
``2026/2026-07-26_Hungarian_Grand_Prix/2026-07-26_Race/`` — trailing slash included, as
the index publishes it. **The identity of everything**: the config record's wire key, the
Kafka key of every record this example produces, the ingest stage's state-bucket key, and
the board's state-bucket key. Deliberately the path and not the numeric ``Key``, because a
path can be requested before the index lists the session (the escape hatch for a live
weekend), while its session key cannot be known until ``SessionInfo`` arrives."""

YEAR: Final = Attribute("year", INT)
"""The season, e.g. ``2026`` — the segment the archive's own index is keyed by, carried on
both config kinds and on the session dimension row."""

SESSION_KEY: Final = Attribute("session_key", INT, optional=True)
"""The archive's numeric session id (``11342`` for the 2026 Hungarian GP race) — **the
dashboards' one filter variable**, on every output row.

Optional on the *config* record because a hand-supplied path may predate the index listing
it; the board learns the real value from ``SessionInfo`` (offset ~0 in every tape) and
stamps it on everything downstream. Deliberately int-typed: it is an id, and Grafana's
``${session}`` interpolates it unquoted."""

MEETING: Final = Attribute("meeting", STR, optional=True)
"""The Grand Prix weekend's name, e.g. ``Hungarian Grand Prix`` — from the index, and from
``SessionInfo.Meeting.Name``."""

SESSION_NAME: Final = Attribute("session_name", STR, optional=True)
"""The session's name within the meeting: ``Race``, ``Sprint``, ``Qualifying``, ``Sprint
Qualifying``, ``Practice 1``…``3``, or ``Day 1``…``3`` for pre-season testing. The exact
strings the year index publishes — :mod:`.request` matches on them case-insensitively."""

SESSION_TYPE: Final = Attribute("session_type", STR, optional=True)
"""``SessionInfo``'s coarser ``Type``: ``Race`` | ``Qualifying`` | ``Practice``. Note
testing days are ``Practice`` and a sprint is its own ``Type``; the fine distinction lives
in :data:`SESSION_NAME`."""

START_LOCAL: Final = Attribute("start_local", STR, optional=True)
"""The scheduled start as the index gives it — **local track time, no zone**
(``2026-07-26T15:00:00``). Kept verbatim as a string precisely because it is *not* an
instant: pairing it with :data:`GMT_OFFSET` is what makes :data:`START_UTC`."""

GMT_OFFSET: Final = Attribute("gmt_offset", STR, optional=True)
"""The track's UTC offset for that session as ``HH:MM:SS`` (``02:00:00`` at the
Hungaroring). The other half of :data:`START_LOCAL`."""

START_UTC: Final = Attribute("start_utc", DATETIME, optional=True)
"""The session's scheduled start as a real instant: ``StartDate − GmtOffset``. Computed once
by the board so no dashboard ever has to do timezone arithmetic in SQL — the season
dashboard's "open" and "▶ replay" links are built from this and :data:`END_UTC`."""

END_UTC: Final = Attribute("end_utc", DATETIME, optional=True)
"""The scheduled end, ``EndDate − GmtOffset`` — see :data:`START_UTC`. Both bounds are
*scheduled*: the tape starts ~50 min earlier and outlives the flag, which is why the season
dashboard pads the window it links to."""

LABEL: Final = Attribute("label", STR, optional=True)
"""A ready-made display name, ``"Hungarian Grand Prix — Race (2026)"`` — the ``__text`` of
the dashboards' ``$session`` variable. Built once here rather than reassembled in every
panel's SQL. Also the human name of a track-status period (``AllClear``, ``VSCDeployed``)."""

CIRCUIT: Final = Attribute("circuit", STR, optional=True)
"""``SessionInfo.Meeting.Circuit.ShortName``, e.g. ``Hungaroring``."""

COUNTRY: Final = Attribute("country", STR, optional=True)
"""``SessionInfo.Meeting.Country.Name``, e.g. ``Hungary``."""

LOCATION: Final = Attribute("location", STR, optional=True)
"""``SessionInfo.Meeting.Location``, e.g. ``Budapest``."""

TELEMETRY: Final = Attribute("telemetry", BOOL, optional=True)
"""Whether to fetch this session's two ``.z`` feeds (``CarData.z``, ``Position.z``).

They are ~14 MB of the ~28 MB a race weighs, and they are the only feeds a modest machine
might not want. Gating is **ingest-side**: with it off the feeds are never requested, so
nothing downstream needs a flag — the board simply forwards whatever telemetry reaches the
tape. Absent means off, so a hand-written config record is cheap by default."""

TYPES: Final = Attribute("types", LIST(STR), optional=True)
"""On a ``follow`` record: which :data:`SESSION_NAME` values to pick up automatically as
the season's index grows. Absent means :data:`~.request.DEFAULT_TYPES` (the competitive
sessions)."""

# --- The tape record (f1-timing; key = session path) ---

SESSION: Final = Attribute("session", STR)
"""The session path this record belongs to — the same string as :data:`PATH`, duplicated
into the *value* of every produced record so a consumer reads it without decoding the Kafka
key (SMARD and wildfire do the same with their keys). Two names for one string because they
play different roles: :data:`PATH` is what a config record *asks for*, :data:`SESSION` is
what a data record *is about*."""

FEED: Final = Attribute("feed", STR)
"""Which live-timing feed the line came from — ``TimingData``, ``TrackStatus``,
``CarData.z``, … . Kept with the ``.z`` suffix exactly as the archive's index names it, even
though the payload is emitted **decoded**: the suffix is the feed's identity, and hiding it
would make the tape unmatchable against the index."""

OFFSET_MS: Final = Attribute("offset_ms", INT)
"""The line's native clock: milliseconds from the **recording's** start, parsed from the
fixed 12-character ``HH:MM:SS.mmm`` prefix.

Kept alongside :data:`EVENT_TIME` rather than thrown away, because it is the *source
position* in disguise — the byte cursor's ordering key, the watermark merge's sort key, and
the one value that is exact rather than anchored. Note it is **not** session-relative:
recording starts well before the session does, so a race's lights-out lands around
``00:54:00`` and not at zero."""

EVENT_TIME: Final = Attribute("event_time", DATETIME)
"""The record's instant in absolute UTC — ``t0 + offset``, where ``t0`` is anchored once per
session from the ``Heartbeat`` feed (see :func:`.tape.anchor`).

**The stream's whole honesty rests here.** Nothing is ever re-timestamped: a March race
backfilled in July carries March's timestamps, which is what makes "replay any past race"
and "watch a live one" the same Grafana query over a different time window. It is also what
makes the topics' unlimited retention a correctness requirement rather than a comfort. The
anchor carries a few seconds of broadcast-pipeline bias (documented in :func:`.tape.anchor`);
relative order and durations are exact."""

PAYLOAD: Final = "payload"
"""Raw key (not an ``Attribute``) under which the decoded feed payload rides whole.

A plain string for the same reason the wildfire tracker's ``F_*`` state keys are: this is
somebody else's schema living *inside* one of our keys, where it cannot collide with an
envelope field and does not pretend to be typed. ``.z`` payloads arrive here **already
inflated**, so no consumer ever needs zlib. Read it as ``record.raw[PAYLOAD]``."""

# --- Driver dimension + standings (f1-status kind=standings, f1-events kind=driver) ---

RACING_NUMBER: Final = Attribute("racing_number", STR)
"""The car number as the feed keys its per-driver dicts — a **string** (``"1"``, ``"81"``),
never an int, because that is what it is on the wire and what every payload's keys are.
Coercing it would break the join to ``TimingData.Lines`` for no gain."""

TLA: Final = Attribute("tla", STR, optional=True)
"""The three-letter abbreviation (``NOR``, ``LEC``, ``VER``) — what a pit wall actually
reads. Optional on a standings row because ``TimingData`` can patch a driver before
``DriverList`` has named them."""

FULL_NAME: Final = Attribute("full_name", STR, optional=True)
"""``Lando NORRIS`` — the feed's own rendering, surname upper-cased."""

FIRST_NAME: Final = Attribute("first_name", STR, optional=True)
"""Given name, from ``DriverList``."""

LAST_NAME: Final = Attribute("last_name", STR, optional=True)
"""Family name, from ``DriverList``."""

TEAM: Final = Attribute("team", STR, optional=True)
"""``TeamName``: ``McLaren``, ``Ferrari``, ``Red Bull Racing``. Note
``ChampionshipPrediction`` uses *constructor* names instead (``McLaren Mercedes``), which is
why the championship rows keep their own :data:`ENTITY_ID` rather than joining on this."""

TEAM_COLOUR: Final = Attribute("team_colour", STR, optional=True)
"""The team's livery colour as a bare six-digit hex string (``F47600``), no ``#``.

Carried because it is genuinely useful data — and *not* used to colour Grafana series, which
cannot take a colour from a data field. The dashboards bake per-team overrides into their
JSON instead; this column is what you build those from each season."""

REFERENCE: Final = Attribute("reference", STR, optional=True)
"""The feed's stable driver key (``LANNOR01``) — an id that survives a number change, worth
keeping even though nothing joins on it today."""

LINE: Final = Attribute("line", INT, optional=True)
"""The feed's display row for the driver. Tracks position closely but not exactly (it is a
*rendering* order, and it updates on its own cadence), so :data:`POSITION` is what the
leaderboard sorts by and this rides along for completeness."""

POSITION: Final = Attribute("position", INT, optional=True)
"""Classified position, ``1``-based. Coerced from the feed's string (``"1"``) because a
position is ordinal and the dashboards sort by it. Also reused on a ``championship`` row."""

GAP_RAW: Final = Attribute("gap_raw", STR, optional=True)
"""``GapToLeader`` **exactly as the feed sends it** — and it sends five different things:
``""`` (unknown), ``"LAP 24"`` (the leader, before anyone is a lap down), ``"+1.234"``
(seconds), ``"1L"``/``"15L"`` (laps down). Kept verbatim next to the parsed
:data:`GAP_S`/:data:`GAP_LAPS` because the string *is* what a pit wall displays, and because
a shape we failed to parse must stay visible rather than silently becoming NULL."""

GAP_S: Final = Attribute("gap_s", FLOAT, optional=True)
"""Gap to the leader in seconds, parsed from :data:`GAP_RAW`. Absent — never 0.0 — when the
gap is unknown, or when it is expressed in laps rather than seconds."""

GAP_LAPS: Final = Attribute("gap_laps", INT, optional=True)
"""Whole laps down to the leader, parsed from a ``"1L"``-shaped :data:`GAP_RAW`. Absent when
the gap is a time. A lapped car has no meaningful gap in seconds, and pretending otherwise
is how leaderboards end up sorting a backmarker onto the podium."""

INTERVAL_RAW: Final = Attribute("interval_raw", STR, optional=True)
"""``IntervalToPositionAhead.Value`` verbatim — the same five shapes as :data:`GAP_RAW`, but
measured to the car ahead. This is the number that decides whether there is a battle."""

INTERVAL_S: Final = Attribute("interval_s", FLOAT, optional=True)
"""The interval to the car ahead in seconds — the battle radar's threshold field."""

INTERVAL_LAPS: Final = Attribute("interval_laps", INT, optional=True)
"""Whole laps down to the car ahead — see :data:`GAP_LAPS`."""

CATCHING: Final = Attribute("catching", BOOL, optional=True)
"""``IntervalToPositionAhead.Catching`` — the feed's own judgement that this car is closing
on the one ahead. A gift: the interesting part of a leaderboard is who is *closing*, and the
upstream already computes it, so the battle radar does not have to differentiate a noisy
series."""

LAST_LAP_MS: Final = Attribute("last_lap_ms", INT, optional=True)
"""The driver's last completed lap, in **integer milliseconds** (``"1:26.406"`` → 86406).
Milliseconds because a lap time is a duration to be compared, summed, and plotted; the
string form is a rendering."""

BEST_LAP_MS: Final = Attribute("best_lap_ms", INT, optional=True)
"""The driver's personal best lap so far, in milliseconds."""

LAST_LAP_PERSONAL_BEST: Final = Attribute("last_lap_personal_best", BOOL, optional=True)
"""Whether the last lap was this driver's own fastest — the green lap on a pit wall. The feed
computes it (``LastLapTime.PersonalFastest``), so no window function has to."""

LAST_LAP_OVERALL_BEST: Final = Attribute("last_lap_overall_best", BOOL, optional=True)
"""Whether the last lap was the **session's** fastest — the purple lap
(``LastLapTime.OverallFastest``). Two booleans the upstream already knows are worth more than
a self-join that has to recompute them per row."""

SECTOR1_MS: Final = Attribute("sector1_ms", INT, optional=True)
"""Sector 1 time in milliseconds. The three sectors are separate columns rather than a list
because every panel and every query wants them individually, and because the feed patches
them **one at a time** — a list-valued column would be rewritten three times a lap."""

SECTOR2_MS: Final = Attribute("sector2_ms", INT, optional=True)
"""Sector 2 time in milliseconds — see :data:`SECTOR1_MS`."""

SECTOR3_MS: Final = Attribute("sector3_ms", INT, optional=True)
"""Sector 3 time in milliseconds — see :data:`SECTOR1_MS`. This is the one that lands
together with the lap, so it is the reliable "lap complete" companion."""

SPEED_I1: Final = Attribute("speed_i1", INT, optional=True)
"""Speed-trap reading at intermediate 1, km/h."""

SPEED_I2: Final = Attribute("speed_i2", INT, optional=True)
"""Speed-trap reading at intermediate 2, km/h."""

SPEED_FL: Final = Attribute("speed_fl", INT, optional=True)
"""Speed across the finish line, km/h."""

SPEED_ST: Final = Attribute("speed_st", INT, optional=True)
"""Speed at the longest straight's trap, km/h — the headline "top speed" number."""

IN_PIT: Final = Attribute("in_pit", BOOL, optional=True)
"""Whether the car is currently in the pit lane."""

PIT_OUT: Final = Attribute("pit_out", BOOL, optional=True)
"""Whether the car is on its out-lap, having just left the pits. Distinct from
:data:`IN_PIT` and worth its own column: an out-lap's times are not comparable to a green
lap's, so this is what a strategist filters on."""

RETIRED: Final = Attribute("retired", BOOL, optional=True)
"""Whether the driver has retired from the session."""

STOPPED: Final = Attribute("stopped", BOOL, optional=True)
"""Whether the car is stationary on track. Arrives *with* :data:`RETIRED` on a real
retirement, and on its own for a car that has stopped and may yet rejoin."""

PIT_COUNT: Final = Attribute("pit_count", INT, optional=True)
"""``NumberOfPitStops`` — how many stops this driver has made. The feed maintains it, so the
board never counts pit entries itself (which would double-count a drive-through)."""

TYRE_COMPOUND: Final = Attribute("tyre_compound", STR, optional=True)
"""``SOFT`` | ``MEDIUM`` | ``HARD`` | ``INTERMEDIATE`` | ``WET``, from the current
``TimingAppData`` stint. The strategy dashboard's whole colour axis."""

TYRE_AGE: Final = Attribute("tyre_age", INT, optional=True)
"""Laps on the *current set* — the stint's ``TotalLaps`` plus its ``StartLaps`` (laps the
set had already done when the stint began, non-zero for a scrubbed set). This is the x-axis
of the degradation panel, and getting ``StartLaps`` wrong is how a used-tyre stint looks
mysteriously slow for its age."""

STINT: Final = Attribute("stint", INT, optional=True)
"""1-based index of the driver's current stint — how many sets they are into the session."""

LAPS_COMPLETED: Final = Attribute("laps_completed", INT, optional=True)
"""``NumberOfLaps`` — laps this driver has finished. Its **increment is the lap-completion
trigger** (see :func:`.board.merge_timing_data`); the feed lands it on the same line as
``LastLapTime``, so one patch carries both the fact and the number."""

TRACK_STATUS: Final = Attribute("track_status", STR, optional=True)
"""The flag state the row was produced under (``AllClear``, ``Yellow``, ``SCDeployed``,
``VSCDeployed``, …) — the **broadcast SCD join**, stamped from the board's ``track`` state
onto every standings row and every lap.

The join it replaces is instructive: ``TrackStatus`` is a 585-byte feed with twelve records
per race, and correlating it to 40 000 leaderboard rows in SQL means a range join against
an interval table. Folding it into the transformer's state instead makes it a plain column,
and the flag a lap ran under becomes a ``GROUP BY`` rather than a query problem."""

# --- Weather (f1-status kind=weather) ---

AIR_TEMP: Final = Attribute("air_temp", FLOAT, optional=True)
"""Air temperature °C. Every weather field arrives as a **string** (``"30.0"``) and is
wrapped in ``float()`` — the ``FLOAT`` codec rejects both ``str`` and ``int``."""

TRACK_TEMP: Final = Attribute("track_temp", FLOAT, optional=True)
"""Track surface temperature °C — 20 °C hotter than the air, and the number that actually
decides tyre behaviour."""

HUMIDITY: Final = Attribute("humidity", FLOAT, optional=True)
"""Relative humidity %."""

PRESSURE: Final = Attribute("pressure", FLOAT, optional=True)
"""Air pressure hPa."""

RAINFALL: Final = Attribute("rainfall", FLOAT, optional=True)
"""The feed's rain indicator — ``"0"`` or ``"1"`` in practice, kept numeric rather than
boolean because the upstream never documented it as one."""

WIND_SPEED: Final = Attribute("wind_speed", FLOAT, optional=True)
"""Wind speed m/s."""

WIND_DIRECTION: Final = Attribute("wind_direction", FLOAT, optional=True)
"""Wind direction in degrees."""

# --- Session clock (f1-status kind=clock) ---

LAP: Final = Attribute("lap", INT, optional=True)
"""The current lap number (``LapCount.CurrentLap``) on a ``clock`` row; the lap a
``lap``/``pit``/``race_control`` record belongs to elsewhere. ``LapCount`` is a **race-only
feed** — practice and qualifying do not publish it, which is exactly why the wish-list is
intersected with each session's own feed index."""

TOTAL_LAPS: Final = Attribute("total_laps", INT, optional=True)
"""The scheduled race distance in laps, sent once with the first ``LapCount`` record and
then never repeated — so it has to be *folded into state*, not read off the latest patch."""

REMAINING_S: Final = Attribute("remaining_s", FLOAT, optional=True)
"""``ExtrapolatedClock.Remaining`` (``"01:59:59"``) in seconds — the session clock."""

EXTRAPOLATING: Final = Attribute("extrapolating", BOOL, optional=True)
"""Whether the session clock is running (``true``) or held (``false``, e.g. under a red
flag). Without it a paused clock looks like a stalled feed."""

# --- Lap + pit events (f1-events) ---

LAP_MS: Final = Attribute("lap_ms", INT, optional=True)
"""The completed lap's time in milliseconds."""

PITTED: Final = Attribute("pitted", BOOL, optional=True)
"""Whether the driver was in the pit lane at any point during this lap — folded across the
lap rather than sampled at its end, so both the in-lap and the out-lap are marked."""

CLEAN: Final = Attribute("clean", BOOL, optional=True)
"""Whether the whole lap ran under ``AllClear`` and outside the pits — the flag that makes a
pace comparison legitimate. Derived from :data:`TRACK_STATUS` (the worst status seen *during*
the lap, not at its end) and :data:`PITTED`."""

STATIONARY_S: Final = Attribute("stationary_s", FLOAT, optional=True)
"""``PitStopTime`` — seconds the car stood still in the box (``"2.1"``). The number a pit
crew is judged on."""

PIT_LANE_S: Final = Attribute("pit_lane_s", FLOAT, optional=True)
"""``PitLaneTime`` — seconds from pit entry to pit exit (``"21.789"``). Always ~20 s more
than :data:`STATIONARY_S`: the difference is the pit-lane speed limit, i.e. the real cost of
a stop."""

# --- Track-status periods (f1-events kind=track_period) ---

CODE: Final = Attribute("code", STR, optional=True)
"""The raw ``TrackStatus.Status`` digit as a string — ``1`` AllClear, ``2`` Yellow,
``4`` SCDeployed, ``5`` Red, ``6`` VSCDeployed, ``7`` VSCEnding. Codes 1/2/6/7 were observed
live; 4 and 5 are documented by the ecosystem and mapped defensively. An **unknown** code is
kept verbatim rather than guessed at."""

SEVERITY: Final = Attribute("severity", INT, optional=True)
"""How bad the flag is, on a total order the board can take a ``max`` over: AllClear 0 <
VSCEnding 1 < Yellow 2 < VSC 3 < SC 4 < Red 5. Ranked rather than compared by code because
the codes are not ordered (``7`` VSCEnding is *less* severe than ``6`` VSCDeployed), and the
lap tag needs "the worst thing that happened during this lap"."""

STARTED_AT: Final = Attribute("started_at", DATETIME, optional=True)
"""When this flag period began (event time)."""

ENDED_AT: Final = Attribute("ended_at", DATETIME, optional=True)
"""When it ended — **absent while it is still open**, which is how the Grafana annotation
query coalesces an open period to "now" instead of dropping it."""

# --- Race control (f1-events kind=race_control) ---

UTC: Final = Attribute("utc", DATETIME, optional=True)
"""The message's own timestamp as race control issued it. Second-precision and **zone-less**
on the wire (``2026-07-26T12:20:00``, verified UTC) — the one inner clock that is coarser
than the tape's, kept because it is upstream truth about when a decision was made."""

CATEGORY: Final = Attribute("category", STR, optional=True)
"""``Flag`` | ``Drs`` | ``SafetyCar`` | ``CarEvent`` | ``Other`` — race control's own
taxonomy."""

FLAG: Final = Attribute("flag", STR, optional=True)
"""``GREEN`` | ``YELLOW`` | ``DOUBLE YELLOW`` | ``RED`` | ``CHEQUERED`` | ``BLUE`` | … , on
``Flag``-category messages."""

SCOPE: Final = Attribute("scope", STR, optional=True)
"""``Track`` | ``Sector`` | ``Driver`` — how far the message reaches."""

SECTOR: Final = Attribute("sector", INT, optional=True)
"""The **marshalling** sector a sectored message applies to (1–20-ish) — not one of the
three timing sectors."""

MESSAGE: Final = Attribute("message", STR, optional=True)
"""The message itself, as broadcast: ``GREEN LIGHT - PIT EXIT OPEN``, ``CAR 44 (HAM) TIME
5.0 SEC PENALTY``. The ticker's whole content."""

# --- Overtakes + championship (f1-events) ---

OVERTAKES: Final = Attribute("overtakes", INT, optional=True)
"""``OvertakeSeries``' ``count`` for one recorded pass. The feed's semantics are its own
(values of ``1`` and ``21`` both occur, undocumented), so it is carried verbatim under a
clearer name and left to ad-hoc SQL — no dashboard panel claims to interpret it."""

ENTITY_TYPE: Final = Attribute("entity_type", STR, optional=True)
"""``driver`` | ``team`` — which championship table a ``championship`` row belongs to. One
table with a discriminator rather than two, because the two shapes are identical and the
progression panel wants them side by side."""

ENTITY_ID: Final = Attribute("entity_id", STR, optional=True)
"""The racing number for a driver row, the *constructor* name for a team row (``Red Bull
Racing Red Bull Ford`` — longer than :data:`TEAM`, and not joinable to it, which is why it
is kept as its own id)."""

POINTS: Final = Attribute("points", FLOAT, optional=True)
"""Championship points as they stand. ``FLOAT`` because the feed ships ``204.0`` — and
because half-points are a real F1 outcome."""

PREDICTED_POSITION: Final = Attribute("predicted_position", INT, optional=True)
"""Where the feed's own model expects this entity to end up once the session's points are
applied — the ``ChampionshipPrediction`` feed's reason for existing, updated live as the
race unfolds."""

PREDICTED_POINTS: Final = Attribute("predicted_points", FLOAT, optional=True)
"""Points including the session's projected result — see :data:`PREDICTED_POSITION`."""

# --- Telemetry (f1-telemetry) ---

RPM: Final = Attribute("rpm", INT, optional=True)
"""Engine speed, channel ``0``."""

SPEED: Final = Attribute("speed", INT, optional=True)
"""Road speed km/h, channel ``2``."""

GEAR: Final = Attribute("gear", INT, optional=True)
"""Selected gear, channel ``3`` (``0`` = neutral, up to ``8``)."""

THROTTLE: Final = Attribute("throttle", INT, optional=True)
"""Throttle application, channel ``4`` — **on the upstream's own 0–104 scale**, stored
verbatim.

Not rescaled to a percentage, because the scale is undocumented and only *looks* like one:
across a mid-race sample the value tops out at ``104`` far more often than at ``100``, so
dividing by 100 would produce 104 % throttle and dividing by 104 would silently reinterpret
every historical row. Raw ints are honest; a panel that wants a percentage can pick its own
denominator."""

BRAKE: Final = Attribute("brake", INT, optional=True)
"""Brake application, channel ``5`` — the same 0–104 scale as :data:`THROTTLE`, and in
practice nearly binary: only ``0``, ``100``, and ``104`` occur."""

X: Final = Attribute("x", INT, optional=True)
"""Track-frame X coordinate from ``Position.z``, in decimetres-ish upstream units. No
projection, no metadata, no documented origin — which is fine for the one thing it is for:
plotting cars against each other on a scatter whose axes are fixed from the session's own
min/max."""

Y: Final = Attribute("y", INT, optional=True)
"""Track-frame Y coordinate — see :data:`X`."""

Z: Final = Attribute("z", INT, optional=True)
"""Track-frame Z (elevation) — see :data:`X`."""

STATUS: Final = Attribute("status", STR, optional=True)
"""A position sample's ``Status``: ``OnTrack`` | ``OffTrack``. Also the session's own status
on a session dimension row (``Finalised``)."""

# --- Ingest state: per-feed byte cursors and the t0 anchor ---

PHASE: Final = Attribute("phase", STR, optional=True)
"""``archive`` (the recording is finished — ``ArchiveStatus.json`` says ``Complete``) or
``live`` (still being written). The two differ in exactly one respect: how a feed's
*frontier* is computed when its chunk reaches end-of-file (see
:func:`.tape.watermark`). Everything else — the cursor, the merge, the emission — is one
code path, which is the example's central claim."""

T0_MS: Final = Attribute("t0_ms", INT, optional=True)
"""The recording's start instant as epoch milliseconds — the anchor that turns
:data:`OFFSET_MS` into :data:`EVENT_TIME`.

**Persisted, and that is not incidental.** It is derived from the first ``Heartbeat`` line,
and once the merge has consumed that line it can never be re-read without rewinding the
cursor. A restart that lost ``t0`` would have to either rewind (re-emitting the whole tape)
or invent one; so it is part of the cursor, committed in the same transaction."""

CURSORS: Final = Attribute("cursors", DICT(INT), optional=True)
"""``{feed: byte offset just past the last consumed line}`` — the source position, one
integer per feed.

This is the example's first teaching point in a single field: because the source is an
append-only byte stream and an HTTP ``Range`` read is exactly reproducible, a byte offset is
a *real* resume cursor. Backfill is this at 0, live tailing is this at the end of the file,
and crash recovery is neither — it is just the same cursor, restored. Advanced only past
lines actually **emitted** (never merely fetched), so the watermark merge can hold a line
back for a later poll without losing it."""

LENGTHS: Final = Attribute("lengths", DICT(INT), optional=True)
"""``{feed: Content-Length}`` — each feed's total size, learned from the ``Content-Range``
of any ranged read. In the archive phase it is how "fully consumed" is decided
(``cursor == length``), and therefore how completion is detected."""

DONE: Final = Attribute("done", BOOL, optional=True)
"""Terminal marker: every feed is fully consumed and the archive is ``Complete``.

A ``done`` session's poll returns before issuing a single request, which is what lets a
30-session season backfill sit idle at zero network cost once it finishes. Note the state is
**kept, not tombstoned** — a falsy ``State()`` would delete the cursors, and the next poll
would cheerfully re-ingest the entire season from byte 0."""

SEEN: Final = Attribute("seen", LIST(STR), optional=True)
"""On the ``follow`` target: session paths already self-produced as config records, so each
is written exactly once. Bounded by construction — a season holds ~60 sessions, and the
record it protects is one changelog entry."""

CHECKED_MS: Final = Attribute("checked_ms", INT, optional=True)
"""On the ``follow`` target: epoch milliseconds of the last year-index fetch, so the index
is polled on its own slow cadence (:data:`~.ingest.FOLLOW_INTERVAL`) while the target itself
is visited every cycle like any other. The one place this example reads a wall clock — and
it is the ops-facing discovery loop, not the data path."""

# --- Board state: the leaderboard as keyed state ---

META: Final = Attribute("meta", DICT(ANY), optional=True)
"""The session's identity, folded from ``SessionInfo``: key, meeting, names, the computed
UTC bounds, and the display label. **The gate on every emission** — no output row exists
without a :data:`SESSION_KEY`, so records that arrive before ``SessionInfo`` (a heartbeat or
two, at most) are folded into state and emit nothing."""

DRIVERS: Final = Attribute("drivers", DICT(DICT(ANY)), optional=True)
"""``{racing_number: driver entry}`` — **the leaderboard itself**, ≤ ~22 entries.

The feed never sends a leaderboard; it sends patches (*this* driver's *this* sector). Folding
those into per-driver state and emitting wide snapshots is the second teaching point: a
materialized view built by accumulation rather than by query. Entry keys are the plain
``D_*`` strings below (the wildfire ``F_*`` idiom) — they live inside our value and can
never collide with an attribute."""

TRACK: Final = Attribute("track", DICT(ANY), optional=True)
"""The open track-status period: ``{code, label, severity, started_at}``. One dict, because
only the current flag matters — closed periods have already been emitted."""

CLOCK: Final = Attribute("clock", DICT(ANY), optional=True)
"""The folded session clock: ``{lap, total_laps, remaining_s, extrapolating}``. Folded
rather than passed through because ``LapCount`` sends ``TotalLaps`` exactly once and then
only ``CurrentLap`` deltas."""

FINALISED: Final = Attribute("finalised", BOOL, optional=True)
"""Whether the official classification has already been emitted, so ``Finalised`` followed
by ``Ends`` produces one final snapshot rather than two."""

# Raw keys inside a DRIVERS entry — plain strings read at the compute site, the wildfire
# `F_*` / odds `L_*` placement. They live inside DICT(DICT(ANY)) where they cannot collide
# with an Attribute, and they hold JSON scalars only (a State nests nothing else), so the
# board's fold never has to encode or decode them.
D_TLA: Final = "tla"
"""Three-letter abbreviation, from ``DriverList``."""
D_FULL_NAME: Final = "full_name"
"""``Lando NORRIS``."""
D_FIRST_NAME: Final = "first_name"
"""Given name."""
D_LAST_NAME: Final = "last_name"
"""Family name."""
D_TEAM: Final = "team"
"""Team name."""
D_COLOUR: Final = "colour"
"""Team livery hex, no ``#``."""
D_REFERENCE: Final = "reference"
"""The feed's stable driver key."""
D_LINE: Final = "line"
"""The feed's display row."""
D_POSITION: Final = "position"
"""Classified position, int."""
D_GAP: Final = "gap"
"""``GapToLeader`` verbatim."""
D_INTERVAL: Final = "interval"
"""``IntervalToPositionAhead.Value`` verbatim."""
D_CATCHING: Final = "catching"
"""The feed's closing-on-the-car-ahead flag."""
D_LAST_LAP: Final = "last_lap"
"""Last lap, ms."""
D_BEST_LAP: Final = "best_lap"
"""Personal best lap, ms."""
D_LAST_LAP_PB: Final = "last_lap_pb"
"""Last lap was the driver's own fastest."""
D_LAST_LAP_OB: Final = "last_lap_ob"
"""Last lap was the session's fastest."""
D_SECTORS: Final = "sectors"
"""``[s1, s2, s3]`` in ms, ``None`` where not yet set this lap — a fixed-length list because
the feed indexes sectors 0–2 and patches them one at a time."""
D_SPEEDS: Final = "speeds"
"""``{I1, I2, FL, ST}`` speed-trap readings in km/h."""
D_IN_PIT: Final = "in_pit"
"""In the pit lane now."""
D_PIT_OUT: Final = "pit_out"
"""On an out-lap."""
D_RETIRED: Final = "retired"
"""Retired from the session."""
D_STOPPED: Final = "stopped"
"""Stationary on track."""
D_PIT_COUNT: Final = "pit_count"
"""``NumberOfPitStops``."""
D_COMPOUND: Final = "compound"
"""Current tyre compound."""
D_TYRE_AGE: Final = "tyre_age"
"""Laps on the current set, including ``StartLaps``."""
D_STINT: Final = "stint"
"""1-based current stint index."""
D_LAPS: Final = "laps"
"""``NumberOfLaps`` — laps completed; its increment fires a lap event."""
D_LAP_WORST: Final = "lap_worst"
"""Worst :data:`SEVERITY` seen since the current lap began — the accumulator behind a lap's
:data:`TRACK_STATUS` tag. Reset when a lap completes, so the tag describes the lap that just
ran and not the one starting now."""
D_LAP_PITTED: Final = "lap_pitted"
"""Whether the driver has been in the pits at any point during the current lap."""

# Raw keys inside the TRACK and CLOCK dicts.
T_CODE: Final = "code"
"""The ``TrackStatus.Status`` digit."""
T_LABEL: Final = "label"
"""Its human name (``VSCDeployed``)."""
T_SEVERITY: Final = "severity"
"""Its rank on the total order (see :data:`SEVERITY`)."""
T_STARTED: Final = "started_at"
"""ISO-8601 start of the open period."""
C_LAP: Final = "lap"
"""Current lap number."""
C_TOTAL: Final = "total_laps"
"""Scheduled race distance."""
C_REMAINING: Final = "remaining_s"
"""Seconds left on the session clock."""
C_EXTRAPOLATING: Final = "extrapolating"
"""Whether that clock is running."""

# Raw keys inside the META dict — the session identity every output row is stamped with.
M_KEY: Final = "session_key"
"""The archive's numeric session id."""
M_YEAR: Final = "year"
"""The season."""
M_MEETING: Final = "meeting"
"""Weekend name."""
M_NAME: Final = "session_name"
"""Session name within the weekend."""
M_TYPE: Final = "session_type"
"""``SessionInfo.Type``."""
M_LABEL: Final = "label"
"""``"Hungarian Grand Prix — Race (2026)"``."""
M_START_LOCAL: Final = "start_local"
"""Scheduled start, local and zone-less."""
M_GMT_OFFSET: Final = "gmt_offset"
"""The track's UTC offset, ``HH:MM:SS``."""
M_START_UTC: Final = "start_utc"
"""Scheduled start as an instant, ISO-8601."""
M_END_UTC: Final = "end_utc"
"""Scheduled end as an instant, ISO-8601."""
M_CIRCUIT: Final = "circuit"
"""Circuit short name."""
M_COUNTRY: Final = "country"
"""Country name."""
M_LOCATION: Final = "location"
"""City."""
M_STATUS: Final = "status"
"""Latest ``SessionStatus.Status``."""

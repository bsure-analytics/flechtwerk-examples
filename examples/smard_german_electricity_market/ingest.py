"""SMARD ingest — an ``Extractor`` that turns each series' week files into observations,
diffing every snapshot against a rolling revision window so only *news* crosses Kafka.

Stage 1. A ``smard-series`` config record names one SMARD series (a ``filter`` id, a
``region``, a ``resolution``). Each poll:

1. GETs the series' **index** (``index_{resolution}.json``) — the list of week-file
   timestamps SMARD currently publishes;
2. picks the week file(s) whose coverage intersects the last ``REVISION_WINDOW`` (48 h)
   — normally one, two at a week boundary; the index is the only authority on which
   files exist, so a **dead series** (nuclear, ended 2024) selects nothing and the poll
   is one cheap index GET;
3. GETs each file and **diffs** its points against the ``WINDOW`` held in state: a point
   not seen before is a new observation; a point whose value changed is a *revision*
   (carrying its ``PREVIOUS_VALUE``); an unchanged point is suppressed.

**The window is the whole resume cursor.** It is the last known value for every non-null
point still inside the 48 h window — ≤192 floats for a quarter-hour series. There is no
separate high-water mark: "what's new" is exactly "what the fresh snapshot says that the
window doesn't". This is the legitimate use of extractor state as a cursor — the
counterpart to the stateless-replay sources (ADS-B) that keep none.

**The poller owns the clock, so the poller emits the punctuation.** Flechtwerk
transformers are purely event-driven — no timers. As an interval ages past the window's
trailing edge it can no longer be revised, so (for the one series flagged
``SETTLE_MARKER``) ingest emits a ``settled`` marker for it; the mix stage turns that
into the interval's final record. Event time is ``FETCHED_AT`` — the server ``Date`` of
the poll — so ``select_week_files`` / ``diff_series`` / ``aged_out`` are pure and the
logic tier drives every branch.

Data © Bundesnetzagentur | SMARD.de, CC BY 4.0.
"""
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import DATETIME

from .attributes import (
    BOOTSTRAPPED,
    FETCHED_AT,
    FILTER,
    INTERVAL_TS,
    KIND,
    PREVIOUS_VALUE,
    REGION,
    RESOLUTION,
    REVISED,
    ROLE,
    SERIES_KEY,
    SERIES_NAME,
    SETTLE_MARKER,
    SOURCE,
    UNIT,
    VALUE,
    WINDOW,
)

log = logging.getLogger(__name__)

SERIES_CONFIG_TOPIC = "smard-series"
"""Compacted config topic, one record per series to poll (keyed by
``"{filter}_{region}_{resolution}"``), seeded by ``setup.py``. Each entry is one poll
target; any producer (Kafbat included) may add or change one live."""

OBSERVATIONS_TOPIC = "smard-observations"
"""Partitioned output: observations and settled markers, both keyed by the interval
instant so a given quarter-hour's records — across every series — co-partition onto one
mix task. Time is the join key."""

BASE_URL = "https://www.smard.de/app/chart_data"
"""SMARD's public chart-data root (no auth, no key). The demo constant; the per-series
path is built from the config's ``filter`` / ``region`` / ``resolution``."""

REVISION_WINDOW = timedelta(hours=48)
"""How far back a value may still be corrected. Points inside it are kept in ``WINDOW``
and diffed each poll; as one ages past it, it can no longer change — so it *settles*."""

REVISION_MIN_DELTA = 0.5
"""Smallest change (in the series' own unit — MWh or €/MWh) that counts as a revision.
SMARD continually restates load/residual-load by sub-MWh amounts (e.g. 5091.38 → 5091.37)
as it reconciles metering — real, but jitter, not a *correction*. Below this the fresh
value is treated as unchanged (nothing emitted, the last emitted value kept), so the
corrections feed and the per-series counts show meaningful restatements, not float noise.
A first publication is always emitted regardless; only revisions are thresholded."""

_WEEK_MS = 8 * 24 * 3600 * 1000
"""Coverage slop for the open (current) week file, past a 7-day week. Only the newest
index entry is open-ended; using an 8-day upper bound keeps ``select_week_files`` from
ever missing the current week yet still excludes a long-dead series, and it is immune to
the ±1 h a DST-change week would add to a hard 7-day span."""


@dataclass(frozen=True, slots=True)
class Observation:
    """One point the diff decided is news: a first publication or a revision."""
    interval: datetime
    value: float
    revised: bool
    previous: float | None


def _from_epoch_ms(ms: int) -> datetime:
    """SMARD epoch-milliseconds → an aware UTC instant (quarter-hour boundaries are whole
    seconds, so the division is exact)."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def interval_key(interval: datetime) -> str:
    """The wire key / ``INTERVAL_TS`` string for an interval — the ``DATETIME`` codec's
    own rendering, so a message key and its ``INTERVAL_TS`` attribute coincide exactly."""
    return DATETIME.encode(interval)


def select_week_files(index_timestamps: list[int], window_start: datetime, bootstrapped: bool) -> list[int]:
    """Pick the week-file timestamps whose coverage intersects ``[window_start, ∞)`` — pure.

    Consecutive index entries bound each file's coverage: file *i* covers ``[ts[i],
    ts[i+1])``; the newest file is open-ended (bounded here by ``_WEEK_MS`` of slop).
    Before bootstrap, only the newest file is taken — the backfill starts from the
    current week and the window fills forward on the next polls. A dead series, whose
    newest file lies far in the past, selects nothing: the poll is one cheap index GET.
    """
    ts = sorted(set(index_timestamps))
    if not ts:
        return []
    if not bootstrapped:
        return [ts[-1]]
    ws_ms = int(window_start.timestamp() * 1000)
    selected = []
    for i, t in enumerate(ts):
        coverage_end = ts[i + 1] if i + 1 < len(ts) else t + _WEEK_MS
        if coverage_end > ws_ms:
            selected.append(t)
    return selected


def diff_series(
    points: list[list],
    window: dict[str, float],
    window_start: datetime,
    bootstrapped: bool,
) -> tuple[list[Observation], dict[str, float]]:
    """Diff one week file's ``[epoch_ms, value]`` points against the window — pure.

    Returns ``(observations, new_window)``. Nulls are skipped. Before bootstrap every
    non-null point is emitted (the backfill) but only in-window points are remembered.
    Once bootstrapped, only in-window points are considered: one not in the window is a
    first publication; one whose value moved by at least ``REVISION_MIN_DELTA`` is a
    revision (with its previous value); a smaller move (SMARD's sub-MWh metering jitter)
    or none at all is suppressed, keeping the last emitted value. A windowed point that
    has *disappeared* from the file is carried forward at its last value (SMARD emits no
    deletions) until it ages out.
    """
    new_window = {
        iso: value for iso, value in window.items()
        if datetime.fromisoformat(iso) >= window_start
    }
    observations: list[Observation] = []
    for ms, value in points:
        if value is None:
            continue
        interval = _from_epoch_ms(ms)
        fvalue = float(value)
        in_window = interval >= window_start
        if not bootstrapped:
            observations.append(Observation(interval, fvalue, revised=False, previous=None))
            if in_window:
                new_window[interval_key(interval)] = fvalue
            continue
        if not in_window:
            continue
        iso = interval_key(interval)
        previous = window.get(iso)
        if previous is None:
            observations.append(Observation(interval, fvalue, revised=False, previous=None))
            new_window[iso] = fvalue
        elif abs(fvalue - previous) >= REVISION_MIN_DELTA:
            observations.append(Observation(interval, fvalue, revised=True, previous=previous))
            new_window[iso] = fvalue
        # else: an immaterial restatement (SMARD sub-REVISION_MIN_DELTA jitter) — keep the
        # last emitted value (already carried into new_window) and emit nothing.
    observations.sort(key=lambda o: o.interval)
    return observations, new_window


def aged_out(window: dict[str, float], window_start: datetime) -> list[datetime]:
    """The window's intervals that fell below ``window_start`` this poll — now settled."""
    return sorted(
        interval for iso in window
        if (interval := datetime.fromisoformat(iso)) < window_start
    )


class SmardIngest(Extractor):
    """Polls each configured SMARD series and emits its new/revised points, once.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed
    in ``__aexit__``); tests inject a ``MockTransport`` client serving the fixtures."""

    config_topics = [SERIES_CONFIG_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *,
                 base_url: str = BASE_URL, observations_topic: str = OBSERVATIONS_TOPIC) -> None:
        super().__init__()
        self._client = client
        self._base_url = base_url
        self._topic = observations_topic

    async def __aenter__(self) -> "SmardIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(  # pragma: no cover — live path
                timeout=httpx.Timeout(60.0), follow_redirects=True,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    def _index_url(self, config: Config) -> str:
        return f"{self._base_url}/{config[FILTER]}/{config[REGION]}/index_{config[RESOLUTION]}.json"

    def _file_url(self, config: Config, file_ts: int) -> str:
        f, r, res = config[FILTER], config[REGION], config[RESOLUTION]
        return f"{self._base_url}/{f}/{r}/{f}_{r}_{res}_{file_ts}.json"

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Emit this series' new/revised points and (for the marker series) its settled
        intervals, then advance the window — all in one transaction.

        Observations are yielded first, settled markers next, and the ``State`` last, so
        the whole poll commits atomically: on a crash the window is unadvanced and the
        re-poll re-derives exactly the same news (nothing is double-emitted, because a
        value already in the restored window is suppressed).
        """
        assert self._client is not None, "client is opened in __aenter__ or injected"
        index_response = await self._client.get(self._index_url(config))
        index_response.raise_for_status()
        fetched_at = self._fetched_at(index_response)
        window_start = fetched_at - REVISION_WINDOW

        window: dict[str, float] = state.get(WINDOW) or {}
        bootstrapped = bool(state.get(BOOTSTRAPPED))
        settled = aged_out(window, window_start) if config.get(SETTLE_MARKER) else []

        files = select_week_files(index_response.json()["timestamps"], window_start, bootstrapped)
        observations: list[Observation] = []
        new_window = window
        for file_ts in files:
            file_response = await self._client.get(self._file_url(config, file_ts))
            if file_response.status_code == 404:
                continue  # a file the index just announced but the CDN hasn't served yet — next poll
            file_response.raise_for_status()
            file_obs, new_window = diff_series(
                file_response.json()["series"], new_window, window_start, bootstrapped)
            observations += file_obs

        identity = self._identity(config)
        series_key = identity[SERIES_KEY]
        for obs in observations:
            yield Message(key=interval_key(obs.interval), topic=self._topic,
                          value=self._observation(identity, obs, fetched_at))
        for interval in settled:
            yield Message(key=interval_key(interval), topic=self._topic,
                          value=Event({KIND: "settled", SERIES_KEY: series_key,
                                       INTERVAL_TS: interval, FETCHED_AT: fetched_at}))
        log.info("%s: %d observation(s), %d settled (fetched_at %s)",
                 series_key, len(observations), len(settled), fetched_at.isoformat())
        yield State({BOOTSTRAPPED: True, WINDOW: new_window})

    @staticmethod
    def _fetched_at(response: httpx.Response) -> datetime:
        """The server ``Date`` as aware UTC — the event-time clock. Server-controlled, so
        tests pin it via a ``Date`` response header; falls back to now if it is absent."""
        raw = response.headers.get("Date")
        if raw:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        return datetime.now(timezone.utc)  # pragma: no cover — live feed always sends Date

    @staticmethod
    def _identity(config: Config) -> Event:
        """The per-series identity fields every observation carries (built once per poll)."""
        identity = Event({
            SERIES_KEY: f"{config[FILTER]}_{config[REGION]}_{config[RESOLUTION]}",
            SERIES_NAME: config[SERIES_NAME],
            ROLE: config[ROLE],
            UNIT: config[UNIT],
        })
        if (source := config.get(SOURCE)) is not None:
            identity[SOURCE] = source
        return identity

    @staticmethod
    def _observation(identity: Event, obs: Observation, fetched_at: datetime) -> Event:
        """Project one diffed point into an observation record (identity + the point)."""
        record = Event({
            **identity,
            KIND: "observation",
            INTERVAL_TS: obs.interval,
            VALUE: obs.value,
            REVISED: obs.revised,
            FETCHED_AT: fetched_at,
        })
        if obs.previous is not None:
            record[PREVIOUS_VALUE] = obs.previous
        return record


stage = SmardIngest()
"""The stage the dispatcher runs (``python -m examples.smard_german_electricity_market ingest``)."""

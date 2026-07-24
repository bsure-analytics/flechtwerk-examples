# PLAN — Odds Arbitrage Radar (Polymarket × Kalshi)

A new example for this repo: poll the same real-world event's binary markets on two
prediction-market venues — **Polymarket** and **Kalshi** — normalize both order books
into one quote stream, and detect **cross-venue arbitrage**: moments where buying YES on
one venue and NO on the other costs less than the guaranteed $1 payout. Emit a continuous
*margin* stream ("distance to free money") for the dashboard and a sparse *signal* stream
when a real (fee-adjusted) arb appears.

This plan was researched and written on **2026-07-23** against live APIs and the repo at
commit `d821e6a` (flechtwerk pinned to **0.7.4**). Everything in §2 was verified live on
that date; everything in §3 must be re-verified by the implementer before writing code,
because sports markets die within days and APIs drift.

---

## 1. Why this example earns its place (the theme)

Each example teaches a framework pattern the others don't (see the root README's table).
This one adds two genuinely new ones:

1. **N-source fan-in to one keyed state.** Two *independent extractor processes* consume
   the **same config topic** (`arb-pairs`) and produce normalized quotes to the **same
   partitioned topic** (`arb-quotes`), keyed by pair — so both venues' quotes for a pair
   co-partition onto one radar task, which merges them into a per-pair
   best-price state and recomputes a derived condition on every update. smard/gtfs join
   two *streams*; this merges N *sources* into a materialized "watchdog" view.
2. **Event-time staleness.** A 5-minute-old quote must not fabricate an arb. The radar
   compares the two legs' embedded `fetched_at` event times (never wall clock — the
   framework has no timers; the pollers own the clock, as smard's settle markers teach)
   and demotes margins computed against an aged leg to `fresh = false`, which suppresses
   signals. Plus the full state lifecycle: a venue reporting the market closed
   tombstones the pair's join state (falsy `State()`).

Secondary teaching points: binary-contract arb math is *symmetric* (one contract per leg —
no bookie-style stake splitting), and **gross vs. net**: fees routinely eat a visible gross
edge, which the live worked example in §2.4 shows beautifully.

Everything is keyless public data — the repo's convention (adsb.lol, gtfs.de, SMARD,
GDELT all advertise "no auth, no key") is preserved.

---

## 2. Verified facts (live-checked 2026-07-23 — trust but re-probe, see §3)

### 2.1 Polymarket — two endpoints, both keyless

**Gamma (market discovery/metadata):** `GET https://gamma-api.polymarket.com/markets?slug=<slug>`
→ JSON **array** of market objects. Relevant fields observed live:

| field | type/shape | notes |
|---|---|---|
| `question` | string | human title — stamp onto quotes |
| `outcomes` | **string** containing a JSON array | e.g. `"[\"Minnesota Twins\", \"Cleveland Guardians\"]"` — must `json.loads` it |
| `clobTokenIds` | **string** containing a JSON array | one CLOB token id per outcome, same order — must `json.loads` it |
| `active`, `closed`, `archived`, `acceptingOrders` | bool | status normalization inputs |
| `negRisk` | bool | multi-outcome events — **out of scope**, reject in `request.py` |
| `orderPriceMinTickSize`, `orderMinSize` | number | informational only |
| `outcomePrices`, `bestBid`, `bestAsk`, `spread` | string/number | midpoint-ish; semantics under-documented — **do not use for arb math**; use CLOB books |
| `makerBaseFee`, `takerBaseFee` | int (e.g. `1000`) | **units undocumented** — do not use; fee rates come from config (§2.3) |
| `gameStartTime`, `sportsMarketType`, `events[].title` | misc | sports metadata, informational |

Discovery for the README: `GET https://gamma-api.polymarket.com/events?tag_slug=mlb&active=true&closed=false`
lists live events with nested markets (verified working).

**CLOB (order books):** `GET https://clob.polymarket.com/book?token_id=<id>` → keyless:

```json
{"market": "0x…", "asset_id": "<token id>", "timestamp": "1784836362061",
 "bids": [{"price": "0.01", "size": "171706.22"}, …],
 "asks": [{"price": "…", "size": "…"}, …]}
```

Prices/sizes are **strings**. Array order is not guaranteed sorted the way you want:
compute **best bid = max(bid prices)**, **best ask = min(ask prices)**, and take the size
at that level. Either side may be empty (one-sided book — observed live). Also verified:
`GET /price?token_id=…&side=buy` → best bid, `side=sell` → best ask (numbers confirmed
bid < ask live), but `/book` gives sizes too, so use `/book`.

Per binary market there are **two tokens** (one per outcome), each with its own book:
2 book GETs + 1 gamma GET = 3 GETs per pair per poll.

### 2.2 Kalshi — one endpoint, keyless

`GET https://api.elections.kalshi.com/trade-api/v2/markets/<ticker>` → `{"market": {…}}`.
No auth headers needed (verified live). Public rate limit reported ~30 req/s — a 30 s
poll over a handful of pairs is orders of magnitude below it. Relevant fields observed:

| field | example | notes |
|---|---|---|
| `title` | `"Minnesota vs Cleveland Winner?"` | stamp onto quotes |
| `status` | `"active"` | anything else → normalize to `closed` |
| `yes_bid_dollars` / `yes_ask_dollars` | `"0.7500"` / `"0.7600"` | **strings**, `float()` them |
| `no_bid_dollars` / `no_ask_dollars` | `"0.2400"` / `"0.2500"` | present directly; note it's the complement: `no_ask = 1 − yes_bid`, `no_bid = 1 − yes_ask` (verified live) |
| `yes_bid_size_fp` / `yes_ask_size_fp` | `"58258.01"` / `"1339.60"` | **NO-side sizes are crosswise**: size at `no_ask` = `yes_bid_size_fp`, size at `no_bid` = `yes_ask_size_fp` (single book, two views) |
| `close_time`, `expected_expiration_time`, `rules_primary` | ISO / text | informational |

The market object's top-of-book suffices; the (also keyless) `/orderbook` endpoint is not
needed. Discovery for the README: `GET …/trade-api/v2/markets?limit=…&status=open` and
`GET …/trade-api/v2/events?series_ticker=KXMLBGAME&status=open` (re-verify the exact
events-endpoint params — only `/markets` was probed live).

### 2.3 Fees — same parabolic formula on both venues, different rate

- **Kalshi** (fee schedule, July 2026): `fees = roundup(M × 0.07 × C × P × (1−P))`,
  M=1 default, roundup to the cent. Max 1.75 ¢/contract at P=0.50. Maker fees exist on
  some series but takers are what the radar models.
- **Polymarket** (docs.polymarket.com): `fee = C × rate × P × (1−P)`, **taker-only**;
  rate by category: crypto 0.07, **sports 0.05**, economics/culture/weather 0.05,
  finance/politics 0.04, **geopolitics 0.00**.

Model both as the smooth per-contract formula `fee(p) = rate × p × (1−p)` and document
Kalshi's per-order rounding as a caveat. The **rate rides on each quote** (stamped by the
venue's extractor: module-constant default, config-record override) so the radar stays a
pure function of its inputs.

### 2.4 Live worked example (capture for the README — the pair will be dead by impl time)

MIN@CLE MLB game, 2026-07-23, observed **in-game** ~19:50–20:00 UTC:

- Kalshi `KXMLBGAME-26JUL231310MINCLE-CLE` ("Cleveland wins"): yes_ask **0.76**.
- Polymarket `mlb-min-cle-2026-07-23`, outcome "Minnesota Twins" (= Cleveland NO):
  best ask **0.22** (moved to 0.30 within ~10 min — in-game prices move fast; this is
  exactly the divergence the radar watches).
- Gross: 0.76 + 0.22 = **0.98** → 2 ¢ guaranteed gross edge per contract pair.
- Fees: Kalshi 0.07×0.76×0.24 ≈ 1.28 ¢; Polymarket 0.05×0.22×0.78 ≈ 0.86 ¢ → ≈ 2.1 ¢.
- Net: **−0.1 ¢** — fees ate it. That's the gross-vs-net lesson, live.

### 2.5 Repo conventions confirmed (templates to mirror)

- **Best template overall: `examples/smard_german_electricity_market/`** (newest, closest
  shape). Extractor idiom = `ingest.py` (subclass `Extractor`, `config_topics = […]`,
  `async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]`,
  httpx client owned in `__aenter__`/`__aexit__` and injectable for tests, `_fetched_at`
  from the server `Date` header). Transformer idiom = `mix.py` (pure
  `run_*(state, msg)` async generator wrapped by `@transformer(input_topics=[…])`,
  tombstone via `yield State()`).
- **Stateless snapshot pollers yield no `State` at all** (see memory
  `flechtwerk-extractor-no-cursor-needed`; adsb ingest is the exemplar). Both extractors
  here are pure snapshot polls → **no State yields, ever**.
- **Attributes**: normally "declare only what you compute with", but when the stages
  *construct* every record (no foreign schema), declare the whole schema — smard's
  `attributes.py` documents this rationale verbatim; same situation here.
- **ClickHouse via the Kafka-engine shortcut** (smard/adsb/gtfs style, *no* sink stage —
  that pattern is already taught by `clickhouse_sink` and the GDELT sink): per topic, a
  `Kafka`-engine queue table (`kafka_broker_list = 'kafka:19092'`,
  `kafka_format = 'JSONAsObject'`, group `arb-*-clickhouse`) + a target table + a
  materialized view that promotes dashboard columns and keeps the whole message in a
  `payload JSON` catch-all. The stack already configures read-committed for the Kafka
  engine (clickhouse/config/kafka.xml) — required, EOS producers upstream.
- **setup.py idiom** = smard's: `create_topics()` (idempotent, 8 partitions, RF 1,
  config topic `cleanup.policy=compact`) + `apply_clickhouse_schema()` (strips `--`
  comments, splits on `;`, posts each statement; reused by the integration test).
  **Nothing is seeded** here (the adsb pattern): pairs come from `request.py`.
- **request script idiom** = `adsb_flight_tracker/request.py` (plain `AIOKafkaProducer`
  write to the compacted config topic under `quiet_fresh_topic_produce_race()`).
- **`__main__.py` idiom** = smard's: `dispatch({stage: lambda: run(…)})`; demo constants
  (ports, poll interval) live there; `run()` handles logging/uvloop/metrics labels.
- **Integration tier** = `smard_mix_integration_test.py`: real broker via the root
  `conftest.py` fixtures (`kafka_bootstrap`, `clickhouse`), transformer run via
  `Flechtwerk.of(...)` + `asyncio.create_task(app.run())`, read back `read_committed`;
  second test applies `clickhouse.sql` and asserts the objects exist in `system.tables`.
- **Ports 9101–9116 are allocated** (prometheus/prometheus.yml) → this example takes
  **9117 (polymarket), 9118 (kalshi), 9119 (radar)**.
- Root README has a numbered example table (smard is row 7) → this becomes **row 8**,
  and the README's port-allocation paragraph (~line 109) and CLAUDE.md's naming/port
  sections need the same additions.

---

## 3. Phase 0 — re-verify before writing code (assumption ledger)

Sports markets from §2.4 will be resolved. Re-run, with today's equivalents:

1. `curl 'https://gamma-api.polymarket.com/events?tag_slug=mlb&active=true&closed=false&limit=2'`
   — pick a live game; confirm the field names in §2.1 (especially that `outcomes` /
   `clobTokenIds` are still JSON-in-string) and grab that game's slug.
2. `curl 'https://clob.polymarket.com/book?token_id=<from step 1>'` — confirm shape,
   string prices, unordered levels.
3. `curl 'https://api.elections.kalshi.com/trade-api/v2/markets?limit=5&status=open'` and
   `…/markets/<ticker>` — confirm the `*_dollars` / `*_size_fp` fields and that no auth
   header is required. Find the Kalshi ticker for the same game (series `KXMLBGAME`,
   ticker grammar `KXMLBGAME-<yy><MON><dd><hhmm><AWAY><HOME>-<WINNER>`; verify by GET).
4. Confirm both HTTP responses carry a `Date` header (event-time source, smard-style).
5. Fee rates: re-check https://docs.polymarket.com/polymarket-learn/trading/fees and the
   Kalshi fee schedule (https://kalshi.com/docs/kalshi-fee-schedule.pdf — it 429'd once;
   the formula in §2.3 was cross-confirmed via secondary sources).
6. Against the **pinned flechtwerk 0.7.4 source** (github.com/bsure-analytics/flechtwerk
   at tag 0.7.4 — study `flechtwerk/extractor.py`, `flechtwerk/testing`, and
   `tests/integration/`):
   a. Two extractor processes with different `application_id`s may consume the **same
      config topic** independently (each gets its own consumer group). Expected yes;
      confirm.
   b. What a **null-value (tombstone) config record** does to a poll target — `request.py
      retire` (§5.7) depends on it removing the target. If 0.7.4 ignores tombstones,
      drop the `retire` subcommand and document "overwrite the record via Kafbat" instead.
   c. Exact names of the shipped testing doubles (`FakeKafkaConsumer`, `FakeKafkaProducer`,
      `InMemoryStateStore`, `make_record`, `RecordingObserver`, `poll_one`,
      `process_batch`) — mirror smard's `runner_test.py` wiring rather than guessing.
7. Ports 9117–9119 still unallocated in `prometheus/prometheus.yml`.

Explicitly **assumed, not verified** (design around, don't fight):
- Gamma's `bestBid`/`bestAsk`/`takerBaseFee` semantics — unused by design.
- Kalshi maker fees, per-order fee rounding — modeled smoothly, documented as caveat.
- Cross-venue **resolution-criteria mismatch** (the classic arb trap: the two venues may
  settle the "same" event differently, e.g. postponement rules). Not solvable in code —
  it is *why* pairs are user-curated. Must be a prominent README caveat.

---

## 4. Design

### 4.1 Naming (per CLAUDE.md rules)

| what | value |
|---|---|
| key | `arb` |
| display title | **Odds Arbitrage Radar** (README H1: `Odds Arbitrage Radar — Polymarket × Kalshi`) |
| folder | `examples/odds_arbitrage_radar/` |
| topics | `arb-pairs` (compacted config), `arb-quotes`, `arb-margins`, `arb-signals` — 8 partitions each |
| ClickHouse | `arb_quotes`, `arb_margins`, `arb_signals` (+ `_queue` / `_mv` companions) |
| dashboard | `grafana/dashboards/odds-arbitrage-radar.json`, uid `flechtwerk-arb`, title `Flechtwerk — Odds Arbitrage Radar` |
| poe | `setup-arb`, `request-pair`, `run-arb-polymarket`, `run-arb-kalshi`, `run-arb-radar`, `run-arb`, quickstart `arb` |
| metrics ports | 9117 polymarket (extractor), 9118 kalshi (extractor), 9119 radar (transformer) |
| application/client ids | `arb-polymarket`/`arb-polymarket-0`, `arb-kalshi`/`arb-kalshi-0`, `arb-radar`/`arb-radar-0` |
| Prometheus `example` label | `odds_arbitrage_radar` |

### 4.2 Topology

```
                       arb-pairs (compacted config; user-curated via request.py)
                          │                       │
            ┌─────────────┴──────────┐  ┌─────────┴────────────┐
            │ PolymarketQuotes       │  │ KalshiQuotes         │   two independent
            │ (Extractor, 30 s poll) │  │ (Extractor, 30 s)    │   snapshot pollers,
            │ gamma + 2 CLOB books   │  │ 1 market GET         │   NO cursor state
            └─────────────┬──────────┘  └─────────┬────────────┘
                          └──────► arb-quotes ◄───┘        keyed by pair → co-partition
                                       │
                          ┌────────────┴────────────┐
                          │ radar (Transformer)     │  per-pair state {venue: last quote}
                          │ merge → margins both    │  event-time staleness; tombstone on
                          │ directions → signal?    │  closed
                          └───────┬─────────┬───────┘
                            arb-margins   arb-signals
                                  │             │
                       ClickHouse Kafka engine + MVs (arb-quotes lands too)
                                  │
                          Grafana flechtwerk-arb
```

### 4.3 Config record (`arb-pairs`, wire key = the Polymarket slug)

| attribute | codec | req? | meaning |
|---|---|---|---|
| `polymarket_slug` | STR | ✓ | gamma market slug (also the wire key — smard also duplicates key fields into the value) |
| `kalshi_ticker` | STR | ✓ | e.g. `KXMLBGAME-26JUL231310MINCLE-CLE` |
| `yes_outcome` | STR | ✓ | the Polymarket outcome string that corresponds to Kalshi's YES side (e.g. `"Cleveland Guardians"`) — the entity-resolution the user curates |
| `polymarket_fee_rate` | FLOAT, optional | | default `0.05` (module constant in `polymarket.py`; sports rate — README notes 0.00–0.07 by category) |
| `kalshi_fee_rate` | FLOAT, optional | | default `0.07` (module constant in `kalshi.py`) |

The polymarket extractor reads slug/yes_outcome/its fee override; the kalshi extractor
reads ticker/its fee override. Neither needs the other's fields.

### 4.4 Quote record (`arb-quotes`, key = pair key, one per venue per poll)

`KIND="quote"`, `PAIR_KEY` STR, `VENUE` STR (`polymarket`|`kalshi`), `TITLE` STR,
`STATUS` STR (`active`|`closed`), `YES_BID`/`YES_ASK`/`NO_BID`/`NO_ASK` FLOAT optional
(absent on a one-sided/empty book — never a fabricated 0), `YES_ASK_SIZE`/`NO_ASK_SIZE`
FLOAT optional (contracts at best ask; Kalshi NO sizes crosswise per §2.2), `FEE_RATE`
FLOAT, `FETCHED_AT` DATETIME (server `Date`).

Quotes are emitted **every poll even if unchanged** (heartbeat: drives the staleness
logic, the freshness panel, and flat-line charts) — the deliberate opposite of smard's
diff-then-emit, called out in the README. Volume is trivial (2 records/pair/30 s).

**Status normalization** — polymarket: `closed or archived or not active or not
acceptingOrders` → `closed`; kalshi: `status != "active"` → `closed`. Closed markets keep
emitting `closed` quotes each poll (radar no-ops once tombstoned; self-healing, no
extractor state needed) until the user retires the pair (§5.7).

### 4.5 The arb math (module `arbitrage.py`, pure — this is the logic tier's playground)

Binary contracts pay $1. Buy 1 YES on venue A at ask `a` and 1 NO on venue B at ask `b`:
exactly one leg pays $1 whatever happens (assuming identical resolution — the README
caveat). **One contract per leg — no stake splitting** (unlike decimal-odds bookies).

For each direction `d` ∈ {polymarket YES + kalshi NO, kalshi YES + polymarket NO},
computable only when both asks are present:

```
fee(p, rate)   = rate × p × (1 − p)                     # per contract, both venues (§2.3)
gross_edge(d)  = 1 − (yes_askᴬ + no_askᴮ)
fees(d)        = fee(yes_askᴬ, rateᴬ) + fee(no_askᴮ, rateᴮ)
net_edge(d)    = gross_edge(d) − fees(d)                # per $1 payout
executable(d)  = min(yes_ask_sizeᴬ, no_ask_sizeᴮ)       # None if either size absent
```

Also a `best_of_book(levels) -> (price, size) | None` helper for the Polymarket books
(max over bids / min over asks; None on empty side).

### 4.6 Radar transformer (`radar.py`)

Module constants: `STALE_AFTER = timedelta(minutes=5)` (10 missed polls), `MIN_EDGE = 0.0`.
State: `LEGS = DICT(DICT(ANY))` — `{venue: quote-entry}` (like smard's `CONTRIBUTIONS`).

`run_radar(state, msg)` (pure, I/O-free; wrapped `@transformer(input_topics=[QUOTES_TOPIC])`):

1. `status == "closed"`: if state exists → `yield State()` (tombstone; the other venue's
   subsequent active quotes rebuild a one-legged state that computes nothing and dies on
   its own close — bounded, document it); if no state → no-op (marker replay, smard-style).
2. Active quote: merge into `LEGS[venue]`; `yield State({LEGS: legs})` always.
3. If both venues present: `fresh = (trigger.FETCHED_AT − other.FETCHED_AT) <= STALE_AFTER`
   (signed comparison — a *newer* other leg, possible with two producers racing, is
   trivially fresh). For each computable direction, emit to `arb-margins`:
   `PAIR_KEY, TITLE, DIRECTION` (`polymarket_yes+kalshi_no` | `kalshi_yes+polymarket_no`),
   `YES_ASK, NO_ASK, GROSS_EDGE, FEES, NET_EDGE, EXECUTABLE_SIZE?, FRESH` BOOL,
   `COMPUTED_AT` DATETIME (= trigger `FETCHED_AT` — event time, replay-deterministic).
4. If `fresh` and `net_edge > MIN_EDGE`: emit the same record to `arb-signals` too. 🎉

### 4.7 Extractors

Both mirror smard's `SmardIngest` skeleton (client in `__aenter__`, injectable;
`config_topics = [PAIRS_TOPIC]`; `_fetched_at` from the `Date` header) but **yield only
Messages — never State** (stateless snapshot; the no-cursor rule). Let it crash: a
missing slug/ticker (typo, or gamma returning `[]`) raises — the supervisor restarts,
the error stays loud, and `request.py` validation (§5.7) makes it rare.

- **`polymarket.py` / `PolymarketQuotes`**: GET gamma by slug → `market = resp[0]`;
  `json.loads` the `outcomes` + `clobTokenIds` strings; `yes_idx =
  outcomes.index(config[YES_OUTCOME])` (ValueError = loud config error); GET both books;
  normalize via `best_of_book`; one quote Message. Pure projection function
  `normalize_polymarket(market_json, yes_book, no_book, fee_rate) -> Event` for tier 1.
- **`kalshi.py` / `KalshiQuotes`**: GET market by ticker; `float()` the `*_dollars`
  strings; crosswise NO sizes (§2.2); one quote Message. Pure
  `normalize_kalshi(market_json, fee_rate) -> Event`.

Poll interval **30 s** (constant in `__main__.py`): ≤4 GETs/pair/poll, far below both
venues' public limits, fast enough to catch in-game divergence.

---

## 5. Files to create (all under `examples/odds_arbitrage_radar/` unless noted)

Mirror the smard example file-for-file; study each named template before writing.

| file | template | contents |
|---|---|---|
| 5.1 `__init__.py` | smard's | empty/docstring |
| 5.2 `attributes.py` | smard's | every attribute from §4.3/§4.4/§4.6 with smard-quality docstrings; include the "we own the whole schema" rationale note; `LEGS` state attr; raw-key constants for inside `LEGS` entries |
| 5.3 `arbitrage.py` | (new, pure) | `fee()`, `best_of_book()`, a `Direction`/margin dataclass, `compute_margins(pm_quote, kalshi_quote) -> list[…]` — no framework imports beyond none; fully covered by tier 1 |
| 5.4 `polymarket.py` | smard `ingest.py` | §4.7; module docstring explains the two-endpoint dance and why gamma's own best-bid/ask fields are not trusted |
| 5.5 `kalshi.py` | smard `ingest.py` | §4.7; docstring explains the complement book and crosswise sizes |
| 5.6 `radar.py` | smard `mix.py` | §4.6; docstring carries the staleness + lifecycle story |
| 5.7 `request.py` | adsb `request.py` | `uv run poe request-pair <polymarket-slug> <kalshi-ticker> "<yes outcome>"` → **validates before writing**: GETs both venues (httpx, sync-ish via asyncio like adsb), rejects `negRisk` or `len(outcomes) != 2` or unknown `yes_outcome`, prints both top-of-books and the computed edges via `arbitrage.py`, **warns loudly if a resting net edge > 3 %** (almost certainly a flipped `yes_outcome` mapping), then writes the config record keyed by slug. Subcommand `retire <slug>` writes a null-value tombstone (pending §3.6b) |
| 5.8 `setup.py` | smard's | topics per §4.1 (config compacted) + `apply_clickhouse_schema()`; seeds **nothing** (adsb pattern); final print points at `request-pair` |
| 5.9 `__main__.py` | smard's | `dispatch({"polymarket": …, "kalshi": …, "radar": …})` with the ids/ports from §4.1 and `POLL_INTERVAL = timedelta(seconds=30)` for both extractors |
| 5.10 `clickhouse.sql` | smard's | three queue+table+MV triples, smard's header comment style (Kafka-engine shortcut, JSONAsObject, read-committed note): **`arb_quotes`** MergeTree `ORDER BY (pair_key, venue, fetched_at)` TTL 30 days — promoted: pair_key, venue LowCardinality, title, status LowCardinality, the four prices + two sizes as `Nullable(Float64)` (absent ≠ 0 — the smard rule), fee_rate, fetched_at `DateTime64(3,'UTC')`, payload JSON; **`arb_margins`** MergeTree `ORDER BY (pair_key, direction, computed_at)` TTL 30 days — pair_key, title, direction LowCardinality, yes_ask, no_ask, gross_edge, fees, net_edge, executable_size Nullable, fresh UInt8, computed_at, payload; **`arb_signals`** same columns, `ORDER BY (computed_at, pair_key)`, **no TTL** (rare + precious) |
| 5.11 `README.md` | smard's | H1 `Odds Arbitrage Radar — Polymarket × Kalshi`; mermaid diagram (§4.2); the theme (§1); the math incl. one-contract-per-leg symmetry (§4.5); the §2.4 worked example verbatim (dated); how to find + request pairs (browse polymarket.com/kalshi.com; the two discovery curls from §2.1/§2.2; a full `request-pair` invocation); **caveats**: resolution-criteria mismatch (why pairs are user-curated), fee model smoothness vs Kalshi rounding, top-of-book only (depth ignored beyond executable size), quotes-as-heartbeat vs smard's diffing; **responsible-use note**: read-only public data, no order placement, venue availability varies by jurisdiction; extension points: negRisk intra-venue arb, The Odds API bring-your-own-key bookie mode, WebSocket push (cite the framework's deferred push-source direction) |
| 5.12 `tests/…` | §6 | logic_test.py, runner_test.py, fixtures/, integration/arb_radar_integration_test.py |

**Repo-level edits:**

| file | change |
|---|---|
| `pyproject.toml` | the seven poe tasks (§4.1), placed after the smard block; `run-arb` is the standard 3-stage supervisor shell (copy `run-smard`'s, stages `polymarket kalshi radar`, echo tag `[run-arb]`); quickstart `arb = sequence ["setup-arb", "run-arb"]`; `request-pair = python -m examples.odds_arbitrage_radar.request` (poe passes trailing args through — `request-region` proves it) |
| `prometheus/prometheus.yml` | three targets 9117/9118/9119, `example: odds_arbitrage_radar`, stages `extractor`/`extractor`/`transformer`, with a comment line matching the neighbors' style |
| `grafana/dashboards/odds-arbitrage-radar.json` | copy smard's JSON as the structural template (datasource uid, templating, refresh). Panels: (1) **net edge over time** per (pair, direction) — timeseries of `arb_margins`, zero-line threshold, signals as annotations; (2) **"closest to free money"** leaderboard — table, latest fresh net_edge per pair/direction via `argMax`; (3) gross vs net for a selected pair (template var); (4) venue YES-ask overlay per pair (the divergence view, from `arb_quotes`); (5) signals table from `arb_signals`; (6) quote freshness (now() − max(fetched_at) per venue/pair). All queries `FROM flechtwerk.arb_*` |
| `CLAUDE.md` | add `arb` to the keys list + title to the naming paragraph; extend the metrics-port allocation (`9117` arb polymarket + `9118` arb kalshi + `9119` arb radar); one-line example description where siblings have one |
| root `README.md` | row **8** in the example table (theme wording: "N-source fan-in + event-time staleness: two extractors sharing one config topic, merged into a per-pair best-price state that flags cross-venue arbitrage — live Polymarket × Kalshi prediction-market quotes"); extend the port list (~line 109) |

---

## 6. Tests (three tiers, smard's as the model)

### 6.1 `tests/logic_test.py` — pure, no framework

Drive `arbitrage.py` and both `normalize_*` functions directly:
- `best_of_book`: unordered levels → max-bid/min-ask with correct sizes; empty side → None.
- `normalize_polymarket`: real trimmed gamma+book fixtures → correct YES/NO mapping for
  `yes_outcome` at index 0 **and** index 1; JSON-in-string fields decoded; one-sided book
  → absent attrs; closed/acceptingOrders=false → `status=closed`; unknown `yes_outcome`
  raises.
- `normalize_kalshi`: dollar-strings → floats; **crosswise NO sizes**; complement holds
  (`no_ask == 1 − yes_bid` on the fixture); non-active status → closed.
- `fee`: 0.07×0.5×0.5 = 0.0175 (Kalshi's documented max); 0 at p∈{0,1}.
- `compute_margins`: both directions; §2.4's numbers reproduced (0.76/0.22 → gross 0.02,
  net < 0); a contrived true arb (e.g. 0.70 + 0.25 with fees) → net > 0; missing ask →
  that direction absent; executable = min of sizes, None when a size is absent.
- `run_radar` (driven as a bare async generator with hand-built `State`/messages, the
  smard `run_mix` style): first leg → state only, no margin; second leg → two margin
  messages + state; stale other leg → `FRESH=false`, **no** signal; fresh arb → signal on
  the signals topic; closed with state → lone `State()` tombstone; closed without state
  → nothing; out-of-order (other leg newer) → fresh.

### 6.2 `tests/runner_test.py` — shipped fakes only (Docker-free)

Mirror smard's wiring exactly (fixture that builds the runner over
`FakeKafkaConsumer`/`FakeKafkaProducer`/`InMemoryStateStore`, per §3.6c):
- Each extractor over `httpx.MockTransport` serving the fixtures → `poll_one` →
  `producer.sent` holds one quote keyed by the pair, values matching the normalization.
- Gamma `[]` / Kalshi 404 → the poll raises (let-it-crash, visible in the runner).
- Radar over the fakes: feed venue-A quote then venue-B quote through `process_batch` →
  margins land on `arb-margins`; contrive an arb → record on `arb-signals`; feed a closed
  quote → the state store's key is tombstoned.

### 6.3 `tests/integration/arb_radar_integration_test.py` — testcontainers

Two tests, mirroring `smard_mix_integration_test.py`:
- `test_radar_join_over_the_broker`: create the topics; produce a Polymarket-shaped and a
  Kalshi-shaped quote for one pair (values contrived to a fresh net arb) with the same
  key; run the real `radar` via `Flechtwerk.of`; assert a margin **and** a signal read
  back `read_committed`, with the expected `net_edge`.
- `test_clickhouse_schema_applies`: `apply_clickhouse_schema()` against the `clickhouse`
  fixture; assert all 9 objects (3 queues, 3 tables, 3 MVs) in `system.tables`.

### 6.4 `tests/fixtures/`

Trimmed **real** captures (gtfs's `make_fixtures.py` provenance pattern): one gamma
market response, its two `/book` responses, one Kalshi market response — captured in
Phase 0, trimmed to the used fields plus a few extras (to prove tolerance of unknowns).

---

## 7. Live verification protocol (the proof, per the pinning rule)

1. `uv run poe test` → all Docker-free tiers green; `uv run poe test-all` → integration green.
2. `uv run poe up` → stack healthy. `uv run poe arb` (quickstart) → three stages start,
   idle politely with zero pairs (expected — README says so).
3. Find a live pair (Phase-0 discovery curls; any MLB/NBA game listed on both venues
   today). `uv run poe request-pair <slug> <ticker> "<yes outcome>"` → validation output
   shows both books + current edges, config record lands (check Kafbat, :8080).
4. Within ~60 s: quotes on `arb-quotes`, margins on `arb-margins` (Kafbat); rows in
   ClickHouse (`SELECT * FROM flechtwerk.arb_margins ORDER BY computed_at DESC LIMIT 10`
   via :8123); the Grafana board (:3000) plots the margin series. Signals appear only if
   the market gods smile — the margin panels must be self-sufficiently interesting.
5. Kill one extractor mid-run (Ctrl-C the stage, restart) → no duplicates, radar state
   intact (spot-check margins continuity) — the let-it-crash story.
6. Prometheus targets 9117–9119 up (:9090/targets); the shared Observability dashboard
   shows the three stages.
7. Deliberately request a pair with a flipped `yes_outcome` → request.py's >3 % warning
   fires → retire it (`request.py retire <slug>` or Kafbat tombstone per §3.6b).

---

## 8. Out of scope (documented as README extension points, not built)

- **Order placement / trading of any kind** — the radar reads public data, full stop.
- **Automatic pair discovery** (fuzzy title matching across venues) — the resolution-
  criteria trap is exactly why pairs are user-curated config records.
- **negRisk / multi-outcome intra-venue arb** (sum of asks across mutually exclusive
  outcomes < 1) — a natural follow-up, needs no second venue.
- **The Odds API bookie mode** (real bookmakers, free API key, 500 credits/mo) — would
  break the repo's keyless convention; note it as a bring-your-own-key variant.
- **WebSocket push** (both venues have WS feeds) — belongs to the framework's deferred
  push-source direction; the 30 s poll is the point for now.
- Order-book depth beyond top-of-book (only `executable_size` acknowledges depth).

## 9. References

- Polymarket Gamma/CLOB (keyless reads confirmed live): https://gamma-api.polymarket.com,
  https://clob.polymarket.com, rate limits https://docs.polymarket.com/api-reference/rate-limits,
  fees https://docs.polymarket.com/polymarket-learn/trading/fees
- Kalshi API v2 (keyless market data confirmed live): https://api.elections.kalshi.com/trade-api/v2,
  quickstart https://docs.kalshi.com/getting_started/quick_start_market_data,
  fee schedule https://kalshi.com/docs/kalshi-fee-schedule.pdf
- Repo templates: `examples/smard_german_electricity_market/` (all files),
  `examples/adsb_flight_tracker/request.py`, root `conftest.py`, `prometheus/prometheus.yml`.

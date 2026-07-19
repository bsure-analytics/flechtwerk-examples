# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this repo is

Complete, runnable examples for [Flechtwerk](https://github.com/bsure-analytics/flechtwerk)
(PyPI `flechtwerk`). It **complements** the framework's own docs: the main repo
keeps minimal quickstart snippets (CI-tested via testcontainers); this repo
carries full scenarios with real infrastructure, **pinned to a released PyPI
version and upgraded deliberately**, and doubles as an integration test of the
published package the way a consumer uses it.

Examples must read as if the framework's authors wrote them: study the pinned
framework source (`github.com/bsure-analytics/flechtwerk` at the pinned tag —
especially `flechtwerk.testing` and `tests/integration/`) and reuse its idioms
rather than inventing parallel ones.

## Commands

```bash
uv sync                     # venv + pinned dependencies (Python 3.14)
uv run poe up               # start the shared stack, wait until healthy
uv run poe down             # stop the stack, delete its volumes
uv run poe test             # unit tiers (pure-logic + runner-fake) — Docker-free
uv run poe test-integration # integration tier — testcontainers (needs Docker)
uv run poe test-all         # every tier (what CI runs)
uv run poe cov              # every tier + coverage report
uv run poe setup-<example>  # create topics / seed config / apply schema
uv run poe run-<example>    # run one example against the shared stack
uv run poe <example>        # quickstart: setup + run in one command, for the
                            # self-contained examples (adsb, chaos, fermentation);
                            # clickhouse_sink has none — it consumes example 1's output
```

## The shared stack

One `docker-compose.yaml` at the repo root — six long-running services plus a
one-shot `kafka-init`, no profiles, no override files. Ports: Kafka `9092` (host) / `kafka:19092` (in-network),
Kafbat UI `8080`, Mosquitto `1883`, ClickHouse `8123` HTTP + `9000` native,
Prometheus `9090`, Grafana `3000` (anonymous). ClickHouse holds all example
output in the `flechtwerk` database. Kafka persists across restarts (the
`kafka-init` one-shot `chown`s the volume to the broker's uid). Prometheus
scrapes host-run stages via `host.docker.internal:<port>`. Grafana provisions
datasources + dashboards under `grafana/`: a per-example dashboard for the
examples that ship one (adsb ships two — `adsb-flight-tracker` and
`adsb-aviation-events` — plus fermentation), and the shared `framework-metrics`
and `stream-data`.

Stages run **on the host** (`uv run poe run-<example>`) and connect to
`localhost` ports; the stack is only the infrastructure.

## Pinning rule (deliberate, not automatic)

- `flechtwerk` is pinned to an **exact** version in `pyproject.toml`
  (`flechtwerk[mqtt]==X.Y.Z`), never a path/git dependency, with the full
  resolution in `uv.lock`. `requires-python = "==3.14.*"` — one version, not a
  range (the framework supports 3.12+; the examples pin the current release).
- Docker images are pinned to **specific** tags (no `:latest`).
- To upgrade: bump the pin, `uv lock`, bump image tags, then re-verify — the
  tests and a live end-to-end pass are the proof.

## The three test tiers

Every example ships tests in three tiers mirroring the framework's own suite.
Unit tiers (1 + 2) must run **Docker-free**.

1. **Pure logic** — no framework, no mocks. A stage's core is a plain async
   generator; build a `State`, drive it, collect the yielded `Message`/`State`,
   assert. Factor the pure logic out of any I/O (HTTP/DB) so this tier can drive
   it directly — it is the two-yield contract's biggest payoff. File:
   `tests/logic_test.py`.
2. **Runner tier** — the shipped `flechtwerk.testing` doubles only
   (`FakeKafkaConsumer`/`FakeKafkaProducer`, `InMemoryStateStore`,
   `FakeMqttConnection`/`make_mqtt_message`, `make_record`, `RecordingObserver`).
   Wire the real `ExtractorRunner`/`TransformerRunner` (or `_FlechtwerkModule`)
   over those fakes and drive `poll_one` / `process_batch`, asserting on
   `producer.sent` and the state store. Stub any external client (HTTP via
   `httpx.MockTransport`; a DB client via a small app-level fake — that is not
   "parallel scaffolding", which means reinventing the framework's own fakes).
   File: `tests/runner_test.py`.
3. **Integration** — testcontainers (ephemeral Kafka / Mosquitto / ClickHouse),
   marked `pytest.mark.integration`, run with `-m integration`. Session-scoped
   container fixtures live in the repo-root `conftest.py` (`kafka_bootstrap`,
   `clickhouse`, `mosquitto`, `unique_*`). Files under `tests/integration/`.

## Example layout

Each example is a package under `examples/<name>/` — self-contained, with one
deliberate exception: `clickhouse_sink` consumes example 1's output topic and
imports its typed attributes (`examples.adsb_flight_tracker.attributes`) rather
than redeclaring the wire schema, so the two can't drift:

```
examples/
  _runner.py                        # shared: run(stage, ...) + dispatch({name: thunk})
  _setup.py                         # shared ops helpers for setup.py (quiet_fresh_topic_produce_race)
  <name>/
    __init__.py  README.md  <stage>.py …  [attributes.py]  clickhouse.sql  __main__.py  setup.py
    tests/{logic_test.py, runner_test.py, integration/<topic>_integration_test.py}
```

Each `<stage>.py` exports a module-level `stage` (an `Extractor`/`Transformer`);
`__main__.py` is a thin dispatcher that maps a stage name to
`examples._runner.run(stage, ...)` with that stage's demo constants, run via
`python -m examples.<name> <stage>` (a single-stage example may omit the name).
`examples/_runner.py` is the one copy of the logging + `Flechtwerk.of(...).run()`
boilerplate — don't reinvent it per example. `chaos_harness` is the deliberate
exception: its `__main__` reads env vars (the harness spawns fenced copies) and
runs metrics-off, but still calls `_runner.run(...)`. `examples/_setup.py` is the
setup-time twin: shared ops helpers each `setup.py` imports (e.g.
`quiet_fresh_topic_produce_race`, which silences aiokafka's guaranteed-transient
`NotLeaderForPartitionError` when seeding a just-created topic — the controller
names a leader before the broker finishes becoming one, so the first produce
retries once; metadata-level waiting can't close that window).

Its Grafana dashboard, when it ships one, lives under `grafana/dashboards/` with
a hyphenated name (e.g. `adsb-flight-tracker.json`); its poe targets are
`setup-<name>` / `run-<name>`; its host metrics port follows the allocation in
`prometheus/prometheus.yml` (`9101` adsb ingest + `9105` adsb enrich + `9106` adsb
conflict + `9107` adsb boundary loader, `9102` sink, `9103` fermentation monitor +
`9104` fermentation bridge; the chaos harness runs metrics-off — its rapid SIGKILL
restarts would race to rebind a scrape port). The ADS-B example is a three-stage
data pipeline (ingest extractor → enrich transformer → conflict transformer) plus a
companion **boundary-loader extractor** (`boundaries.py`, `CountryLoader`) — four host
processes. Reverse geocoding is **staged and traffic-driven** over a stack of ClickHouse
`POLYGON` dictionaries (no Nominatim/PostGIS on the reverse path): the loader downloads a
global ADM0 **world map** at startup (`__aenter__`, Natural Earth admin-0 — geoBoundaries'
own global ADM0/CGAZ is ~400 MB, too heavy), and enrich detects each aircraft's country
against it, writing that ISO-3 to the compacted `adsb.countries` topic; the loader consumes
those as its poll targets and downloads **all** admin levels that country publishes
(geoBoundaries ADM1…ADM5) into one `region_adm{n}_dict` each (all from the single
`region_boundaries` table filtered by level), just-in-time. enrich `dictGet`s every level
for a point and concatenates the hits into a hierarchical label (`Le Bourget; Marne; Grand
Est`) — one dict per level because a polygon dict returns only the finest containing
polygon. **Nothing is seeded** — `setup.py` only creates topics + schema; a user requests a
poll region with `uv run poe request-region "<name>"` (→ `request.py` → `adsb.regions`),
and forward geocoding of that name→centre uses public Nominatim (`ingest`). An extractor
takes one config record per poll target, keyed on a compacted config topic (any producer,
Kafbat included, works too); a transformer consumes a partitioned input topic instead.

## Conventions carried from the framework (keep these)

- **No environment-variable magic inside stages.** All configuration is injected
  by the caller (`Flechtwerk.of(...)`, or a config topic record). `setup.py` /
  `__main__.py` are the ops callers and may hold demo constants.
- **`metrics_labels` must be non-empty** when `metrics_port > 0`: the framework's
  `PrometheusObserver` calls `.labels(**metrics_labels)` on every metric, so `{}`
  crashes at startup. Pass at least one label (e.g. `{"client_id": client_id}`).
  Metrics are named `flechtwerk_*`; the `example` label the dashboards filter on
  is added by the Prometheus scrape config, so don't also set it in the app.
- **`client_id`** is the process identity — unique per instance, stable across
  restarts; it anchors transactional-producer fencing and the MQTT session.
- Typed attributes at the JSON edge (`Config`/`Event`/`State` + `Attribute`);
  codecs are exact-type (`INT` rejects `bool`, `FLOAT` rejects `int` — wrap with
  `float()`). Required attributes reject `None`; use `optional=True` or omit the
  key. Yielding a falsy `State()` tombstones the key. `Record.wrap(raw)` for
  wire JSON, `Record({ATTR: v})` for typed literals.
- All framework consumers run `read_committed`; downstream consumers of any EOS
  output must too.
- **"Let it crash":** no in-process retry for transient errors — let a timeout /
  5xx propagate; the orchestrator restarts and state restores from the changelog.
  Only catch what you can actually remedy.

## Explicitly rejected (hard constraints — do not reintroduce without asking)

- **TimescaleDB** — bad experience at scale.
- **Druid in the default stack** — too heavy for a demo (may return later as an
  optional profile).
- **Postgres as a second store** — ClickHouse covers it; YAGNI.
- **DuckDB as a live sink** — in-process, single-writer, wrong shape (fine for a
  historical/analytical angle only).
- **Examples living in the main repo** — weight, issue-tracker noise, silent rot
  vs. this repo's deliberate version pinning.

## License

MIT, same as the framework.

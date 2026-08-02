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

Task runner: poe. `uv run poe --help` lists every target with its help text.

## The shared stack

One `docker-compose.yaml` at the repo root — six long-running services plus two
one-shots (`kafka-init` and the ELR-downgrading `kafka-features`), no profiles,
no override files. Ports: Kafka `9092` (host) / `kafka:19092` (in-network),
Kafbat UI `8080`, Mosquitto `1883`, ClickHouse `8123` HTTP + `9000` native,
Prometheus `9090`, Grafana `3000` (anonymous). ClickHouse holds all example
output in the **one** `flechtwerk` database (created by
`clickhouse/init/01-init.sql`) — deliberately not the built-in `default`, and
deliberately not one database per example. `default` is the unqualified-
resolution target every client lands in without asking, so app tables there mix
with ad-hoc scratch and can't be dropped as a unit; a named DB is the "this
stack's data" boundary and matches how a real deployment looks. One DB (not
per-app) because the demo presents a **single output surface** — one Grafana
datasource, one `SHOW TABLES FROM flechtwerk` — and because the `<pipeline>_`
table prefix already tracks *the pipeline the data belongs to, not the process
that wrote it*: the `clickhouse_sink` example writes `adsb_positions`, so
per-app DBs would force it to either reach into another app's DB (`adsb.*`) or
mislabel the data's pipeline (`sink.*`). The per-app-teardown upside never pays
off here anyway — `setup-<key>` is `CREATE TABLE IF NOT EXISTS` and the only
reset is `poe clean` dropping the whole volume. Every SQL reference is
**fully-qualified** (`flechtwerk.<table>`) rather than relying on a session
default: `setup.py` applies `clickhouse.sql` one statement per HTTP request (a
`USE` wouldn't carry across requests), and materialized views / polygon
dictionaries bind their DB at creation time — qualification keeps the schema
file self-contained and re-runnable. Kafka persists across restarts (the
`kafka-init` one-shot `chown`s the volume to the Kafka broker's uid). Prometheus
scrapes host-run stages via `host.docker.internal:<port>`. Grafana provisions
datasources + dashboards under `grafana/`: a per-example dashboard for the
examples that ship one (adsb ships two — `adsb-flight-tracker` and
`adsb-aviation-events` — and f1 ships three — `f1-live-timing`, `f1-strategy`
and `f1-season` — plus fermentation), and the shared `observability`
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
than redeclaring the wire schema, so the two can't drift.

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
names a leader before the Kafka broker finishes becoming one, so the first produce
retries once; metadata-level waiting can't close that window).

**Naming**: every example has one **key** — its Kafka prefix (`adsb`, `gdelt`,
`gtfs`, `smard`, `fermentation`, `chaos`, `odds`, `wildfire`, `f1`; the sink's ops key is `sink`) — and one
**display title** (`ADS-B Flight Tracker`, `GTFS German Rail Delays`, …). The two are
**not independent**: the key is the *first token of the folder*, i.e. of
snake_case(title) — `odds_arbitrage_radar` → `odds`, `wildfire_watch` → `wildfire`. Every
example obeys this (the sink is the lone exception, ops key `sink`), so don't invent a key
that doesn't appear in the title: a synonym (`fires` for `wildfire_watch`) leaves the repo
with two competing prefixes for one example and every derived name has to pick a side.
Everything else derives from those two: topics and consumer groups are
`<key>-*`; ClickHouse tables are `<key>_*`, prefixed by the pipeline the data
belongs to (which is why the sink writes `adsb_positions`); the folder is
snake_case(title); the Grafana dashboard file is kebab-case(title) under
`grafana/dashboards/`, its title `Flechtwerk — <Title>`, its uid
`flechtwerk-<key>` (a secondary dashboard suffixes it: `flechtwerk-adsb-events`);
poe targets are `setup-<key>` / `run-<key>[-<stage>]` / quickstart `<key>`; the
Prometheus `example` label is the folder name. A README H1 is the display
title, optionally followed by an em-dash tagline (`Chaos Harness — an
Exactly-Once Proof`). An example's host metrics port follows the allocation in
`prometheus/prometheus.yml`; the chaos harness runs metrics-off, because its rapid
SIGKILL restarts would race to rebind a scrape port.

An extractor
takes one config record per poll target, keyed on a compacted config topic (any producer,
Kafbat included, works too); a transformer consumes a partitioned input topic instead.

The four examples with load-bearing design rationale carry it in their own
`examples/<name>/CLAUDE.md`, which loads only when you work under that directory:
`adsb_flight_tracker` (staged polygon-dictionary reverse geocoding),
`odds_arbitrage_radar` (N-source fan-in), `wildfire_watch` (spatiotemporal
sessionization, the one example needing an API key) and `f1_live_timing`
(the append-only-file cursor).

## Conventions carried from the framework (keep these)

- **No environment-variable magic inside stages.** All configuration is injected
  by the caller (`Flechtwerk.of(...)`, or a config topic record). `setup.py` /
  `__main__.py` are the ops callers and may hold demo constants. Two `__main__.py`s
  deliberately read the environment *as the ops caller* and inject what they find:
  `chaos_harness` (an env-driven `application_id`, to prove transactional fencing) and
  `wildfire_watch` (the `FIRMS_MAP_KEY` credential NASA requires, injected as
  `FirmsIngest(map_key=…)`). The rule holds where it matters — no stage touches
  `os.environ`. `wildfire_watch/ingest.py` therefore has **no module-level `stage`**
  singleton, since a credential can't be baked in at import time.
- **`metrics_labels` must be non-empty** when `metrics_port > 0`: the framework's
  `PrometheusObserver` calls `.labels(**metrics_labels)` on every metric, so `{}`
  crashes at startup. Pass at least one label (e.g. `{"client_id": client_id}`).
  Metrics are named `flechtwerk_*`; the `example` and `stage`
  (`extractor`/`transformer`) labels the dashboards filter on are added by the
  Prometheus scrape config, so don't also set them in the app.
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
  historical/analytical angle only). Same verdict for **chDB** (embedded
  ClickHouse).
- **StarRocks / Apache Doris** — the closest real competitors (MySQL protocol,
  StarRocks even ships an `allin1` image), but no equivalent of ClickHouse's
  polygon dictionaries, which the ADS-B reverse-geocoding path is built on;
  Doris also wants separate FE/BE processes.
- **QuestDB and time-series engines (InfluxDB 3, VictoriaMetrics, GreptimeDB,
  TDengine)** — time-series-shaped, not general OLAP: weak JSON, no polygon
  support, weaker joins. Fine for fermentation/smard-style data, fails
  adsb/gdelt.
- **Apache Pinot** — Druid's sibling, rejected for the same reason: multi-
  component + Zookeeper, far too heavy for a demo stack.
- **Streaming databases (RisingWave, Materialize, Timeplus/Proton, Feldera)** —
  they do the stream transformation themselves, competing with the thing the
  examples exist to showcase. Flechtwerk is the streaming layer; the store
  should just store.
- **Elasticsearch / OpenSearch** — JVM-heavy document store, query DSL instead
  of first-class SQL; wrong idiom for the analytics-store role.
- **CrateDB** — decent geo + Postgres protocol, but fading ecosystem and a
  Lucene-based engine that's slower for OLAP scans.
- **Rockset / Tinybird** — SaaS-only (Rockset shut down after the OpenAI
  acquisition); not self-hostable infrastructure.
- **Examples living in the main repo** — weight, issue-tracker noise, silent rot
  vs. this repo's deliberate version pinning.

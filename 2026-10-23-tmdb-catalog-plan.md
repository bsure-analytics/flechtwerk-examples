---
branch: main
closeout_push: false
created: 2026-10-23
executor:
  effort: high
  model: claude-sonnet-5-5
  permission_mode: auto
permalink: none
plan_file: 2026-10-23-tmdb-catalog-plan.md
planner:
  effort: xhigh
  model: claude-fable-5-1
  permission_mode: auto
plugin: pdca
plugin_url: https://github.com/christian-schlichtherle/cs7-claude-plugins
plugin_version: 0.16.2
review_rounds: 10
sources: []
status: handed-off
ticket: none
---

# Add the TMDB Catalog example

## Goal

After this work the repo has an eleventh example, `examples/tmdb_catalog` (display title
**TMDB Catalog**, key `tmdb`), that keeps a local copy of The Movie Database's three entity
catalogs — movies, TV series and people, plus the credits that link them — current from TMDB's
daily change feeds, starting from an epoch carried on the config topic (default: the last 90
days), lands versioned rows in ClickHouse for ad-hoc analysis, and is the repo's first example
to carry its credential **encrypted on the config topic** through `flechtwerk[secrets]`. The
Kafka output topics are flat JSON, one row per entity version and one per credit, so a
downstream OLAP store (the user will read them into Druid) can take them without a flatten step.
It teaches two shapes no example teaches yet: *reference data as a change stream* (slowly
changing dimensions behind a content-hash version gate, with a calendar-day cursor and a
seen-set for the open day) and the framework's keyring facility in a real ops flow (keyring file
→ `request-tmdb token` encrypts → stages decrypt on read, with the secret counters on the
metrics endpoint).

## Requirements

No ticket and no spec were named: this section is dropped. Every decision below was agreed in
the planning conversation and is recorded in Constraints & Non-Goals.

## Verified Context

Every fact carries the command that established it on 2026-10-23, in this repository, under
permission mode `auto` (this session's mode, read from its transcript: `"permissionMode":"auto"`).
Paths under `.venv/` are the installed framework.

**Repository and framework**

- Pin is `flechtwerk[mqtt]==0.17.0`, Python 3.14.7, no `flechtctl` installed:
  `grep -n 'flechtwerk\[' pyproject.toml` → line 6; `uv run python --version`;
  `uv run flechtctl --version` → `Failed to spawn: flechtctl`.
- `flechtwerk[mqtt,secrets]==0.17.0` resolves and locks: the extra pins `flechtctl==0.17.0`
  (`uv run python -c "import importlib.metadata as m; print([r for r in m.requires('flechtwerk') if 'secrets' in r])"`),
  and a spike (`sed -i '' 's/flechtwerk\[mqtt\]==0.17.0/flechtwerk[mqtt,secrets]==0.17.0/' pyproject.toml && uv lock && uv sync`)
  added `flechtctl 0.17.0`, `joserfc 1.7.7`, `pycparser 3.12` and made `uv run flechtctl --version`
  print `flechtctl 0.17.0` and `from flechtwerk.secrets import ENCRYPTED, Keyring, encrypt_value, install_keyring`
  import. The spike was reverted (`git checkout pyproject.toml uv.lock && uv sync`); the tree is clean.
- The secrets API, verified by a snippet under `uv run --with flechtctl==0.17.0 python`:
  `Keyring.from_json(Path(...).read_text())` loads a keyring file; under
  `flechtwerk.testing.installed_keyring()` a typed write `Config({FEED: "movie", ACCESS_TOKEN: "secret-123", SINCE: date(2026, 7, 25)}).raw`
  stores `access_token` as a `flenc:jwe:…` string and `since` as `2026-07-25`, and
  `Config.wrap(raw)[ACCESS_TOKEN]` returns `secret-123`; `encrypt_value(ACCESS_TOKEN, "x")` returns
  a `flenc:jwe:` token; `DATE.encode(date(2026, 10, 1))` is `2026-10-01`. `ENCRYPTED` lives in
  `flechtwerk.secrets` (not in `flechtwerk.attribute`): `.venv/lib/python3.14/site-packages/flechtwerk/attribute/codec.py:22`.
- `Flechtwerk.of(..., keyring=)` installs the process keyring at stage startup
  (`.venv/lib/python3.14/site-packages/flechtwerk/module.py:320,742`); `_runner.run` has no
  `keyring` parameter today (`grep -n keyring examples/_runner.py` prints nothing).
- `flechtwerk.keyring` imports without the extra (it is the joserfc-free seam):
  `.venv/.../flechtwerk/keyring.py` module docstring.
- The config store merges **every** config topic a stage declares into one namespace, and the
  extractor runner polls every active entry: `.venv/.../flechtwerk/configs.py:67-80`,
  `.venv/.../flechtwerk/extractor.py:637-660`. So a credentials record on a second config topic
  would be polled as a target — the token therefore rides on each feed record.
- A transformer with `config_topics` looks records up via `self.configs.get(key)`:
  `examples/gdelt_news_stories/stories.py:308,322`; the runner-tier idiom for seeding it is in
  `examples/gdelt_news_stories/tests/runner_test.py` (grep `ConfigStore`).
- Spread idiom: `Event({**Record.wrap(flat), MEDIA_TYPE: "movie", VERSION: 1}).raw` keeps nested
  lists, dicts and `None` from `flat` and adds the typed fields (`uv run python -c` snippet).
- Secret metrics: `flechtwerk_secret_decrypts_total{scope,kid}` and
  `flechtwerk_keyring_keys_loaded{kid}`: `.venv/.../flechtwerk/metrics.py:319-340`.
- Prometheus ports allocated up to 9123: `grep -o 'host.docker.internal:9[0-9]*' prometheus/prometheus.yml | sort | tail -1`.
- Grafana provisions every JSON file in `grafana/dashboards` (`grafana/provisioning/dashboards/dashboards.yaml`,
  `allowUiUpdates: false`); the ClickHouse datasource has uid `clickhouse`, user `default`, default
  database `flechtwerk` (`grafana/provisioning/datasources/datasources.yaml`). The wildfire
  dashboard is the template: `jq -c '{uid,title,tags,schemaVersion}' grafana/dashboards/wildfire-watch.json`
  → `{"uid":"flechtwerk-wildfire","title":"Flechtwerk — Wildfire Watch","tags":["flechtwerk"],"schemaVersion":39}`.
- Baseline tests: `uv run poe test` → `421 passed, 20 deselected in 0.65s`;
  `uv run pytest -m integration examples/wildfire_watch -q` → `2 passed, 130 deselected in 12.11s`
  (Docker 29.9.2, testcontainers pulls fine under `auto`).
- Docker: `docker info --format '{{.ServerVersion}}'` → `29.9.2`; no containers running
  (`docker ps` empty), so ports 9092/8080/1883/8123/9000/9090/3000 are free;
  `docker compose up --detach --wait --dry-run` runs clean under `auto`; with the stack down,
  `curl -s -m 3 'http://localhost:8123/?query=SELECT%201'` exits 7 (connection refused — the
  command shape is allowed, the server is simply not up).
- ClickHouse 25.8 JSON casts, proved in a throwaway `clickhouse/clickhouse-server:25.8` container
  (`docker run --rm -e CLICKHOUSE_USER=flechtwerk -e CLICKHOUSE_PASSWORD=flechtwerk …`, removed
  afterwards): from a `message JSON` column, `message.genres::Array(String)` → `['Drama','Thriller']`;
  an absent field `::Array(String)` → `[]`; `::Nullable(String)` of an absent or `null` field → NULL;
  `parseDateTime64BestEffort(message.version_at::String, 3)` parses `2026-10-23T10:00:00+00:00`;
  `message.certifications.US::String` → `R`; an `INSERT … SELECT` of those casts into a
  `ReplacingMergeTree(version_at)` with a `payload JSON` column works and `payload.title` reads
  back. **`message.certifications::Map(String,String)` returns `{}` silently** — never promote a
  JSON object to a `Map` column; read maps as `payload.<map>.<key>` subcolumns instead.
- Numeric and Bool casts, same container, from a row holding `budget`, `revenue: 0`,
  `popularity`, `vote_average`, `vote_count`, `adult: false`, `in_production: true`,
  `runtime: null`, `gender`, `number_of_seasons`, `video`: `::UInt64`, `::UInt32`, `::UInt16`,
  `::UInt8`, `::Float64` and `::Bool` of **present** fields return the value; a **null** field as
  `::Nullable(UInt32)` is NULL; an **absent or null** field cast to a non-nullable `::UInt64`,
  `::Float64` or `::Bool` yields `0` / `0` / `false` — no error; `ifNull(message.absent::Nullable(UInt8), 0)`
  yields `0`; an integer JSON value cast `::Float64` is fine; a float cast `::UInt32` truncates
  (`12.3456` → `12`), so never cast `popularity` or `vote_average` to an integer type.
- The TMDB logo URL used by the dashboard's attribution panel answers
  `HTTP/2 200`, `content-type: image/svg+xml`:
  `curl -sI https://www.themoviedb.org/assets/2/v4/logos/v2/blue_short-8e7b30f73a4020692ccca9c88bafe5dcb6f8a62a4c6bc55cd9ba82bb2cd95f6c.svg | head -3`.
- `kill <pid>` and `pkill -f <pattern>` on a background process run under `auto`.

**Secrets on this machine (created in the planning session, owned by the user)**

- Keyring `~/.config/flechtwerk-examples/keyring.json`, mode 600, one `oct` key, kid `dev-2026-10`,
  primary: created by `uvx flechtctl keyring new --kid dev-2026-10 ~/.config/flechtwerk-examples/keyring.json`;
  `uvx flechtctl keyring show ~/.config/flechtwerk-examples/keyring.json` → `dev-2026-10  oct  private  scopes=-  roles=primary`;
  `uvx flechtctl encrypt -k … --string hello | uvx flechtctl decrypt -k … --string` → `hello`.
  `flechtctl encrypt -k PATH --string VALUE` reads VALUE from stdin when omitted.
- Token `~/.config/flechtwerk-examples/tmdb-token`, mode 600, one line (the TMDB *API Read Access
  Token*, a JWT): `curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(tr -d '[:space:]' < ~/.config/flechtwerk-examples/tmdb-token)" https://api.themoviedb.org/3/configuration`
  → `200`. Without a token the same URL returns 401 with body
  `{"status_code":7,"status_message":"Invalid API key: You must be granted a valid key.","success":false}`.

**TMDB API (all calls with the bearer header; numbers measured 2026-10-23)**

- Change feeds `GET /3/{movie|tv|person}/changes?start_date=D&end_date=E&page=N` return
  `{"results":[{"id":…,"adult":…,"softcore":…}],"page":N,"total_pages":…,"total_results":…}`.
  `softcore` is undocumented (so the projection spreads unknown fields, never enumerates them);
  `adult` is sometimes `null`. The window is **inclusive at both ends**: `[2026-10-01, 2026-10-01]`
  = 5394 results, `[2026-10-02, 2026-10-02]` = 4853, `[2026-10-01, 2026-10-02]` = 9635 (ids changed
  on both days are listed once per window). A window longer than 14 days answers
  `{"status_code":20,"status_message":"Invalid date range: Should be a range no longer than 14 days."}`;
  page 501 answers `{"status_code":22,"status_message":"Invalid page: Pages start at 1 and max at 500. …"}`;
  pages hold 100 ids, are stable between calls and carry no duplicate ids across pages 1–3;
  today's open day is queryable (`[2026-10-23, 2026-10-23]` = 3349 so far); a future day and an
  inverted range return `total_results: 0`; without dates the default is the past 24 h.
- Volume: 2026-10-22 had 8730 changed movies (88 pages), 2829 TV series, 5895 people. A 14-day
  people window is 599 pages — **over the 500-page cap**, which is why the extractor lists one
  calendar day per window. Sum over the last 90 days, by 14-day windows: movies 389 575, TV
  111 290, people 386 669 *id-days* (not unique ids).
- Details: `GET /3/movie/{id}?append_to_response=credits,keywords,release_dates,external_ids`
  (movie 550: 87 KB; `keywords.keywords[].name`; `release_dates.results[].{iso_3166_1, release_dates[].{certification, type, release_date}}`;
  `credits.cast[]` has `credit_id, id, name, gender, known_for_department, popularity, character, order`;
  `credits.crew[]` has `credit_id, id, name, gender, known_for_department, popularity, department, job`;
  `external_ids` has `imdb_id, wikidata_id, facebook_id, instagram_id, twitter_id`; top level has
  `belongs_to_collection` (object or null), `genres[]/{id,name}`, `production_companies[]/{id,name,origin_country,logo_path}`,
  `production_countries[]/{iso_3166_1,name}`, `spoken_languages[]/{iso_639_1,name,english_name}`,
  `origin_country[]` of strings, plus scalars `adult, backdrop_path, budget, homepage, id, imdb_id, original_language, original_title, overview, popularity, poster_path, release_date, revenue, runtime, softcore, status, tagline, title, video, vote_average, vote_count`).
  `GET /3/tv/{id}?append_to_response=aggregate_credits,keywords,content_ratings,external_ids`
  (tv 87108 *Chernobyl*: 129 KB, 90 aggregate cast, 295 aggregate crew; `keywords.results[].name`
  — note the different key from movies; `aggregate_credits.cast[]` has `roles[]/{credit_id, character, episode_count}`
  and `order`, `aggregate_credits.crew[]` has `jobs[]/{credit_id, job, episode_count}` and `department`;
  `content_ratings.results[]/{iso_3166_1, rating}`; `networks[]/{id,name}`, `created_by[]/{id,name}`,
  `seasons[]`, `last_episode_to_air`, `next_episode_to_air` objects; scalars include
  `first_air_date, last_air_date, in_production, number_of_episodes, number_of_seasons, type, status, name, original_name`,
  and `external_ids` adds `tvdb_id`). `GET /3/person/{id}?append_to_response=external_ids`
  (person 287: 3 KB; keys `adult, also_known_as, biography, birthday, deathday, external_ids, gender, homepage, id, imdb_id, known_for_department, name, place_of_birth, popularity, profile_path`).
- An unknown id on any of the three answers HTTP 404
  `{"success":false,"status_code":34,"status_message":"The resource you requested could not be found."}`.
- Latency: ten sequential movie detail fetches took 2.5 s (≈0.25 s each).
- **The listing day is the UTC calendar day.** For 40 ids taken from page 1 and page 60 of the
  `[2026-10-22, 2026-10-22]` movie list, every per-entity change item
  (`GET /3/movie/{id}/changes?start_date=2026-10-22&end_date=2026-10-23`, 127 items in all) is
  stamped `… UTC` and dated `2026-10-22`, the earliest at `2026-10-22 00:00:03 UTC` and the
  latest at `2026-10-22 23:59:59 UTC` — a non-UTC boundary would have spilled items into the
  neighbouring dates. So `clock().date()` in UTC is the right notion of *today*. (That
  per-entity endpoint takes an **exclusive** end date — `[D, D]` returns `{"changes":[]}` —
  unlike the global list; the plan does not use it, but a README sentence comparing the two
  must get this right.)
- The fixture page `/3/movie/changes?start_date=2026-10-01&end_date=2026-10-01&page=1` (a closed
  day, so a re-capture returns the same entries) has 100 entries, every one carrying a
  `softcore` key, one with `adult: null`, none with `adult: true`, ids unique but **not sorted**
  (`jq -c '{n:(.results|length), with_softcore:([.results[]|select(has("softcore"))]|length), adult_null:([.results[]|select(.adult==null)]|length), adult_true:([.results[]|select(.adult==true)]|length), unique:(.results|map(.id)|unique|length), ascending:((.results|map(.id)) == (.results|map(.id)|sort))}'`
  → `{"n":100,"with_softcore":100,"adult_null":1,"adult_true":0,"unique":100,"ascending":false}`).
- TMDB's rate limit is a soft ceiling of roughly 40 requests per second; honour HTTP 429
  (https://developer.themoviedb.org/docs/rate-limiting). Terms: non-commercial use, attribution
  with the TMDB logo and the sentence "This product uses TMDB and the TMDB APIs but is not
  endorsed, certified, or otherwise approved by TMDB.", and **no caching longer than six months**
  (https://www.themoviedb.org/api-terms-of-use).

## Constraints & Non-Goals

- **Mirror `examples/wildfire_watch`** for layout, docstring voice, test harnesses and README
  structure; reuse the shipped `flechtwerk.testing` doubles; add no dependency beyond the
  `secrets` extra. Read `CLAUDE.md` at the repo root before writing a line — its naming rule,
  the three test tiers and the "no environment-variable magic inside stages" rule all apply.
- **Stages read no environment.** Only the ops callers (`__main__.py`, `request.py`) read
  `FLECHTWERK_KEYRING`, through one shared helper; the token never appears in argv, logs or
  output — `request-tmdb token` takes it from stdin.
- **No `Map` columns in ClickHouse** (verified silently empty). Promote scalars and
  `Array(String)` lists; keep `certifications` / `content_ratings` inside `payload`.
- **One process per stage**, two stages (`changes` extractor, `catalog` transformer), ports
  9124 and 9125.
- **Respect TMDB.** Concurrency bounded to 8 in-flight detail fetches; one calendar day per
  change-feed window; the poll interval is one hour; a 180-day `TTL` on every `tmdb_*` table
  enforces the terms' six-month cache ceiling by construction; `adult: true` ids are dropped at
  the extractor; the README carries the required attribution and the dashboard a text panel with it.
- **Let it crash** stays the rule: a 5xx, a timeout or a 429 propagates and the supervisor
  restarts the stage. The only remedied conditions are a 404 on a detail fetch (the entity is
  gone: tombstone the key's state, emit nothing) and a change record whose feed has no config
  record any more (warn and skip — the remedy is `request-tmdb token`).
- **Out of scope, deliberately:** seasons and episodes; images, videos and watch providers;
  per-person combined credits (credits come from the title side); the non-aggregate TV
  `credits`; `SIGNED` tokens, scopes or key rotation (named as extension points);
  backfill from TMDB's daily ID export files (the epoch on the config topic replaces it);
  anything Druid- or FRET-specific (the topics are flat; nothing in this repo touches Druid);
  a README screenshot (no `assets/` image — do not add an `<img>` tag); co-hosting both stages
  in one process.
- Do not change other examples' behaviour. The only edits outside the new package are listed
  in task 5 (wording) and task 4 (wiring).
- Commits: short imperative subject, no ticket prefix (the repo's convention, see `git log`),
  plus any attribution trailer the execution session's own system reminder prescribes.

## Pre-Flight

Run in the first turn, before task 1. Each check is one command with one expected result;
any failure stops the run: write the blocked report named in step 10 of the Execution Protocol
and do not start the tasks. Model, effort and permission mode are spelled literally here, in the
same spelling as the frontmatter's `executor` block.

Before check 1: if the frontmatter carries a `blocked_report`, this is a relaunch. Fold that
report into the Run Log below — its `date`, the check it named, what was tried, its `next` —
then delete it and remove `blocked_report` from the frontmatter, in this same turn, and only
then run the checks.

1. **Driven by this plan's /goal; model, effort and permission mode.**

   ```bash
   T=$(ls -t "$HOME"/.claude/projects/*/"$CLAUDE_CODE_SESSION_ID".jsonl | head -1)
   echo "T=$T"
   G=$(grep '"type":"goal_status"' "$T" | tail -1)
   echo "$G" | grep -o '"met":[a-z]*'
   echo "$G" | grep -o '"condition":"[^"]\{0,120\}'
   grep '"type":"assistant"' "$T" | tail -1 | grep -o '"model":"[^"]*"' | head -1
   grep -o '"permissionMode":"[^"]*"' "$T" | tail -1
   echo "effort=$CLAUDE_EFFORT"
   ```

   Expected: `"met":false` with a condition naming `2026-10-23-tmdb-catalog-plan.md`;
   `"model":"claude-sonnet-5-5"`, `"permissionMode":"auto"`, `effort=high`. A last record with
   `"met":true`, a different goal's condition, or no record at all in a readable transcript each
   mean no evaluator guards this run: stop and write the blocked report. If the `T=` line prints
   empty, the transcript could not be found: record that in the Run Log, take the model from what
   this session states about itself, the effort from `CLAUDE_EFFORT`, the permission mode from the
   launch flags (walk up from `$PPID` until the argv starts with `claude` and match
   `--permission-mode <mode>`), treat the goal check as unverifiable, and continue. If the
   `permissionMode` line is empty, use the same launch-flag fallback; if neither answers, say the
   mode is unverifiable rather than assuming it.

2. **Privileges** — skip this group only if the mode above is `bypassPermissions`, and say so in
   the Run Log.
   - `docker info --format "{{.ServerVersion}}"` prints a version.
   - `docker compose up --detach --wait --dry-run` exits 0 (run from the repo root).
   - `docker ps --format "{{.Names}}" | grep -vc "^flechtwerk-examples-"` prints `0` — no other
     project holds the stack's ports (the stack itself may be up or down).
   - `test -s ~/.config/flechtwerk-examples/tmdb-token && curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $(tr -d "[:space:]" < ~/.config/flechtwerk-examples/tmdb-token)" https://api.themoviedb.org/3/configuration`
     prints `200`.
   - `uvx flechtctl keyring show ~/.config/flechtwerk-examples/keyring.json` prints a line
     containing `dev-2026-10`.
   - `uv run poe test` exits 0 (proves the venv and the Docker-free tiers before anything changes).
   - No push probe: `closeout_push` is false and there is no ticket.

3. **Ground.**
   - This file exists at `2026-10-23-tmdb-catalog-plan.md`.
   - `git remote -v` names `flechtwerk-examples`, `git branch --show-current` prints `main`, and
     `git status --porcelain` is empty (this file is committed at the handoff, so at launch it is
     tracked and clean) — or this is a resume: the frontmatter says
     `status: executing` or `status: blocked`, and the only changes are this file, the blocked
     report just consumed, and the files the Run Log names as work in progress.

4. **Preconditions from the Verified Context.**
   - `grep -c "flechtwerk\[mqtt\]==0.17.0" pyproject.toml` prints `1` (task 1 not yet applied) —
     or, on a resume past task 1, `grep -c "flechtwerk\[mqtt,secrets\]==0.17.0"` prints `1`.
   - `test ! -e examples/tmdb_catalog` succeeds on a fresh start (on a resume, the Run Log says
     which task created it).
   - `grep -c 9124 prometheus/prometheus.yml` prints `0` on a fresh start.

## Tasks

Ordered: task 1 gives the venv the secrets extra every later task imports; task 2 is the code
the tests in task 3 exercise; task 4 wires what task 6 runs; task 5 documents what exists by
then. Each task ends in a commit.

- [ ] **1. The secrets extra and the two shared ops hooks**
  - `pyproject.toml`: change the dependency to `"flechtwerk[mqtt,secrets]==0.17.0"` and add a
    comment in the style of the `gtfs-realtime-bindings` one: the `secrets` extra installs
    `flechtctl` (the keyring and the `ENCRYPTED` codec), used only by `tmdb_catalog`, whose
    credential rides encrypted on its config topic. Then `uv lock` and `uv sync`.
  - `examples/_runner.py`: add a keyword parameter `keyring: Keyring | None = None` to `run()`,
    imported as `from flechtwerk.keyring import Keyring` (the joserfc-free seam; importing it
    needs no extra), pass `keyring=keyring` to `Flechtwerk.of(...)`, and add one sentence to the
    docstring: the process keyring for `ENCRYPTED` config attributes, loaded by the ops caller
    (`examples._setup.load_keyring`), `None` for stages without encrypted attributes.
  - `examples/_setup.py`: add `KEYRING_ENV = "FLECHTWERK_KEYRING"`,
    `DEFAULT_KEYRING = Path("~/.config/flechtwerk-examples/keyring.json")` and
    `def load_keyring(env_var: str = KEYRING_ENV, default: Path = DEFAULT_KEYRING) -> Keyring`:
    the path is `os.environ.get(env_var)` or `default`, `expanduser()`d; a missing file is a
    `sys.exit` with a three-line message naming the path, the command
    `uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) <path>` and `chmod 600 <path>`;
    otherwise `Keyring.from_json(path.read_text())`. The docstring states why an ops-side
    environment read is sanctioned (the `wildfire_watch` / `chaos_harness` precedent, restated
    for key material: it must never enter a stage module or a topic) and that stages never call
    this.
  - Verify: `uv run flechtctl --version` prints `flechtctl 0.17.0`;
    `uv run python -c "from examples._setup import load_keyring; print(load_keyring().kids())"`
    prints `['dev-2026-10']`; `uv run poe test` still reports `421 passed`.
  - Commit: stage exactly `pyproject.toml`, `uv.lock`, `examples/_runner.py` and
    `examples/_setup.py` by name; message `Add the secrets extra and a keyring hook for the ops callers`.

- [ ] **2. The example package `examples/tmdb_catalog/`**

  Every module carries the house-style docstrings (what, why, which framework lesson). Names
  below are exact; the wire key of **every** record on `tmdb-changes` and on the four catalog
  topics is `f"{feed}:{id}"` (`movie:550`), which co-partitions a title's versions and credits
  and is the catalog's state key.

  - `attributes.py` — topics and typed attributes:
    - Topics: `FEEDS_TOPIC = "tmdb-feeds"` (compacted config, key = feed name),
      `CHANGES_TOPIC = "tmdb-changes"`, `MOVIES_TOPIC = "tmdb-movies"`, `TV_TOPIC = "tmdb-tv"`,
      `PEOPLE_TOPIC = "tmdb-people"`, `CREDITS_TOPIC = "tmdb-credits"`;
      `FEEDS = ("movie", "tv", "person")` — the config keys double as TMDB path segments.
    - Config record: `FEED = Attribute("feed", STR)`,
      `ACCESS_TOKEN = Attribute("access_token", ENCRYPTED(STR))` (import `ENCRYPTED` from
      `flechtwerk.secrets`), `SINCE = Attribute("since", DATE, optional=True)`.
    - Change record: `TMDB_ID = Attribute("id", INT)`, `CHANGED_ON = Attribute("changed_on", DATE)`,
      `LISTING = Attribute("listing", STR)` (`open` while the day was still accruing, `closed`
      for the one full listing after the day ended), `DISCOVERED_AT = Attribute("discovered_at", DATETIME)`
      (reuses `FEED`).
    - Extractor state: `CURSOR = Attribute("cursor", DATE, optional=True)` — the last **closed**
      day fully listed; `SEEN = Attribute("seen", DICT(LIST(INT)))` — `{iso_date: [ids]}` for the
      open day only.
    - Catalog record (computed fields; everything else is spread from TMDB's response):
      `MEDIA_TYPE = Attribute("media_type", STR)`, `VERSION = Attribute("version", INT)`,
      `VERSION_AT = Attribute("version_at", DATETIME)`, `CONTENT_HASH = Attribute("content_hash", STR)`,
      `GENRES`, `KEYWORDS`, `PRODUCTION_COMPANIES`, `PRODUCTION_COUNTRIES`, `SPOKEN_LANGUAGES`,
      `NETWORKS`, `CREATED_BY` (each `Attribute(<snake name>, LIST(STR))`),
      `COLLECTION = Attribute("collection", STR, optional=True)`,
      `CERTIFICATIONS = Attribute("certifications", DICT(STR))`,
      `CONTENT_RATINGS = Attribute("content_ratings", DICT(STR))`,
      `IMDB_ID = Attribute("imdb_id", STR, optional=True)`,
      `WIKIDATA_ID = Attribute("wikidata_id", STR, optional=True)`,
      `TVDB_ID = Attribute("tvdb_id", INT, optional=True)`.
    - Credit record (ours, so fully declared): `MEDIA_ID = Attribute("media_id", INT)`,
      `CREDIT_ID = Attribute("credit_id", STR)`, `KIND = Attribute("kind", STR)` (`cast` | `crew`),
      `PERSON_ID = Attribute("person_id", INT)`, `NAME = Attribute("name", STR)`,
      `GENDER = Attribute("gender", INT)`, `KNOWN_FOR_DEPARTMENT = Attribute("known_for_department", STR, optional=True)`,
      `PERSON_POPULARITY = Attribute("person_popularity", FLOAT)`,
      `CHARACTER = Attribute("character", STR, optional=True)`,
      `BILLING_ORDER = Attribute("billing_order", INT, optional=True)` (TMDB's `order`; renamed
      because `order` is an SQL keyword downstream), `DEPARTMENT`, `JOB` (both `STR, optional=True`),
      `EPISODE_COUNT = Attribute("episode_count", INT, optional=True)`; reuses `MEDIA_TYPE`,
      `VERSION`, `VERSION_AT`.
    - Catalog transformer state: reuses `CONTENT_HASH`, `VERSION`, `VERSION_AT`.
  - `projection.py` — pure, framework-free (the logic tier drives it straight off the fixtures):
    - `TMDB_BASE_URL = "https://api.themoviedb.org/3"`;
      `APPEND = {"movie": "credits,keywords,release_dates,external_ids", "tv": "aggregate_credits,keywords,content_ratings,external_ids", "person": "external_ids"}`;
      `PAGE_SIZE = 100`, `MAX_PAGES = 500`, `DEFAULT_LOOKBACK_DAYS = 90`, `SEEN_HARD_CAP = 60_000`
      (≈ 540 KB of JSON ids, under the ~1 MB changelog-record ceiling the repo's CLAUDE.md explains).
    - `details_path(feed, tmdb_id) -> str` → `/{feed}/{id}?append_to_response={APPEND[feed]}`;
      `changes_path(feed, day, page) -> str` → `/{feed}/changes?start_date=D&end_date=D&page=N`
      with `D = day.isoformat()`.
    - `parse_changes_page(body: dict) -> tuple[list[int], int]`: the page's ids **in order,
      without duplicates, skipping entries whose `adult` is `True`** (a `null` adult passes), and
      `total_pages`; raises `ValueError` when `results` is missing. Unknown fields such as
      `softcore` are ignored, never enumerated.
    - `project_movie(raw) -> tuple[dict, list[dict]]` returns `(flat, credits)`. `flat` is every
      top-level key of `raw` **as delivered** (the spread-through rule: declare only what you
      compute), except: the appended objects `credits`, `keywords`, `release_dates`, `external_ids`
      are removed; `genres`, `production_companies` → lists of `name`; `production_countries` →
      list of `iso_3166_1`; `spoken_languages` → list of `iso_639_1`; `belongs_to_collection` →
      key `collection` holding its `name` (key absent when null); `keywords` → list of
      `keywords.keywords[].name`; `release_dates` → key `certifications` = `{iso_3166_1: certification}`
      taking, per country, the first non-empty `certification` in its list (countries with none
      omitted); `external_ids` → its non-null entries merged at top level (`imdb_id`,
      `wikidata_id`, …). `origin_country` stays (already a list of strings). `credits` is one dict
      per cast entry — `credit_id, kind="cast", person_id (TMDB's id), name, gender, known_for_department, person_popularity (TMDB's popularity), character, billing_order (TMDB's order)` —
      and one per crew entry — `credit_id, kind="crew", person_id, name, gender, known_for_department, person_popularity, department, job`;
      keys whose value is `None` are omitted.
    - `project_tv(raw)`: same spread; `genres`, `production_companies`, `production_countries`,
      `spoken_languages` as above; `networks`, `created_by` → lists of `name`; `keywords` →
      `keywords.results[].name` (**`results`, not `keywords`** — TMDB differs here);
      `content_ratings` → `{iso_3166_1: rating}`; `external_ids` merged (adds `tvdb_id`);
      **drop** `seasons`, `last_episode_to_air`, `next_episode_to_air`, `aggregate_credits`;
      keep `languages`, `origin_country`, `episode_run_time`. Credits from `aggregate_credits`:
      one row per `roles[]` entry of each cast member (`credit_id = role.credit_id`,
      `character = role.character`, `episode_count = role.episode_count`, `billing_order = order`)
      and one per `jobs[]` entry of each crew member (`credit_id = job.credit_id`, `job = job.job`,
      `episode_count`, `department`).
    - `project_person(raw)`: spread everything, merge `external_ids`, credits `[]`.
    - `PROJECTORS = {"movie": project_movie, "tv": project_tv, "person": project_person}`.
    - `content_hash(flat, credits) -> str`: the first 16 hex of
      `sha256(json.dumps([flat, credits], sort_keys=True, separators=(",", ":"), ensure_ascii=False))`,
      computed **before** the version fields are added. `popularity` is in the hash on purpose:
      TMDB recomputes it daily, and a new version per move is the popularity history the
      dashboard reads.
    - `day_windows(start: date, today: date) -> list[date]`: every day from `start` to `today`
      inclusive, `[]` when `start > today`.
    - `prune_seen(seen, keep_from: date, *, hard_cap=SEEN_HARD_CAP) -> dict`: drop buckets whose
      date is before `keep_from`; if the surviving ids exceed `hard_cap`, drop whole oldest
      buckets first, then trim the oldest ids of the last bucket, with a WARNING naming what went
      (copy `examples/wildfire_watch/ingest.py:prune_seen`'s shape).
  - `changes.py` — `class TmdbChanges(Extractor)`, the stage-1 poller:
    - `config_topics = [FEEDS_TOPIC]`; `__init__(self, client: httpx.AsyncClient | None = None, *, base_url=TMDB_BASE_URL, changes_topic=CHANGES_TOPIC, max_pages=MAX_PAGES, clock: Callable[[], datetime] | None = None)`;
      `clock` defaults to `lambda: datetime.now(timezone.utc)` and is the one wall-clock read
      (it decides which day is *today* — operational, not event time; `changed_on` is TMDB's).
      `__aenter__`/`__aexit__` build and close an `httpx.AsyncClient(timeout=httpx.Timeout(60.0))`
      when none was injected (`# pragma: no cover — live path`, as wildfire).
    - `_page(token, feed, day, page) -> httpx.Response`: GET `base_url + changes_path(...)` with
      header `Authorization: Bearer <token>`; a 401 raises `RuntimeError` quoting the body's
      `status_message` and saying `re-run: uv run poe request-tmdb token < <token file>`; anything
      else non-2xx `raise_for_status()`.
    - `poll(config, state)`: `feed = config[FEED]`, `token = config[ACCESS_TOKEN]` (decrypted by
      the codec on read), `today = clock().date()`, `since = config.get(SINCE) or today - timedelta(days=DEFAULT_LOOKBACK_DAYS)`,
      `cursor = state.get(CURSOR)`, `first = cursor + 1 day if cursor else since`,
      `seen = {d: list(ids) for d, ids in (state.get(SEEN) or {}).items()}`. For each `day` in
      `day_windows(first, today)`: page through `_page(..., 1..)` until `page >= min(total_pages, max_pages)`
      (empty day: one request, `total_pages` 0), collecting ids via `parse_changes_page`; when
      `total_pages > max_pages` log a WARNING that the listing was capped and ids beyond it are
      caught on their next change; `discovered_at` = the UTC `Date` header of page 1
      (`email.utils.parsedate_to_datetime`, fall back to `clock()`). **Which ids to emit depends on
      whether the day is closed.** If `day < today` the day is closed and gets exactly one full
      listing: emit **every** id of the day with `LISTING: "closed"`, set `cursor = day` and drop
      `seen[day]`. Otherwise the day is still open: emit only the ids not in `seen[day]` with
      `LISTING: "open"` and set `seen[day] = sorted(old ∪ fresh)`. Each emitted id is one
      `Message(key=f"{feed}:{id}", topic=changes_topic, value=Event({FEED, TMDB_ID, CHANGED_ON: day, LISTING, DISCOVERED_AT}))`.
      The closing re-list is deliberate and cheap (≈17 k extra fetches a day, ≈0.2 per second):
      an entity edited again on the same day after its hourly fetch is fetched once more after
      the day closed, so the catalog's copy of any day is final within one poll of midnight UTC,
      and the catalog's hash gate turns a no-op second fetch into nothing. Backfill days are all
      closed, so the backfill lists each day once. Then `seen = prune_seen(seen, keep_from=today)`;
      log one INFO line (feed, day, pages, emitted count, open or closed); and **yield the State
      for this day** —
      `State({CURSOR: cursor, SEEN: seen})`, omitting `CURSOR` while it is still `None`. One
      State per day is deliberate: every `State` yield is a commit boundary, so a 90-day backfill
      is 90 transactions, a crash resumes at the last closed day, and no transaction outlives the
      framework's 10-minute limit (one day is ≤ 500 pages ≈ 2 minutes). Document in the
      docstring: the cursor is monotonic — `since` seeds it once; later edits of `since` do not
      rewind (re-listing means a fresh application id or a deleted changelog).
    - `stage = TmdbChanges()` at module level (the credential is on the config topic, so unlike
      wildfire a singleton is fine).
  - `catalog.py` — `class TmdbCatalog(Transformer)`, the stage-2 fetch-and-version:
    - `input_topics = [CHANGES_TOPIC]`, `config_topics = [FEEDS_TOPIC]`;
      `__init__(self, client=None, *, base_url=TMDB_BASE_URL, concurrency=8, topics: dict[str, str] | None = None, credits_topic=CREDITS_TOPIC)`
      with `topics` defaulting to `{"movie": MOVIES_TOPIC, "tv": TV_TOPIC, "person": PEOPLE_TOPIC}`;
      `__aenter__` builds the client (`timeout=60`, `limits=httpx.Limits(max_connections=concurrency)`)
      when none injected and `self._semaphore = asyncio.Semaphore(concurrency)` always. The
      docstring explains the bound: the runner runs one `transform` per state key of a batch
      concurrently, so a 500-record batch would otherwise open 500 requests against a ~40 rps
      service; at 8 in flight and ≈0.25 s per fetch a batch drains in ≈16 s, far inside
      `max.poll.interval`.
    - `transform(msg, state)`: ignore a `None` value; `feed, tmdb_id, changed_on` from the
      change record; `feed_config = self.configs.get(feed)` — `None` → `log.warning` (no config
      record for the feed: skipped, re-request with `request-tmdb token`) and return;
      `token = feed_config[ACCESS_TOKEN]`; under the semaphore GET `base_url + details_path(feed, tmdb_id)`
      with the bearer header; **404** → if `state` is non-empty `log.info(... removed)` and
      `yield State()`, return; **401** → the same `RuntimeError` as `changes.py`; otherwise
      `raise_for_status()`. Then `flat, credits = PROJECTORS[feed](response.json())`,
      `digest = content_hash(flat, credits)`; if `digest == state.get(CONTENT_HASH)`: return
      (unchanged: no output, no state write). Else `version = (state.get(VERSION) or 0) + 1`,
      `version_at` = the response's UTC `Date` header (fallback `datetime.now(timezone.utc)`);
      yield `Message(key=msg.key, topic=topics[feed], value=Event({**Record.wrap(flat), MEDIA_TYPE: feed, VERSION: version, VERSION_AT: version_at, CHANGED_ON: changed_on, CONTENT_HASH: digest}))`;
      then one `Message(key=msg.key, topic=credits_topic, value=Event({**Record.wrap(credit), MEDIA_TYPE: feed, MEDIA_ID: tmdb_id, VERSION: version, VERSION_AT: version_at}))`
      per credit; then `State({CONTENT_HASH: digest, VERSION: version, VERSION_AT: version_at})`.
    - `stage = TmdbCatalog()`.
  - `setup.py` — copy `examples/wildfire_watch/setup.py`'s shape: `create_topics()` makes
    `tmdb-feeds` (`cleanup.policy: compact`) and the five data topics, all 8 partitions,
    replication 1, idempotent; `apply_clickhouse_schema(base_url, *, database, user, password)`
    identical to wildfire's; `main()` prints the next steps: (1) the keyring command from
    `load_keyring`'s message, (2) `uv run poe request-tmdb token < ~/.config/flechtwerk-examples/tmdb-token`,
    (3) optionally `uv run poe request-tmdb since 2026-07-25 [movie|tv|person]`, (4) `uv run poe tmdb`.
  - `request.py` — the ops tool, `python -m examples.tmdb_catalog.request …`:
    - `token`: read stdin, strip whitespace, exit 2 with a message if empty; `load_keyring()` →
      `install_keyring(keyring)`; read the current `tmdb-feeds` records to the end with a
      group-less `AIOKafkaConsumer` (as `examples/wildfire_watch/request.py` does for its overlap
      check); for every feed in `FEEDS` produce `json.dumps(Config({FEED: feed, ACCESS_TOKEN: token}).raw)`
      keyed by the feed name, adding `SINCE: <the stored date>` to the literal **only when** the
      stored record had one (an optional attribute rejects `None`) — the typed write encrypts;
      print one line per feed showing the
      feed, its `since` (or `default: last 90 days`) and the token's `kid` via
      `flechtwerk.secrets.kid_of(raw["access_token"])`, never the token.
    - `since <YYYY-MM-DD> [feed]`: validate with `date.fromisoformat`; require an existing record
      for each targeted feed (else exit telling the user to run `token` first); produce the raw
      dict `{**stored_raw, "since": day.isoformat()}` as-is — **no typed `Config`, no decrypt, no
      re-encrypt**, the stored ciphertext string travels unchanged. Print that the cursor is
      monotonic: `since` only seeds a feed that has never listed.
    - `retire [feed]`: tombstone the feed's record (all three without an argument).
    - no arguments: list the records (feed, since, kid) or say none are requested.
  - `__main__.py` — the dispatcher, `python -m examples.tmdb_catalog {changes|catalog}` through
    `examples._runner`: `changes` → `run(changes_stage, application_id="tmdb-changes", client_id="tmdb-changes-0", metrics_port=9124, poll_interval=timedelta(hours=1), keyring=load_keyring())`;
    `catalog` → `run(catalog_stage, application_id="tmdb-catalog", client_id="tmdb-catalog-0", metrics_port=9125, keyring=load_keyring())`;
    both in lambdas so the keyring is loaded only for the selected stage. Docstring: the
    credential rides encrypted on the config topic; the ops caller reads only the keyring
    *path*; the stages touch no `os.environ` — the keyring precedent `wildfire_watch/__main__.py`
    left for "its own example".
  - `__init__.py` — module docstring (the two stages, why the example exists: reference data as a
    change stream, the first keyring example, the calendar-day cursor against a 500-page cap,
    TMDB's terms) and `__all__` re-exporting the topic names and the projection functions.
  - `clickhouse.sql` — the Kafka-engine shortcut as wildfire (same header comment conventions,
    `kafka_format = 'JSONAsObject'`, `kafka_broker_list = 'kafka:19092'`, group
    `<topic>-clickhouse`, every object `flechtwerk.`-qualified, `CREATE … IF NOT EXISTS`):
    - Five queues: `tmdb_changes_queue`, `tmdb_movies_queue`, `tmdb_tv_queue`, `tmdb_people_queue`, `tmdb_credits_queue`.
    - `tmdb_changes` (`feed LowCardinality(String), id UInt64, changed_on Date, listing LowCardinality(String), discovered_at DateTime64(3, 'UTC')`,
      MergeTree ORDER BY `(feed, changed_on, id)`, TTL `toDateTime(discovered_at) + INTERVAL 180 DAY`) + `tmdb_changes_mv`;
      `changed_on` as `toDate(message.changed_on::String)`. An id listed while its day was open
      and again at the close is two rows, which is why the dashboard counts `uniqExact(id)`.
    - `tmdb_movies` (history, MergeTree ORDER BY `(id, version_at)`) and `tmdb_movies_current`
      (`ReplacingMergeTree(version)` ORDER BY `id`, queried `FINAL` — the version **counter** is
      the replacing column, not `version_at`, because `version_at` comes from the HTTP `Date`
      header at one-second resolution and two versions in one second would tie), both fed from
      `tmdb_movies_queue` by `tmdb_movies_mv` / `tmdb_movies_current_mv` with the same column
      list: `id UInt64, version UInt32, version_at DateTime64(3, 'UTC'), changed_on Date, title String, original_title String, original_language LowCardinality(String), release_date Nullable(String)`
      (as `nullIf(message.release_date::String, '')`), `runtime Nullable(UInt32), budget UInt64, revenue UInt64, popularity Float64, vote_average Float64, vote_count UInt32, status LowCardinality(String), adult Bool, imdb_id Nullable(String), collection Nullable(String), genres Array(String), keywords Array(String), production_companies Array(String), production_countries Array(String), spoken_languages Array(String), origin_country Array(String), payload JSON`;
      TTL `toDateTime(version_at) + INTERVAL 180 DAY` on both.
    - `tmdb_tv` / `tmdb_tv_current` likewise: `id, version, version_at, changed_on, name String, original_name String, original_language, first_air_date Nullable(String), last_air_date Nullable(String), status, type LowCardinality(String), in_production Bool, number_of_seasons UInt16, number_of_episodes UInt32, popularity, vote_average, vote_count, adult, imdb_id, genres, keywords, networks Array(String), production_companies, production_countries, origin_country, spoken_languages, created_by Array(String), payload JSON`.
    - `tmdb_people` / `tmdb_people_current`: `id, version, version_at, changed_on, name, gender UInt8, known_for_department LowCardinality(String)` (as `ifNull(message.known_for_department::Nullable(String), '')`), `birthday Nullable(String), deathday Nullable(String), place_of_birth Nullable(String), popularity, adult, imdb_id, also_known_as Array(String), payload JSON`.
    - `tmdb_credits`: `ReplacingMergeTree(version)` ORDER BY `(media_type, media_id, credit_id)`
      (the title's version counter, for the same reason as the `_current` tables):
      `media_type LowCardinality(String), media_id UInt64, credit_id String, kind LowCardinality(String), person_id UInt64, name String, gender UInt8, known_for_department LowCardinality(String), person_popularity Float64, character Nullable(String), billing_order Nullable(UInt16), department Nullable(String), job Nullable(String), episode_count Nullable(UInt32), version UInt32, version_at DateTime64(3, 'UTC')`,
      TTL 180 days, `tmdb_credits_mv`.
    - Casts are the ones the Verified Context proved: `::UInt64` / `::UInt32` / `::UInt16` /
      `::UInt8` / `::Float64` / `::Bool` for fields TMDB always sends (an absent one would land as
      `0` or `false`, never fail), `::Nullable(String)` / `::Nullable(UInt32)` / `::Nullable(UInt16)`
      where absence is meaningful (`runtime`, `billing_order`, `episode_count`, dates, ids),
      `nullIf(…::String, '')` for TMDB's empty-string dates, `ifNull(message.known_for_department::Nullable(String), '')`
      on **both** `tmdb_people` and `tmdb_credits`, `::Array(String)` for the name lists,
      `parseDateTime64BestEffort(…::String, 3)` for `version_at` / `discovered_at`,
      `toDate(…::String)` for `changed_on`; never an integer cast of `popularity` or
      `vote_average` (a float cast to an integer truncates), and no `Map`. The header comment
      states why: certifications and content ratings are read as `payload.certifications.US`
      subcolumns, and why the TTL exists (TMDB's six-month cache clause).
  - Verify: `uv run python -c "import examples.tmdb_catalog.changes, examples.tmdb_catalog.catalog, examples.tmdb_catalog.request, examples.tmdb_catalog.setup"`
    exits 0; `uv run python -m examples.tmdb_catalog` exits non-zero printing the usage line
    naming `changes|catalog`.
  - Commit: `Add the TMDB Catalog example: change feeds, versioned catalog, encrypted token`
    (the tests of task 3 may be squashed into this commit if written in the same stretch).

- [ ] **3. Fixtures and the three test tiers**
  - Capture real fixtures into `examples/tmdb_catalog/tests/fixtures/` with the token file
    (`H="Authorization: Bearer $(tr -d "[:space:]" < ~/.config/flechtwerk-examples/tmdb-token)"`):
    `movie_550.json` ← `curl -s -H "$H" "https://api.themoviedb.org/3/movie/550?append_to_response=credits,keywords,release_dates,external_ids"`;
    `tv_87108.json` ← the TV URL for id 87108 with `aggregate_credits,keywords,content_ratings,external_ids`;
    `person_287.json` ← the person URL with `external_ids`;
    `changes_movie_p1.json` ← `…/3/movie/changes?start_date=2026-10-01&end_date=2026-10-01&page=1`
    (a real 100-id page; it carries `softcore` and at least one `adult: null`). Pretty-print each
    with `jq .` and write `PROVENANCE.md` in the wildfire format: capture date, the four commands
    with the token redacted as `[TOKEN]`, "bodies are unmodified", the byte sizes, and the TMDB
    attribution sentence. Fixture-derived numbers (cast and crew counts) are **computed from the
    fixture in the tests**, never hard-coded, because a re-capture changes them.
  - `tests/logic_test.py` (pure, no framework runtime): `parse_changes_page` on the real page
    (as many ids as fixture entries whose `adult` is not `True`, computed from the fixture
    itself; unique; **in page order** — the real page is not sorted by id; the one `adult: null`
    entry passes; `total_pages` equals the fixture's) and on hand-built pages (an `adult: True`
    id dropped, a `null` passes, a duplicate collapses, a missing `results` raises);
    `project_movie(movie_550)` — `flat["title"] == "Fight Club"`, `"Drama" in flat["genres"]`,
    `flat["imdb_id"] == "tt0137523"`, `flat["certifications"]["US"] == "R"`, no `credits` /
    `release_dates` / `external_ids` keys left, `softcore` still present (spread), credits count
    `== len(raw["credits"]["cast"]) + len(raw["credits"]["crew"])`, the first cast row has
    `kind == "cast"`, `billing_order == 0`, `character == raw["credits"]["cast"][0]["character"]`
    (never a literal name — a re-capture may change it), crew rows carry `department`
    and `job` and no `character`; `project_tv(tv_87108)` — `flat["name"] == "Chernobyl"`,
    `"HBO" in flat["networks"]`, keywords from `results`, `tvdb_id` an int, no `seasons` key,
    credits count `== sum(len(c["roles"]) for cast) + sum(len(c["jobs"]) for crew)`, an
    `episode_count` on every row; `project_person(person_287)` — `name == "Brad Pitt"`,
    `gender == 2`, credits `[]`; `content_hash` identical for key-reordered input and different
    when `popularity` moves or a credit's `job` changes; `day_windows` (inclusive, empty when
    start > today); `prune_seen` (drops old buckets, caps, keeps the newest ids).
  - `tests/runner_test.py` (the shipped fakes, as `examples/wildfire_watch/tests/runner_test.py`
    — copy its `_extractor_runner` and `_make_module` harnesses; seed the transformer's config
    store as `examples/gdelt_news_stories/tests/runner_test.py` does). Every test runs inside
    `with installed_keyring():` (a pytest fixture built on `flechtwerk.testing.installed_keyring`)
    and the config record is `Config({FEED: "movie", ACCESS_TOKEN: "test-token", SINCE: …}).raw`,
    so the wire form is ciphertext and the stage proves it decrypts. The `httpx.MockTransport`
    handler records request paths and headers and stamps a `Date` header the test controls.
    - Extractor, fixed clock `T` (a date), `since = T - 1 day`, day `T-1` served as two pages
      (ids 1,2,3 then 4,5, `total_pages: 2`), day `T` as one page (ids 5,6): one `poll_one`
      emits 7 change messages — 5 for `T-1` with `listing == "closed"`, 2 for `T` with
      `listing == "open"`, id 5 twice with different `changed_on` — every key `movie:<id>`, every
      request carrying `Authorization: Bearer test-token`, and the state holds `cursor == T-1`
      and `seen == {T: [5, 6]}`. A second poll with the same bodies emits nothing and writes no
      state. With the clock at `T+1`, day `T` now serving ids 5,6,7 and `T+1` empty: day `T`
      closes, so **all three** ids 5, 6, 7 are emitted as `closed` (the closing re-list), nothing
      for `T+1`, `cursor == T`, `seen == {T+1: []}`. Further: an `adult: True` id is never
      emitted; `max_pages=1` against a two-page day emits only page 1 and logs the cap warning;
      a 401 raises `RuntimeError` mentioning `request-tmdb token`; a 500 raises
      `httpx.HTTPStatusError`.
    - Transformer over `process_batch`: one change record for `movie:550` with the movie fixture
      served → `producer.sent` holds exactly one `tmdb-movies` message (`media_type == "movie"`,
      `version == 1`, `title == "Fight Club"`, `content_hash` 16 hex) followed by
      `len(cast) + len(crew)` `tmdb-credits` messages (`media_id == 550`, same `version`), all
      keyed `movie:550`, and the state holds the hash and version 1; replaying the same change
      sends nothing more and leaves the state untouched; the TV fixture yields the aggregate
      credit rows; the person fixture yields no credits; a 404 body leaves no message and
      tombstones a pre-seeded state; a 500 raises; a change for a feed with no config record is
      skipped with a warning (`caplog`).
  - `tests/integration/tmdb_catalog_integration_test.py` (`pytestmark = pytest.mark.integration`,
    mirroring `examples/wildfire_watch/tests/integration/wildfire_tracker_integration_test.py`):
    `test_catalog_versions_over_the_broker` — create the topics idempotently, produce the
    `movie` config record (encrypted under `fixture_keyring()`, built inside
    `installed_keyring()`) to `tmdb-feeds`, run `Flechtwerk.of(..., stage=TmdbCatalog(client=<MockTransport client serving movie_550>), keyring=fixture_keyring())`,
    produce one change record keyed `movie:550`, read `tmdb-movies` and `tmdb-credits` under
    `read_committed`, assert one movie message with `version == 1` and `len(cast) + len(crew)`
    credits; `test_clickhouse_schema_applies` — apply `clickhouse.sql` and assert the 5 queues,
    8 tables and 8 materialized views named in task 2 exist in `system.tables`.
  - Verify: `uv run poe test` exits 0 with more than 421 passed and 0 failed;
    `uv run pytest -m integration examples/tmdb_catalog -q` exits 0 with 2 passed.
  - Commit: `Test the TMDB Catalog example in all three tiers`.

- [ ] **4. Wiring: poe targets, Prometheus, the dashboard**
  - `pyproject.toml`, `[tool.poe.tasks]`, after the `run-f1-timing` line: `setup-tmdb`
    ("Create the TMDB Catalog topics and apply the ClickHouse schema (no feed seeded — use request-tmdb)"),
    `request-tmdb` ("Store the TMDB token encrypted on tmdb-feeds / set an epoch / retire: uv run poe request-tmdb token < FILE | since 2026-07-25 [movie|tv|person] | retire [feed]; no args lists them"),
    `run-tmdb-changes` ("Run TMDB stage 1: list each feed's daily changes -> tmdb-changes (needs the keyring)"),
    `run-tmdb-catalog` ("Run TMDB stage 2: fetch, project and version each changed entity -> tmdb-movies / tmdb-tv / tmdb-people / tmdb-credits");
    then a `[tool.poe.tasks.run-tmdb]` supervisor copied from `run-wildfire` (same trap,
    `supervise`, `for stage in changes catalog`) whose guard is the keyring file instead of an
    env var:
    `: "${FLECHTWERK_KEYRING:=$HOME/.config/flechtwerk-examples/keyring.json}"; [ -r "$FLECHTWERK_KEYRING" ] || { echo "no keyring at $FLECHTWERK_KEYRING — create one: uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) $FLECHTWERK_KEYRING"; exit 1; }; export FLECHTWERK_KEYRING`,
    and a `[tool.poe.tasks.tmdb]` quickstart `sequence = ["setup-tmdb", "run-tmdb"]`.
  - `prometheus/prometheus.yml`: after the f1 block, a comment and two targets in the same
    style — `host.docker.internal:9124` `{ example: tmdb_catalog, stage: extractor }` and
    `host.docker.internal:9125` `{ example: tmdb_catalog, stage: transformer }`.
  - `grafana/dashboards/tmdb-catalog.json`: uid `flechtwerk-tmdb`, title
    `Flechtwerk — TMDB Catalog`, tags `["flechtwerk"]`, `schemaVersion` 39, every panel on the
    `clickhouse` datasource uid, built by copying panel objects and the query macros from
    `grafana/dashboards/wildfire-watch.json`. Panels: (1) three `stat`s — `SELECT count() FROM flechtwerk.tmdb_movies_current FINAL`
    and the same for `tmdb_tv_current`, `tmdb_people_current`; (2) `timeseries` "Change feed per day" —
    `SELECT toDateTime(changed_on) AS t, feed, uniqExact(id) AS ids FROM flechtwerk.tmdb_changes WHERE $__timeFilter(toDateTime(changed_on)) GROUP BY t, feed ORDER BY t`
    (`uniqExact`, because an open-day listing and the closing one are two rows per id);
    (3) `timeseries` "Versions per hour" — a `UNION ALL` over the three history tables of
    `SELECT toStartOfHour(version_at) AS t, '<type>' AS media_type, count() AS versions … WHERE $__timeFilter(version_at) GROUP BY t`;
    (4) `barchart` "Movie genres" — `SELECT genre, count() AS movies FROM flechtwerk.tmdb_movies_current FINAL ARRAY JOIN genres AS genre GROUP BY genre ORDER BY movies DESC LIMIT 15`;
    (5) `table` "Most credited people" — `SELECT person_id, any(name) AS name, count() AS credits, uniqExact(media_id) AS titles FROM flechtwerk.tmdb_credits FINAL GROUP BY person_id ORDER BY credits DESC LIMIT 20`;
    (6) `table` "Latest versions" — `UNION ALL` of `version_at, media_type, id, title-or-name AS name, version` from the three history tables `ORDER BY version_at DESC LIMIT 25`;
    (7) a `text` panel with the TMDB logo
    (`https://www.themoviedb.org/assets/2/v4/logos/v2/blue_short-8e7b30f73a4020692ccca9c88bafe5dcb6f8a62a4c6bc55cd9ba82bb2cd95f6c.svg`,
    verified to answer 200 — should it ever stop, take the current one from
    https://www.themoviedb.org/about/logos-attribution) and the required disclaimer sentence.
  - Verify: `uv run poe --help | grep -c tmdb` prints `6`; `docker compose config --quiet`
    exits 0; `jq -r .uid grafana/dashboards/tmdb-catalog.json` prints `flechtwerk-tmdb`;
    `grep -c "host.docker.internal:9124" prometheus/prometheus.yml` prints `1`. The Prometheus
    file is validated for real in task 6: the config is a read-only directory mount
    (`./prometheus:/etc/prometheus/config:ro` in `docker-compose.yaml`) that Prometheus reads
    only at start, so task 6 restarts the container when the stack was already up; no promtool
    run is prescribed.
  - Commit: `Wire the TMDB Catalog example into poe, Prometheus and Grafana`.

- [ ] **5. Documentation**
  - `examples/tmdb_catalog/README.md`, the wildfire structure: H1 `# TMDB Catalog — reference data as a change stream`;
    an intro naming the non-nerd pitch; a Mermaid `flowchart LR` (keyring file → `request.py`
    → `tmdb-feeds` → `TmdbChanges` ← TMDB change feeds → `tmdb-changes` → `TmdbCatalog` ← TMDB
    details → the four topics → ClickHouse → Grafana, classes as wildfire's); **What it
    demonstrates** (1. reference data as a change stream: the content-hash version gate, why
    `popularity` is inside it; 2. the first encrypted credential on a config topic: keyring →
    `request-tmdb token` → `ENCRYPTED(STR)` → decrypt on read, with `flechtwerk_secret_decrypts_total`
    as the proof, and why the token rides on each feed record (one config namespace); 3. the
    calendar-day cursor with a seen-set for the open day and one full closing re-list per day,
    chosen because of the 14-day window and 500-page caps measured in the plan; 4.
    commit-per-day backfill: one `State` per day);
    **The data** (the three entities, the four topics, the eight tables, what is promoted and
    what stays in `payload`, the credits fan-out, `_current` vs history, the 180-day TTL);
    **Getting a token and a keyring** (the TMDB *API Read Access Token* from
    `https://www.themoviedb.org/settings/api`; `uv run flechtctl keyring new --kid dev-$(date -u +%Y-%m) ~/.config/flechtwerk-examples/keyring.json`;
    `FLECHTWERK_KEYRING` as override; `request-tmdb token < FILE`; rotation = re-run `token`);
    **Run it** (`poe up`, `setup-tmdb`, `request-tmdb token`, optional `since`, `poe tmdb`;
    the `__main__` targets; ports); **The backfill arithmetic** (the measured 90-day volume,
    ≈890 k id-days, at ≈25 requests/s ≈ 10 h; how to shrink it with `since`; the steady state of
    ≈17 k changes/day, doubled by the closing re-list, ≈ 0.4 requests/s); **Caveats** (the
    500-page cap, adult ids dropped, `seen` hard cap, a `since` edit does not rewind, intraday
    freshness is one poll interval and a day's copy is final one poll after midnight UTC, the
    8 concurrent detail fetches are a global bound per catalog process rather than per feed,
    credit rows are never retracted — `tmdb_credits` holds the union of every credit ever seen
    for a title within the TTL, and a credit TMDB removed keeps its last version's row —
    TMDB's non-commercial terms and the 6-month cache rule the TTL enforces, popularity-driven
    versions); **Attribution** (the logo
    and the sentence); **Extension points** (seasons/episodes, person combined credits, images,
    `SIGNED` tokens and scopes, key rotation with `reencrypt`).
  - `examples/tmdb_catalog/CLAUDE.md`: one dense paragraph in the wildfire `CLAUDE.md` voice
    with the load-bearing rationale: the token on each feed record (single config namespace,
    extractor polls every entry), the clock injection, one State per day, the open-day seen-set
    plus the closing re-list, the hash gate and `popularity`, the page cap, no `Map` columns,
    the TTL, the ops-side env read.
  - Root `README.md`: row 11 in the "What's Inside" table —
    `[`tmdb_catalog`](examples/tmdb_catalog)` | **reference data as a change stream + the first encrypted credential on a config topic**: … | `tmdb` —
    in the register of rows 9 and 10, mentioning movies, TV, people, credits, the content-hash
    version gate, the calendar-day cursor, and that the free TMDB token rides encrypted on the
    config topic (`flechtwerk[secrets]`, a keyring made with `flechtctl`).
  - Root `CLAUDE.md`: add `tmdb` to the key list in the **Naming** paragraph; change "the one
    example needing an API key" (the `wildfire_watch` mention in the per-example CLAUDE.md
    paragraph) to "the first example needing an API key", add `tmdb_catalog` (encrypted
    credential on the config topic) to that list of examples with their own CLAUDE.md and
    change its opening "The four examples with load-bearing design rationale" to "five"; in
    "Conventions carried from the framework" change "Two `__main__.py`s deliberately read the
    environment" to three, naming `tmdb_catalog` (the keyring *path*, `FLECHTWERK_KEYRING`, via
    `examples/_setup.load_keyring`); add one convention bullet: **A credential on a config topic
    goes through `ENCRYPTED`** (`flechtwerk[secrets]`, 0.17.0) — `tmdb_catalog` is the user; the
    keyring is injected by the ops caller through `_runner.run(keyring=…)`; `wildfire_watch`
    keeps the env-var route as the pre-keyring precedent; key material never enters a stage
    module or a topic. Mention ports 9124/9125 wherever the port allocation is described.
  - `examples/wildfire_watch/CLAUDE.md` line 4: "the **only example needing an API key**" →
    "the **first example needing an API key** (TMDB Catalog is the second, and carries its
    credential encrypted on the config topic)". `examples/wildfire_watch/__main__.py` docstring:
    the sentence ending "that deserves its own example (see the README's extension points) rather
    than being smuggled in here." → point at `examples/tmdb_catalog`, which is that example.
  - Run `graphify update .` (its output is gitignored); if it fails, note it in the Run Log and
    continue — it is not an acceptance criterion.
  - Verify: `grep -c tmdb README.md CLAUDE.md` prints at least 1 for each;
    `grep -c "only example needing" examples/wildfire_watch/CLAUDE.md` prints `0`.
  - Commit: `Document the TMDB Catalog example and the keyring convention`.

- [ ] **6. The live run against the shared stack**
  - `uv run poe up` (real; the dry run passed pre-flight). If `docker compose ps --format "{{.Name}}"`
    listed `flechtwerk-examples-prometheus-1` **before** that `up`, the stack was already running
    and Prometheus still holds the old scrape config: run `docker compose restart prometheus`
    (its `--dry-run` form ran clean under `auto` in planning) so the 9124/9125 targets load.
    Then `uv run poe setup-tmdb`;
    `uv run poe request-tmdb token < ~/.config/flechtwerk-examples/tmdb-token` (prints three
    lines with kid `dev-2026-10`); `uv run poe request-tmdb since "$(date -u -v-1d +%F)"` so the
    live check lists two days, not ninety.
  - Start both stages as **background** processes (the Bash tool's `run_in_background`, one
    call each, logs redirected to files under the session's scratchpad directory):
    `uv run python -m examples.tmdb_catalog changes` and `uv run python -m examples.tmdb_catalog catalog`.
  - Poll every 30 s for up to 10 minutes until all four hold:
    `curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20flechtwerk.tmdb_movies"` ≥ 100,
    `…tmdb_tv` ≥ 50, `…tmdb_people` ≥ 100, `…tmdb_credits` ≥ 500. A foreground `sleep` is blocked
    in this harness, so poll with the Monitor tool's until-loop or with one background Bash
    loop (`for i in $(seq 1 20); do …; sleep 30; done`) whose output you read when it ends.
    Record the counts and the elapsed time in the Run Log.
  - While the stages run, show: `curl -s http://localhost:9125/metrics | grep flechtwerk_secret_decrypts_total`
    prints a sample with `kid="dev-2026-10"`; `curl -s http://localhost:9124/metrics | grep flechtwerk_keyring_keys_loaded`
    prints a `1`; `curl -s "http://localhost:8123/?query=SELECT%20feed,%20count()%20FROM%20flechtwerk.tmdb_changes%20GROUP%20BY%20feed%20FORMAT%20TSV"`
    prints all three feeds; and the Grafana check
    `curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/dashboards/uid/flechtwerk-tmdb` prints `200`.
  - Stop the two stages with `pkill -f "python -m examples.tmdb_catalog"` (exit 0 or 1 are both
    fine — 1 only means nothing matched) and confirm `pgrep -f "python -m examples.tmdb_catalog"`
    prints nothing; **leave the stack up** (the user wants to look at the data). Append the numbers to the Run Log; no code changes are
    expected from this task — if one is needed, make it, re-run the affected tier, and commit it
    with a message saying what the live run showed.

## Acceptance Criteria

Work criteria — re-run in full immediately before the Closeout, after the last work commit,
with every command and its output shown:

1. `uv run poe test` exits 0 and its summary line reports more than 421 passed and no failures
   (421 is the count before this plan; the new tiers add to it — never pad tests to reach a number).
2. `uv run pytest -m integration examples/tmdb_catalog -q` exits 0 and reports 2 passed.
3. `uv run flechtctl --version` prints `flechtctl 0.17.0`.
4. `uv run poe --help | grep -c tmdb` prints `6`.
5. `jq -r .uid grafana/dashboards/tmdb-catalog.json` prints `flechtwerk-tmdb`.
6. `grep -c "host.docker.internal:9124" prometheus/prometheus.yml` prints `1`.
7. `curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20flechtwerk.tmdb_movies"` prints a number of at least 100,
   and the same query against `flechtwerk.tmdb_credits` prints a number of at least 500 (the stack stays up after task 6, so these re-run).
8. `curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/dashboards/uid/flechtwerk-tmdb` prints `200`.
9. `grep -c "only example needing" examples/wildfire_watch/CLAUDE.md` prints `0` and `grep -c tmdb_catalog README.md CLAUDE.md` prints a number of at least 1 for each file.

Closeout criteria — hold only once the Closeout section has run, and are shown there:

10. The final state of this plan was committed on its own, and only then was the file deleted
    in a separate later commit. Shown by Closeout steps 2 and 5.
11. `git status --porcelain` is empty — everything committed, plan file gone. Shown after
    Closeout step 5.

## Rollback

- Code: `git revert` the work commits newest first (they are task-sized), then `uv sync` to
  drop `flechtctl` from the venv again.
- Stack state: the `tmdb-*` topics and the `tmdb_*` tables are local demo state. Drop the tables
  with `curl -s "http://localhost:8123/" --data-binary "DROP TABLE IF EXISTS flechtwerk.<name>"`
  per object listed in task 2 (views first, then tables, then queues); delete the topics in
  Kafbat UI (`http://localhost:8080`) or leave them — `uv run poe clean` wipes everything.
- The keyring and token files under `~/.config/flechtwerk-examples/` belong to the user and are
  never deleted by this plan.
- No branch was created, so there is nothing to dispose of.

## Closeout

Run after the work is committed and the work criteria (1–9) have been re-run and shown to pass,
and before this file is removed. This section is not a decision point. There is no ticket, so
the ticket steps are dropped and nothing is pushed (`closeout_push` is false).

1. Bring this file to its final state: every task checkbox ticked, the Run Log complete, the
   outcome recorded, and `status: done` in the frontmatter.
2. Commit this file on its own — the preservation commit:

   ```bash
   git add 2026-10-23-tmdb-catalog-plan.md
   git commit -m "Record the final state of the TMDB Catalog plan"
   SHA=$(git rev-parse HEAD); echo "$SHA"
   ```

   It is a commit of its own because the final Run Log entries are written after the work
   commits, so they exist in no earlier commit. Do not squash it into a work commit and do not
   delete the file in it.
3. No ticket: there is no permalink to build.
4. No ticket: there is no comment to post.
5. Only now delete this file, in a **separate follow-up commit**:

   ```bash
   git rm 2026-10-23-tmdb-catalog-plan.md
   git commit -m "Remove the TMDB Catalog plan file"
   git status --porcelain
   ```

   Separate is the point: the preservation commit has to stay the last commit in which this file
   exists, because that is where the record of the run lives. Then show `git status --porcelain`
   empty — closeout criterion 11.
6. No ticket: nothing to transition.

If the preservation commit cannot be completed, **leave this file in place** and write the
blocked report — that commit is the only thing that preserves this record once the file is gone.

## Execution Protocol

You are executing this plan file autonomously. There is nobody to ask, so decide from what is
written here plus the repository, and record what you decided.

1. Read this file completely before starting.
2. Run the **Pre-Flight** section first, in your first turn, before changing anything. Show each
   check and its real output. If any check fails, stop there: write the blocked report described
   in step 10, naming the failed check, what was expected, what was actually there, and what has
   to change about the launch. Do not start the tasks, do not substitute a different model or
   mode, and do not proceed at a lower spec — a mismatch discovered here costs a relaunch, and
   the same mismatch discovered at task 5 costs the whole run. When every check passes, set
   `status: executing` in the frontmatter — removing `blocked_report` if this relaunch just
   consumed one, since that key moves with the status — and append the gate's outcome to the Run
   Log — the file now says a run is under way, and a later reopen reads that rather than
   guessing. On a resume, continue at the first unticked task; a task the Run Log shows started
   but not finished is inspected before it is redone, because the tree may already hold part of
   it.
3. Work the tasks in order. Append a line to the Run Log when you start a task, and tick its
   checkbox with another line when you finish it. Do this as you go, not at the end — if this
   run is interrupted, the ticked boxes and the last started-line are how the next one knows
   where to resume and what half-done work it may find. Mirror them in this session's task list
   — TaskCreate and TaskUpdate, or TodoWrite where that is the tool — so that whoever attaches to
   this session or follows it over Remote Control sees the checklist without opening the file:
   create it once the gate has passed, one item per task plus one for the Closeout, with the
   ticked tasks already completed; mark a task in progress when you start it and completed when
   you tick it. The list is a view and ends with this session; this file is the record, and
   where the two disagree the file is right. A session with no task-list tool skips the mirror.
4. Run each task's verification and the Acceptance Criteria for real, and show their output. A
   summary is not evidence.
5. Immediately before the Closeout — after the last work commit — re-run every work criterion in
   the Acceptance Criteria and show every command and result again. The closeout criteria are
   not part of this re-run: they describe the Closeout itself, cannot hold before it, and are
   shown passing as its steps complete, so their not holding here is not a failure. The
   evaluator judging whether you are done reads the condition text and the recent conversation —
   a result proved twenty turns ago may no longer be visible to it, and this file is about to
   stop existing.
6. If reality contradicts the Verified Context, stop and record the contradiction in the Run Log
   before deciding anything. Then proceed only if the plan's Goal still makes sense; otherwise
   treat it as blocked.
7. Commit as you go, on the current branch — `branch` in the frontmatter, which the gate
   confirmed — with no ticket prefix: a task, or a coherent slice of one, per commit, so progress
   is durable and a resume or a rollback has real boundaries to work with. Never create a branch:
   when this plan wanted one, the planning session created it at handoff and this file already
   sits on it.
8. Run the **Closeout** section — after the commit in step 7 and before you remove this file in
   step 9. That order is the whole point: this file is the record of the run and it is about to
   stop existing, so its final state is committed before anything is deleted. If the closeout
   cannot be completed, do not remove this file — write the blocked report in step 10 instead,
   naming the closeout as what failed.
9. When every acceptance criterion passes and the closeout is done, remove this plan file exactly
   as the Closeout section prescribes: in a separate commit after the preservation commit, never
   folded into it and never folded into the work commit. The plan is always tracked, so there is
   no `rm` path.
10. If a criterion cannot be made to pass — or the run cannot finish for any other reason: a
    denied command, a contradiction with the Verified Context that leaves the Goal standing but
    the plan wrong, a run that has to stop clean at a task boundary — write the blocked report at
    **exactly the path named in the goal condition**: `2026-10-23-tmdb-catalog-plan.BLOCKED.md`.
    It names the failing check, what you tried, and why it cannot pass; lists the working tree as
    you leave it — every uncommitted file and the task it belongs to; and opens with a
    frontmatter block of exactly three keys, alphabetically — `date:` the day the run stopped;
    `next:` `relaunch` when the Handoff command can resume once the environment is fixed, or
    `reopen` when the plan itself has to change; and `plan_file:` `2026-10-23-tmdb-catalog-plan.md`.
    Set `status: blocked` **and** `blocked_report: 2026-10-23-tmdb-catalog-plan.BLOCKED.md` in
    this file's frontmatter — the two are written in the same commit and cleared in the same
    turn. Bring the Run Log current, and commit this file and the report together — the blocked
    commit — leaving uncommitted only the work that has not reached a commit boundary. Then
    stop. Getting the path wrong is not cosmetic: the evaluator looks for the path the condition
    names, so a different spelling means the report does not register and the session cannot
    stop. Do not weaken a check, skip it, or declare success without it. A report written by
    mistake — on an estimate rather than a check that ran — is retracted in the same session:
    delete it, say so in the Run Log, and continue.

## Handoff

```bash
claude --model claude-sonnet-5-5 --effort high --permission-mode auto --remote-control 2026-10-23-tmdb-catalog-plan '/goal Execute the plan at 2026-10-23-tmdb-catalog-plan.md to completion. Before changing anything, run the Pre-Flight section of that plan and show the output of every check: the model must be claude-sonnet-5-5, the effort high, the permission mode auto, and every privilege probe must pass. If any pre-flight check fails, write the blocked report named below and stop - do not start the tasks and do not continue on a different model or mode. Done when all of the following hold: the Pre-Flight checks were run first in this session and passed; every task checkbox in that file was ticked before the file was removed; uv run poe test was run in this session, exited 0 and reported more than 421 passed and no failures; uv run pytest -m integration examples/tmdb_catalog -q was run and reported 2 passed; uv run flechtctl --version printed flechtctl 0.17.0; uv run poe --help | grep -c tmdb printed 6; jq -r .uid grafana/dashboards/tmdb-catalog.json printed flechtwerk-tmdb; grep -c "host.docker.internal:9124" prometheus/prometheus.yml printed 1; the ClickHouse count query over flechtwerk.tmdb_movies at http://localhost:8123 printed a number of at least 100 and the same query over flechtwerk.tmdb_credits printed a number of at least 500; the Grafana dashboard uid flechtwerk-tmdb answered HTTP 200 at http://localhost:3000/api/dashboards/uid/flechtwerk-tmdb; grep -c "only example needing" examples/wildfire_watch/CLAUDE.md printed 0; every remaining work criterion in the Acceptance Criteria section of that plan was run in this session, its command and output shown, and passed; all of those work criteria were re-run in full immediately before the Closeout with their commands and outputs shown; the final state of the plan file was committed on its own in this session, and only then was the plan file deleted in a separate later commit; git status --porcelain is empty; and all work is committed on the current branch main with no ticket prefix. Do not create a branch. Do not push. Do not relax, skip, or declare passed any check that was not actually run, and do not route around a permission denial - a refused command is a blocked-report situation. If a pre-flight or acceptance check cannot be made to pass, write 2026-10-23-tmdb-catalog-plan.BLOCKED.md in this session, naming a check this session ran, what was tried, and why it cannot pass, commit it with the plan, then stop - that also satisfies this goal. A blocked report left by an earlier launch does not.'
```

`/pdca:execute 2026-10-23-tmdb-catalog-plan.md` runs that command as a background session with
Remote Control, so nothing needs pasting. If this plan file has been sitting for a while, do not
launch it blindly — reopen it first with `/pdca:plan 2026-10-23-tmdb-catalog-plan.md` to re-verify
the Verified Context and get a freshened handoff. The condition inlines work criteria 1 to 9
except the second half of criterion 9 (`grep -c tmdb_catalog README.md CLAUDE.md`), which the
"every remaining work criterion" clause covers.

Short form, for typing into a session already started with the flags above. The evaluator then
holds only the plan path, the protocol and the escape hatch — not the acceptance criteria — so it
has to trust this session's own account of what it ran. Prefer the full command; use this when
the full one is not to hand. A bare `/goal 2026-10-23-tmdb-catalog-plan.md` is not a launch at
all: the evaluator cannot open the file, so it would have nothing to judge.

```
/goal Execute the plan at 2026-10-23-tmdb-catalog-plan.md to completion per its Execution Protocol: Pre-Flight first, every Acceptance Criterion shown passing in a final re-run, the Closeout done before the file was removed in a separate later commit; or 2026-10-23-tmdb-catalog-plan.BLOCKED.md was written in this session.
```

## Run Log

(Appended by the execution session.)

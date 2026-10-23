-- ClickHouse sink for the TMDB Catalog example.
--
-- Like ADS-B / SMARD / GTFS / Odds / Wildfire, this takes the Kafka-engine SHORTCUT:
-- ClickHouse's own Kafka table engine consumes the output topics directly and materialized
-- views land the rows (no Flechtwerk sink stage). The stack configures the Kafka engine to read
-- COMMITTED (clickhouse/config/kafka.xml), so aborted pages from a crash or handover are never
-- ingested — required because the upstream stages are transactional (EOS) producers.
--
-- SCHEMALESS INGEST (as ADS-B / SMARD / Odds / Wildfire): each message is read whole into one
-- JSON column (`kafka_format = 'JSONAsObject'`); the views PROMOTE the columns the Grafana board
-- reads into typed columns and keep the whole message in a `payload JSON` catch-all, so a field
-- TMDB adds tomorrow (it already sends an undocumented `softcore`) is queryable as
-- `payload.<field>` with no DDL change.
--
-- CASTS, as proved against ClickHouse 25.8. Fields TMDB always sends use `::UInt64` / `::UInt32` /
-- `::UInt16` / `::UInt8` / `::Float64` / `::Bool` (an absent or null one lands as 0 / false, never
-- fails); fields where absence is meaningful (`runtime`, `billing_order`, `episode_count`, ids)
-- use `::Nullable(…)`; TMDB's empty-string dates go through `nullIf(…::String, '')`; name lists
-- are `::Array(String)` (an absent list is `[]`). `popularity` and `vote_average` are NEVER cast to
-- an integer type — a float cast to an integer truncates. There is NO `Map` column anywhere:
-- `message.certifications::Map(String,String)` silently returns `{}`, so certifications and
-- content ratings stay inside `payload` and are read as `payload.certifications.US` subcolumns.
--
-- HISTORY vs CURRENT (the GTFS idiom). Each entity topic feeds two tables from the same rows: an
-- append-only history (`tmdb_movies`, ordered by `(id, version_at)` — the popularity timeline and
-- every change) and a current-state table (`tmdb_movies_current`, a ReplacingMergeTree queried
-- FINAL). Both replace/order by the version COUNTER, not `version_at`: that comes from the HTTP
-- `Date` header at one-second resolution, so two versions inside one second would tie.
--
-- CREDITS are never retracted: `tmdb_credits` holds the union of every credit ever seen for a
-- title within the TTL (replaced by `(media_type, media_id, credit_id)` on the title's version
-- counter), so a credit TMDB later removed keeps its last version's row.
--
-- WHY THE 180-DAY TTL. TMDB's API terms forbid caching longer than six months. Every `tmdb_*`
-- table carries a TTL of 180 days on its row's own clock, which enforces that by construction.
--
-- `tmdb_changes` records an id listed while its day was open and again when the day closed as two
-- rows, which is why the dashboard counts `uniqExact(id)`.

-- ========================== change feed ==========================

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_changes_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'tmdb-changes',
    kafka_group_name = 'tmdb-changes-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_changes
(
    feed LowCardinality(String),
    id UInt64,
    changed_on Date,
    listing LowCardinality(String),
    discovered_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (feed, changed_on, id)
TTL toDateTime(discovered_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_changes_mv
TO flechtwerk.tmdb_changes
AS SELECT
    message.feed::String AS feed,
    message.id::UInt64 AS id,
    toDate(message.changed_on::String) AS changed_on,
    message.listing::String AS listing,
    parseDateTime64BestEffort(message.discovered_at::String, 3) AS discovered_at
FROM flechtwerk.tmdb_changes_queue;

-- ========================== movies ==========================

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_movies_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'tmdb-movies',
    kafka_group_name = 'tmdb-movies-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- History: every version of every movie.
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_movies
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    title String,
    original_title String,
    original_language LowCardinality(String),
    release_date Nullable(String),
    runtime Nullable(UInt32),
    budget UInt64,
    revenue UInt64,
    popularity Float64,
    vote_average Float64,
    vote_count UInt32,
    status LowCardinality(String),
    adult Bool,
    imdb_id Nullable(String),
    collection Nullable(String),
    genres Array(String),
    keywords Array(String),
    production_companies Array(String),
    production_countries Array(String),
    spoken_languages Array(String),
    origin_country Array(String),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (id, version_at)
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_movies_mv
TO flechtwerk.tmdb_movies
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.title::String AS title,
    message.original_title::String AS original_title,
    message.original_language::String AS original_language,
    nullIf(message.release_date::String, '') AS release_date,
    message.runtime::Nullable(UInt32) AS runtime,
    message.budget::UInt64 AS budget,
    message.revenue::UInt64 AS revenue,
    message.popularity::Float64 AS popularity,
    message.vote_average::Float64 AS vote_average,
    message.vote_count::UInt32 AS vote_count,
    message.status::String AS status,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.collection::Nullable(String) AS collection,
    message.genres::Array(String) AS genres,
    message.keywords::Array(String) AS keywords,
    message.production_companies::Array(String) AS production_companies,
    message.production_countries::Array(String) AS production_countries,
    message.spoken_languages::Array(String) AS spoken_languages,
    message.origin_country::Array(String) AS origin_country,
    message AS payload
FROM flechtwerk.tmdb_movies_queue;

-- Current: the latest version per id (query FINAL).
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_movies_current
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    title String,
    original_title String,
    original_language LowCardinality(String),
    release_date Nullable(String),
    runtime Nullable(UInt32),
    budget UInt64,
    revenue UInt64,
    popularity Float64,
    vote_average Float64,
    vote_count UInt32,
    status LowCardinality(String),
    adult Bool,
    imdb_id Nullable(String),
    collection Nullable(String),
    genres Array(String),
    keywords Array(String),
    production_companies Array(String),
    production_countries Array(String),
    spoken_languages Array(String),
    origin_country Array(String),
    payload JSON
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_movies_current_mv
TO flechtwerk.tmdb_movies_current
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.title::String AS title,
    message.original_title::String AS original_title,
    message.original_language::String AS original_language,
    nullIf(message.release_date::String, '') AS release_date,
    message.runtime::Nullable(UInt32) AS runtime,
    message.budget::UInt64 AS budget,
    message.revenue::UInt64 AS revenue,
    message.popularity::Float64 AS popularity,
    message.vote_average::Float64 AS vote_average,
    message.vote_count::UInt32 AS vote_count,
    message.status::String AS status,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.collection::Nullable(String) AS collection,
    message.genres::Array(String) AS genres,
    message.keywords::Array(String) AS keywords,
    message.production_companies::Array(String) AS production_companies,
    message.production_countries::Array(String) AS production_countries,
    message.spoken_languages::Array(String) AS spoken_languages,
    message.origin_country::Array(String) AS origin_country,
    message AS payload
FROM flechtwerk.tmdb_movies_queue;

-- ========================== TV series ==========================

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_tv_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'tmdb-tv',
    kafka_group_name = 'tmdb-tv-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- History: every version of every TV series.
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_tv
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    name String,
    original_name String,
    original_language LowCardinality(String),
    first_air_date Nullable(String),
    last_air_date Nullable(String),
    status LowCardinality(String),
    type LowCardinality(String),
    in_production Bool,
    number_of_seasons UInt16,
    number_of_episodes UInt32,
    popularity Float64,
    vote_average Float64,
    vote_count UInt32,
    adult Bool,
    imdb_id Nullable(String),
    genres Array(String),
    keywords Array(String),
    networks Array(String),
    production_companies Array(String),
    production_countries Array(String),
    origin_country Array(String),
    spoken_languages Array(String),
    created_by Array(String),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (id, version_at)
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_tv_mv
TO flechtwerk.tmdb_tv
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.name::String AS name,
    message.original_name::String AS original_name,
    message.original_language::String AS original_language,
    nullIf(message.first_air_date::String, '') AS first_air_date,
    nullIf(message.last_air_date::String, '') AS last_air_date,
    message.status::String AS status,
    message.type::String AS type,
    message.in_production::Bool AS in_production,
    message.number_of_seasons::UInt16 AS number_of_seasons,
    message.number_of_episodes::UInt32 AS number_of_episodes,
    message.popularity::Float64 AS popularity,
    message.vote_average::Float64 AS vote_average,
    message.vote_count::UInt32 AS vote_count,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.genres::Array(String) AS genres,
    message.keywords::Array(String) AS keywords,
    message.networks::Array(String) AS networks,
    message.production_companies::Array(String) AS production_companies,
    message.production_countries::Array(String) AS production_countries,
    message.origin_country::Array(String) AS origin_country,
    message.spoken_languages::Array(String) AS spoken_languages,
    message.created_by::Array(String) AS created_by,
    message AS payload
FROM flechtwerk.tmdb_tv_queue;

-- Current: the latest version per id (query FINAL).
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_tv_current
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    name String,
    original_name String,
    original_language LowCardinality(String),
    first_air_date Nullable(String),
    last_air_date Nullable(String),
    status LowCardinality(String),
    type LowCardinality(String),
    in_production Bool,
    number_of_seasons UInt16,
    number_of_episodes UInt32,
    popularity Float64,
    vote_average Float64,
    vote_count UInt32,
    adult Bool,
    imdb_id Nullable(String),
    genres Array(String),
    keywords Array(String),
    networks Array(String),
    production_companies Array(String),
    production_countries Array(String),
    origin_country Array(String),
    spoken_languages Array(String),
    created_by Array(String),
    payload JSON
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_tv_current_mv
TO flechtwerk.tmdb_tv_current
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.name::String AS name,
    message.original_name::String AS original_name,
    message.original_language::String AS original_language,
    nullIf(message.first_air_date::String, '') AS first_air_date,
    nullIf(message.last_air_date::String, '') AS last_air_date,
    message.status::String AS status,
    message.type::String AS type,
    message.in_production::Bool AS in_production,
    message.number_of_seasons::UInt16 AS number_of_seasons,
    message.number_of_episodes::UInt32 AS number_of_episodes,
    message.popularity::Float64 AS popularity,
    message.vote_average::Float64 AS vote_average,
    message.vote_count::UInt32 AS vote_count,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.genres::Array(String) AS genres,
    message.keywords::Array(String) AS keywords,
    message.networks::Array(String) AS networks,
    message.production_companies::Array(String) AS production_companies,
    message.production_countries::Array(String) AS production_countries,
    message.origin_country::Array(String) AS origin_country,
    message.spoken_languages::Array(String) AS spoken_languages,
    message.created_by::Array(String) AS created_by,
    message AS payload
FROM flechtwerk.tmdb_tv_queue;

-- ========================== people ==========================

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_people_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'tmdb-people',
    kafka_group_name = 'tmdb-people-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

-- History: every version of every people.
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_people
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    name String,
    gender UInt8,
    known_for_department LowCardinality(String),
    birthday Nullable(String),
    deathday Nullable(String),
    place_of_birth Nullable(String),
    popularity Float64,
    adult Bool,
    imdb_id Nullable(String),
    also_known_as Array(String),
    payload JSON
)
ENGINE = MergeTree
ORDER BY (id, version_at)
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_people_mv
TO flechtwerk.tmdb_people
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.name::String AS name,
    message.gender::UInt8 AS gender,
    ifNull(message.known_for_department::Nullable(String), '') AS known_for_department,
    nullIf(message.birthday::String, '') AS birthday,
    nullIf(message.deathday::String, '') AS deathday,
    message.place_of_birth::Nullable(String) AS place_of_birth,
    message.popularity::Float64 AS popularity,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.also_known_as::Array(String) AS also_known_as,
    message AS payload
FROM flechtwerk.tmdb_people_queue;

-- Current: the latest version per id (query FINAL).
CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_people_current
(
    id UInt64,
    version UInt32,
    version_at DateTime64(3, 'UTC'),
    changed_on Date,
    name String,
    gender UInt8,
    known_for_department LowCardinality(String),
    birthday Nullable(String),
    deathday Nullable(String),
    place_of_birth Nullable(String),
    popularity Float64,
    adult Bool,
    imdb_id Nullable(String),
    also_known_as Array(String),
    payload JSON
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_people_current_mv
TO flechtwerk.tmdb_people_current
AS SELECT
    message.id::UInt64 AS id,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at,
    toDate(message.changed_on::String) AS changed_on,
    message.name::String AS name,
    message.gender::UInt8 AS gender,
    ifNull(message.known_for_department::Nullable(String), '') AS known_for_department,
    nullIf(message.birthday::String, '') AS birthday,
    nullIf(message.deathday::String, '') AS deathday,
    message.place_of_birth::Nullable(String) AS place_of_birth,
    message.popularity::Float64 AS popularity,
    message.adult::Bool AS adult,
    message.imdb_id::Nullable(String) AS imdb_id,
    message.also_known_as::Array(String) AS also_known_as,
    message AS payload
FROM flechtwerk.tmdb_people_queue;

-- ========================== credits ==========================

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_credits_queue
(
    message JSON
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'tmdb-credits',
    kafka_group_name = 'tmdb-credits-clickhouse',
    kafka_format = 'JSONAsObject',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS flechtwerk.tmdb_credits
(
    media_type LowCardinality(String),
    media_id UInt64,
    credit_id String,
    kind LowCardinality(String),
    person_id UInt64,
    name String,
    gender UInt8,
    known_for_department LowCardinality(String),
    person_popularity Float64,
    character Nullable(String),
    billing_order Nullable(UInt16),
    department Nullable(String),
    job Nullable(String),
    episode_count Nullable(UInt32),
    version UInt32,
    version_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (media_type, media_id, credit_id)
TTL toDateTime(version_at) + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS flechtwerk.tmdb_credits_mv
TO flechtwerk.tmdb_credits
AS SELECT
    message.media_type::String AS media_type,
    message.media_id::UInt64 AS media_id,
    message.credit_id::String AS credit_id,
    message.kind::String AS kind,
    message.person_id::UInt64 AS person_id,
    message.name::String AS name,
    message.gender::UInt8 AS gender,
    ifNull(message.known_for_department::Nullable(String), '') AS known_for_department,
    message.person_popularity::Float64 AS person_popularity,
    message.character::Nullable(String) AS character,
    message.billing_order::Nullable(UInt16) AS billing_order,
    message.department::Nullable(String) AS department,
    message.job::Nullable(String) AS job,
    message.episode_count::Nullable(UInt32) AS episode_count,
    message.version::UInt32 AS version,
    parseDateTime64BestEffort(message.version_at::String, 3) AS version_at
FROM flechtwerk.tmdb_credits_queue;

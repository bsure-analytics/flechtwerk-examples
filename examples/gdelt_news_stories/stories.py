"""GDELT stories — online clustering of GKG articles into stories, in keyed state.

Stage 2b. It consumes ``gdelt-gkg-raw`` (one enriched article per record) and groups
articles covering the same event into **stories**, emitting an updated story record to
``gdelt-stories`` whenever a cluster changes. Each story carries its article count
(breaking-news velocity), the distinct outlet domains and — annotated from the
``gdelt-outlets`` config table — the distinct outlet *countries* covering it (the
coverage-spread lens: a story only one country's outlets carry vs. a global wire story).

**Clustering lives in keyed state, and the key is a constant.** Online clustering needs
every article visible to every cluster, so all articles must land on one task. Two things
make that so: ``gdelt-gkg-raw`` is a **single-partition** topic (ingest keys it by URL but
its one partition puts every record on one task), and this stage overrides
``extract_state_key`` to a **constant** so every article folds into one state bucket holding
*all* live clusters. That is the honest limit of this design — documented in the README —
and the right one at this feed's volume (~1–2k GKG records / 15 min). The production path for
higher volume is a blocking key (dominant country or top theme) that shards clusters across
partitions; we deliberately do not build it.

**Similarity is thresholded, never exact.** GDELT's entity tags are noisy — the machine-
translated feed especially coins junk "entities" ("a court", "a mission") — so the feature
set is **persons ∪ organizations only** (broad GKG *themes* are excluded: they recur across
unrelated articles and act as merge magnets — the plan's own "clustering must use thresholded
similarity, not exact entity matching" caution, taken to its conclusion), noise tokens
(leading-article phrases, very short tokens) are dropped, and a new article joins a cluster
only when it shares **at least ``MIN_SHARED_FEATURES``** features *and* the **overlap
coefficient** (``|A∩B| / min(|A|,|B|)``) is at or above ``SIMILARITY_THRESHOLD``. The absolute
floor is what stops a thin article from being sucked into a big cluster on one or two generic
tokens (the overlap ratio alone would let it). A re-crawled URL (GDELT relists syndicated
content) is **deduplicated** by a per-URL seen-map, and both the cluster set and the seen-map
are **TTL-pruned** (48 h idle) *and hard-capped* (``MAX_CLUSTERS`` / ``MAX_SEEN_URLS``) so the
single state bucket stays bounded: all clusters share ONE changelog record, and Kafka caps a
record at ~1 MB, so the bucket is kept comfortably under that — the concrete face of the
single-partition limit (shard with a blocking key for higher volume; see the README).

The clustering is the pure functions :func:`assign` / :func:`prune` / :func:`story_record`;
the stage is the thin shell that extracts features, resolves the outlet country from
``self.configs``, and delegates.
"""
import hashlib
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

from flechtwerk import Config, Event, IncomingMessage, Message, State, Transformer
from flechtwerk.attribute import Record

from .ingest import GKG_RAW_TOPIC
from .outlets import OUTLETS_TOPIC
from .parsers import parse_entities, parse_tone
from .schema import (
    ARTICLE_COUNT,
    AVG_STORY_TONE,
    CLUSTER_COUNTRIES,
    CLUSTER_DOMAINS,
    CLUSTER_FEATURES,
    CLUSTER_TONE_SUM,
    CLUSTERS,
    COUNTRIES,
    COUNTRY_COUNT,
    DOCUMENT_IDENTIFIER,
    FILE_TS,
    FIRST_SEEN,
    LAST_SEEN,
    METADATA,
    OUTLET_COUNTRY,
    ROW,
    SAMPLE_TITLE,
    SEEN_URLS,
    SOURCE_COMMON_NAME,
    SOURCE_DOMAINS,
    STORY_ID,
    TOP_ENTITIES,
    V2_ENHANCED_ORGANIZATIONS,
    V2_ENHANCED_PERSONS,
    V2_TONE,
)

log = logging.getLogger(__name__)

STORIES_TOPIC = "gdelt-stories"
CLUSTER_BUCKET = "clusters"
"""The single state key every article folds into (see the module docstring)."""

# Country-code TLD → ISO country, for deriving an outlet's home country at runtime (so the
# config table only needs the non-derivable gTLD outlets). News-relevant ccTLDs only;
# vanity/ambiguous ones (co, io, me, tv, ai, ly, to, fm, cc, ws) and gTLDs are absent, so a
# ``.io`` or ``.com`` domain derives nothing and falls to the config override (or stays
# unannotated). ``.eu`` maps to EU, matching the config convention.
CCTLD_COUNTRY: dict[str, str] = {
    "uk": "GB", "au": "AU", "nz": "NZ", "za": "ZA", "ie": "IE", "ca": "CA", "in": "IN",
    "pk": "PK", "bd": "BD", "lk": "LK", "np": "NP", "ph": "PH", "my": "MY", "sg": "SG",
    "id": "ID", "th": "TH", "vn": "VN", "cn": "CN", "jp": "JP", "kr": "KR", "tw": "TW",
    "hk": "HK", "de": "DE", "fr": "FR", "es": "ES", "it": "IT", "nl": "NL", "be": "BE",
    "ch": "CH", "at": "AT", "se": "SE", "no": "NO", "dk": "DK", "fi": "FI", "is": "IS",
    "pt": "PT", "gr": "GR", "pl": "PL", "cz": "CZ", "sk": "SK", "hu": "HU", "ro": "RO",
    "bg": "BG", "hr": "HR", "si": "SI", "rs": "RS", "ua": "UA", "ru": "RU", "by": "BY",
    "lt": "LT", "lv": "LV", "ee": "EE", "tr": "TR", "il": "IL", "ae": "AE", "sa": "SA",
    "qa": "QA", "kw": "KW", "eg": "EG", "ma": "MA", "ng": "NG", "ke": "KE", "gh": "GH",
    "tz": "TZ", "ug": "UG", "br": "BR", "ar": "AR", "mx": "MX", "cl": "CL", "pe": "PE",
    "ve": "VE", "ec": "EC", "uy": "UY", "eu": "EU",
}


def country_from_tld(domain: str) -> str | None:
    """Derive an outlet's country from its ccTLD (``bbc.co.uk`` → GB), or ``None`` for a
    gTLD/vanity TLD whose country isn't encoded in the domain. Pure — no data, no I/O."""
    return CCTLD_COUNTRY.get(domain.rsplit(".", 1)[-1]) if "." in domain else None

SIMILARITY_THRESHOLD = 0.5
"""Overlap-coefficient floor to join an existing cluster (tune with the fixture). Overlap
(``|A∩B| / min(|A|,|B|)``) not Jaccard, so a small article still matches a grown cluster —
paired with ``MIN_SHARED_FEATURES`` so the ratio alone can't create merge magnets."""
MIN_SHARED_FEATURES = 3
"""Minimum number of features an article must share with a cluster to join it, on top of the
ratio. Without this floor the overlap coefficient would let a 2-feature article join any big
cluster containing those two (generic) tokens — the mega-cluster failure mode on noisy data."""
MIN_FEATURES = 2
"""An article with fewer than this many features is too thin to cluster reliably — it is
marked seen (so it isn't reconsidered) but spawns no story."""
MAX_CLUSTER_FEATURES = 20
"""Cap on a cluster's representative feature set, so a long-running story can't grow it
without bound (the union keeps the most recently added features) — also keeps the single
state bucket small (see ``MAX_CLUSTERS``)."""
MAX_CLUSTERS = 800
"""Hard cap on live clusters kept in the single state bucket, least-recently-seen evicted
beyond it. All clusters share ONE changelog record, and Kafka caps a record at ~1 MB, so the
bucket must stay bounded — this (plus ``MAX_SEEN_URLS`` and the small per-cluster footprint)
keeps it comfortably under that. It is the same limit the README's single-partition note is
about: at higher volume you'd shard clusters across partitions with a blocking key."""
MAX_SEEN_URLS = 4000
"""Hard cap on the dedup seen-set (least-recently-seen evicted), for the same bound. URLs are
stored as short hashes (:func:`_url_key`), not the full URL, to keep the record compact."""
_NOISE_PREFIXES = ("a ", "an ", "the ")
"""Leading-article prefixes that mark a junk entity from machine translation ("a court",
"a mission") — dropped from the feature set so they can't glue unrelated articles together."""
_MIN_TOKEN_LEN = 4
"""Drop features shorter than this — too generic to be a discriminating entity."""


def _meaningful(token: str) -> bool:
    """Whether a parsed entity token is a usable feature (not article-led junk / too short)."""
    return len(token) >= _MIN_TOKEN_LEN and not token.startswith(_NOISE_PREFIXES)
STORY_TTL = timedelta(hours=48)
"""Evict a cluster whose newest article is older than this — bounds the state bucket."""
SEEN_TTL = timedelta(hours=48)
"""Forget a seen URL after this, so the dedup map stays bounded (a much-later relist that
somehow recurs is treated as fresh)."""


def article_features(row: Record) -> set[str]:
    """The similarity feature set for one GKG article: its persons ∪ organizations.

    Parsed and lowercased by :func:`.parsers.parse_entities`, then filtered of article-led /
    too-short junk tokens (:func:`_meaningful`). Themes are deliberately excluded — broad and
    shared across unrelated articles, they over-merge (see the module docstring).
    """
    entities = set(parse_entities(row.get(V2_ENHANCED_PERSONS)))
    entities |= set(parse_entities(row.get(V2_ENHANCED_ORGANIZATIONS)))
    return {token for token in entities if _meaningful(token)}


def similarity(a: set[str], b: set[str]) -> float:
    """Overlap coefficient of two feature sets: ``|A∩B| / min(|A|,|B|)`` (0 if either empty)."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def story_id(url: str, file_ts: datetime) -> str:
    """A stable cluster id: a hash of the first article's URL + the slice timestamp."""
    return hashlib.sha1(f"{url}@{file_ts.isoformat()}".encode()).hexdigest()[:16]


def _url_key(url: str) -> str:
    """A short, fixed-size dedup key for a URL — keeps the seen-set changelog record compact."""
    return hashlib.sha1(url.encode()).hexdigest()[:16]


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def cap(clusters: dict[str, Record], seen: dict[str, datetime]) -> None:
    """Bound the single state bucket in place: keep the most-recently-seen ``MAX_CLUSTERS``
    clusters and ``MAX_SEEN_URLS`` dedup entries, evicting the rest. This is what guarantees
    the (single) changelog record stays under Kafka's ~1 MB limit regardless of feed volume;
    an evicted cluster's story was already emitted, so ClickHouse keeps it — only its
    in-memory clustering context is dropped (a later article may re-spawn it)."""
    if len(clusters) > MAX_CLUSTERS:
        kept = sorted(clusters.items(), key=lambda kv: kv[1].get(LAST_SEEN) or _EPOCH, reverse=True)[:MAX_CLUSTERS]
        clusters.clear()
        clusters.update(kept)
    if len(seen) > MAX_SEEN_URLS:
        kept_seen = sorted(seen.items(), key=lambda kv: kv[1], reverse=True)[:MAX_SEEN_URLS]
        seen.clear()
        seen.update(kept_seen)


def _new_cluster(features: set[str], domain: str, country: str | None, tone: float | None,
                 file_ts: datetime, url: str) -> Record:
    cluster = Record({
        CLUSTER_FEATURES: set(list(features)[:MAX_CLUSTER_FEATURES]),
        ARTICLE_COUNT: 1,
        CLUSTER_DOMAINS: {domain} if domain else set(),
        CLUSTER_COUNTRIES: {country} if country else set(),
        CLUSTER_TONE_SUM: float(tone) if tone is not None else 0.0,
        FIRST_SEEN: file_ts,
        LAST_SEEN: file_ts,
        SAMPLE_TITLE: url,
    })
    return cluster


def _joined_cluster(cluster: Record, features: set[str], domain: str, country: str | None,
                    tone: float | None, file_ts: datetime) -> Record:
    merged_features = set(cluster.get(CLUSTER_FEATURES) or set()) | features
    domains = set(cluster.get(CLUSTER_DOMAINS) or set())
    if domain:
        domains.add(domain)
    countries = set(cluster.get(CLUSTER_COUNTRIES) or set())
    if country:
        countries.add(country)
    return Record({
        CLUSTER_FEATURES: set(list(merged_features)[:MAX_CLUSTER_FEATURES]),
        ARTICLE_COUNT: (cluster.get(ARTICLE_COUNT) or 0) + 1,
        CLUSTER_DOMAINS: domains,
        CLUSTER_COUNTRIES: countries,
        CLUSTER_TONE_SUM: (cluster.get(CLUSTER_TONE_SUM) or 0.0) + (float(tone) if tone is not None else 0.0),
        FIRST_SEEN: cluster.get(FIRST_SEEN) or file_ts,
        LAST_SEEN: file_ts,
        SAMPLE_TITLE: cluster.get(SAMPLE_TITLE),
    })


def prune(clusters: dict[str, Record], seen: dict[str, datetime], now: datetime) -> None:
    """Evict clusters idle past ``STORY_TTL`` and seen URLs past ``SEEN_TTL`` (in place),
    so the single state bucket stays bounded to genuinely live stories."""
    for sid in [s for s, c in clusters.items() if now - (c.get(LAST_SEEN) or now) > STORY_TTL]:
        del clusters[sid]
    for url in [u for u, ts in seen.items() if now - ts > SEEN_TTL]:
        del seen[url]


def assign(clusters: dict[str, Record], seen: dict[str, datetime], *, url: str, features: set[str],
           domain: str, country: str | None, tone: float | None, file_ts: datetime) -> str | None:
    """Assign one article to a cluster (join the best match, or spawn a new one).

    Returns the affected story id, or ``None`` when the article is skipped — a duplicate URL
    (dedup) or too few features to cluster. Mutates ``clusters`` and ``seen`` in place. Pure
    (no framework machinery, no I/O), so the logic tier drives it directly.
    """
    url_key = _url_key(url)
    if url_key in seen:
        return None  # re-crawl / syndication relist — already clustered
    if len(features) < MIN_FEATURES:
        seen[url_key] = file_ts  # too thin to cluster, but don't reconsider it
        return None

    best_id, best_sim = None, 0.0
    for sid, cluster in clusters.items():
        cluster_features = set(cluster.get(CLUSTER_FEATURES) or set())
        sim = similarity(features, cluster_features)
        # Require an absolute overlap floor as well as the ratio: the overlap coefficient
        # alone would let a thin article join a big cluster on one or two generic tokens.
        if sim > best_sim and len(features & cluster_features) >= MIN_SHARED_FEATURES:
            best_id, best_sim = sid, sim

    if best_id is not None and best_sim >= SIMILARITY_THRESHOLD:
        sid = best_id
        clusters[sid] = _joined_cluster(clusters[sid], features, domain, country, tone, file_ts)
    else:
        sid = story_id(url, file_ts)
        clusters[sid] = _new_cluster(features, domain, country, tone, file_ts, url)
    seen[url_key] = file_ts
    return sid


def story_record(sid: str, cluster: Record) -> Event:
    """Project a cluster into a ``gdelt-stories`` output record (sets → sorted lists)."""
    count = cluster.get(ARTICLE_COUNT) or 0
    countries = sorted(cluster.get(CLUSTER_COUNTRIES) or set())
    record = Event({
        STORY_ID: sid,
        ARTICLE_COUNT: count,
        SOURCE_DOMAINS: sorted(cluster.get(CLUSTER_DOMAINS) or set()),
        COUNTRIES: countries,
        COUNTRY_COUNT: len(countries),
        TOP_ENTITIES: sorted(cluster.get(CLUSTER_FEATURES) or set()),
        FIRST_SEEN: cluster[FIRST_SEEN],
        LAST_SEEN: cluster[LAST_SEEN],
    })
    if count and (tone_sum := cluster.get(CLUSTER_TONE_SUM)) is not None:
        record[AVG_STORY_TONE] = tone_sum / count
    if sample := cluster.get(SAMPLE_TITLE):
        record[SAMPLE_TITLE] = sample
    return record


class GdeltStories(Transformer):
    """Clusters ``gdelt-gkg-raw`` articles into stories, annotating coverage from outlets.

    ``@transformer`` won't do here: the stage overrides ``extract_state_key`` (constant
    bucket) and declares a config topic, so it is a subclass — but it owns no resource, so
    ``__aenter__``/``__aexit__`` stay the defaults.
    """

    input_topics = [GKG_RAW_TOPIC]
    config_topics = [OUTLETS_TOPIC]

    def extract_state_key(self, msg: IncomingMessage) -> str:
        return CLUSTER_BUCKET  # every article → one bucket, so all clusters are co-visible

    def _country(self, domain: str) -> str | None:
        """The outlet's home country: a ``gdelt-outlets`` override if present, else derived
        from the domain's country-code TLD (:func:`country_from_tld`).

        Country is a pure function of a ccTLD (``.co.uk`` → GB), so those need no data — they
        are computed at runtime, covering every ccTLD domain whether or not we've seen it. The
        config table carries only what *can't* be derived: gTLD outlets (``nytimes.com`` → US)
        whose country is editorial knowledge. Unknown gTLD domains stay ``None`` (unannotated).
        """
        outlet = self.configs.get(domain)
        if outlet is not None and (country := outlet.get(OUTLET_COUNTRY)):
            return country
        return country_from_tld(domain)

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        row = msg.value[ROW]
        file_ts = msg.value[METADATA][FILE_TS]
        url = row[DOCUMENT_IDENTIFIER]
        domain = (row.get(SOURCE_COMMON_NAME) or "").strip().lower()
        tone = (parse_tone(row.get(V2_TONE)) or {}).get("tone")

        clusters = dict(state.get(CLUSTERS) or {})
        seen = dict(state.get(SEEN_URLS) or {})
        prune(clusters, seen, file_ts)
        sid = assign(clusters, seen, url=url, features=article_features(row), domain=domain,
                     country=self._country(domain), tone=tone, file_ts=file_ts)
        cap(clusters, seen)  # bound the single bucket so its changelog record stays < ~1 MB
        if sid is not None and sid in clusters:  # (a just-created cluster is never capped out)
            yield Message(key=sid, topic=STORIES_TOPIC, value=story_record(sid, clusters[sid]))
        yield State({CLUSTERS: clusters, SEEN_URLS: seen})


stage = GdeltStories()
"""The stage the dispatcher runs (``python -m examples.gdelt_news_stories stories``)."""

"""The pure half of the TMDB Catalog — URL building, change-page parsing, projection, hashing.

Nothing here touches the framework, the network or a clock, so the logic tier drives it straight
off the captured fixtures (``tests/fixtures/``) and the stages stay thin: ``changes`` pages
through :func:`changes_path` (and reads nothing else), ``split`` parses each page with
:func:`parse_changes_page`, ``catalog`` fetches :func:`details_path` and runs the matching
projector.

**Spread through, declare only what you compute.** TMDB's responses are a foreign schema that
grows (``softcore`` appeared undocumented). Each projector therefore starts from the response
*as delivered* and changes only what it must: it drops the appended sub-objects it has
re-shaped, flattens ``[{"id": 18, "name": "Drama"}]`` lists to their names, and lifts the few
maps and ids a downstream store wants at the top level. Everything else — including fields that
don't exist yet — passes through untouched.

**Credits are a fan-out, not a field.** A movie's ``credits`` (and a series' ``aggregate_credits``)
are removed from the flat row and returned separately as one dict per credit, so the catalog
emits one narrow row per person-on-title instead of a 90-element array nobody can join on.

**Versioning is a content hash** (:func:`content_hash`) over the flat row and its credits,
computed *before* the version fields exist. ``popularity`` is deliberately inside it: TMDB
recomputes it daily, and a new version per move *is* the popularity history.
"""
import hashlib
import json
import logging
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
"""The TMDB v3 API host. The demo constant; injectable for tests via the stages' ``base_url``."""

APPEND = {
    "movie": "credits,keywords,release_dates,external_ids",
    "tv": "aggregate_credits,keywords,content_ratings,external_ids",
    "person": "external_ids",
}
"""What each feed's detail call appends, so one request returns the entity with its credits,
keywords, ratings and external ids — a movie is ~90 KB, a person ~3 KB."""

PAGE_SIZE = 100
"""Ids per change-feed page (TMDB's fixed page size)."""

MAX_PAGES = 500
"""TMDB's hard cap on ``page`` — the reason the extractor lists one calendar day per window:
a 14-day people window is ~600 pages, over the cap, while one day is ≤ ~90."""

DEFAULT_LOOKBACK_DAYS = 90
"""How far back a feed with no ``since`` starts."""

SEEN_HARD_CAP = 60_000
"""Maximum ids kept in the open-day seen-set — enforced unconditionally by :func:`prune_seen`.

Each ``State`` is ONE changelog record with a ~1 MB ceiling (the lesson ``wildfire_watch`` and
GDELT taught the hard way): 60 000 ids × ~9 bytes of JSON ≈ 540 KB. A busy day lists ~9 000
movies, so the cap is ~6× what the open day should ever hold."""

_APPENDED = {
    "movie": ("credits", "keywords", "release_dates", "external_ids"),
    "tv": ("aggregate_credits", "keywords", "content_ratings", "external_ids"),
    "person": ("external_ids",),
}


def details_path(feed: str, tmdb_id: int) -> str:
    """The detail call for one entity, with its appended sub-objects — pure."""
    return f"/{feed}/{tmdb_id}?append_to_response={APPEND[feed]}"


def changes_path(feed: str, day: date, page: int) -> str:
    """One page of the change feed for exactly one calendar day — pure.

    TMDB's window is **inclusive at both ends**, so ``start_date == end_date`` is one full day."""
    iso = day.isoformat()
    return f"/{feed}/changes?start_date={iso}&end_date={iso}&page={page}"


def parse_changes_page(body: dict) -> tuple[list[int], int]:
    """A change-feed page as ``(ids in page order, total_pages)`` — pure.

    Duplicate ids collapse and entries whose ``adult`` is ``True`` are dropped (the catalog never
    holds them); a ``null`` ``adult`` — which TMDB does send — passes. Unknown fields such as the
    undocumented ``softcore`` are ignored, never enumerated. A body without ``results`` is not a
    change page (TMDB's error bodies look like that), so it raises rather than reading as "quiet".
    """
    if "results" not in body:
        raise ValueError(f"not a change-feed page (no 'results'): {str(body)[:200]!r}")
    ids: list[int] = []
    seen: set[int] = set()
    for entry in body["results"]:
        if entry.get("adult") is True or entry["id"] in seen:
            continue
        seen.add(entry["id"])
        ids.append(entry["id"])
    return ids, int(body.get("total_pages") or 0)


def _names(items: list[dict] | None, key: str = "name") -> list[str]:
    return [item[key] for item in items or [] if item.get(key) is not None]


def _merge_external_ids(flat: dict[str, Any], external_ids: dict | None) -> None:
    """The non-null external ids (``imdb_id``, ``wikidata_id``, ``tvdb_id``, …) at top level."""
    for key, value in (external_ids or {}).items():
        if value is not None:
            flat[key] = value


def _person_fields(entry: dict) -> dict[str, Any]:
    """The person-side fields every credit row carries, ``None`` values omitted."""
    row = {
        "person_id": entry["id"],
        "name": entry.get("name"),
        "gender": entry.get("gender"),
        "known_for_department": entry.get("known_for_department"),
        "person_popularity": entry.get("popularity"),
    }
    return {key: value for key, value in row.items() if value is not None}


def _drop_none(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if value is not None}


def _spread(raw: dict, feed: str) -> dict[str, Any]:
    """The response as delivered, minus the appended sub-objects this projector re-shapes."""
    return {key: value for key, value in raw.items() if key not in _APPENDED[feed]}


def project_movie(raw: dict) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """A movie detail response as ``(flat row, credit rows)`` — pure."""
    flat = _spread(raw, "movie")
    flat["genres"] = _names(raw.get("genres"))
    flat["production_companies"] = _names(raw.get("production_companies"))
    flat["production_countries"] = _names(raw.get("production_countries"), "iso_3166_1")
    flat["spoken_languages"] = _names(raw.get("spoken_languages"), "iso_639_1")
    flat.pop("belongs_to_collection", None)
    if (collection := raw.get("belongs_to_collection")) and collection.get("name") is not None:
        flat["collection"] = collection["name"]
    flat["keywords"] = _names((raw.get("keywords") or {}).get("keywords"))
    certifications: dict[str, str] = {}
    for country in (raw.get("release_dates") or {}).get("results") or []:
        for release in country.get("release_dates") or []:
            if release.get("certification"):
                certifications[country["iso_3166_1"]] = release["certification"]
                break
    flat["certifications"] = certifications
    _merge_external_ids(flat, raw.get("external_ids"))

    credits_raw = raw.get("credits") or {}
    credits = [
        _drop_none({
            "credit_id": cast["credit_id"], "kind": "cast", **_person_fields(cast),
            "character": cast.get("character"), "billing_order": cast.get("order"),
        })
        for cast in credits_raw.get("cast") or []
    ] + [
        _drop_none({
            "credit_id": crew["credit_id"], "kind": "crew", **_person_fields(crew),
            "department": crew.get("department"), "job": crew.get("job"),
        })
        for crew in credits_raw.get("crew") or []
    ]
    return flat, credits


def project_tv(raw: dict) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """A TV-series detail response as ``(flat row, credit rows)`` — pure.

    Seasons, episodes and the aggregate credits leave the flat row (seasons and episodes are out
    of scope; the credits fan out). TMDB names the keyword list ``results`` here, unlike movies'
    ``keywords``."""
    flat = _spread(raw, "tv")
    for key in ("seasons", "last_episode_to_air", "next_episode_to_air"):
        flat.pop(key, None)
    flat["genres"] = _names(raw.get("genres"))
    flat["production_companies"] = _names(raw.get("production_companies"))
    flat["production_countries"] = _names(raw.get("production_countries"), "iso_3166_1")
    flat["spoken_languages"] = _names(raw.get("spoken_languages"), "iso_639_1")
    flat["networks"] = _names(raw.get("networks"))
    flat["created_by"] = _names(raw.get("created_by"))
    flat["keywords"] = _names((raw.get("keywords") or {}).get("results"))
    flat["content_ratings"] = {
        rating["iso_3166_1"]: rating["rating"]
        for rating in (raw.get("content_ratings") or {}).get("results") or []
        if rating.get("rating")
    }
    _merge_external_ids(flat, raw.get("external_ids"))

    aggregate = raw.get("aggregate_credits") or {}
    credits = [
        _drop_none({
            "credit_id": role["credit_id"], "kind": "cast", **_person_fields(cast),
            "character": role.get("character"), "billing_order": cast.get("order"),
            "episode_count": role.get("episode_count"),
        })
        for cast in aggregate.get("cast") or []
        for role in cast.get("roles") or []
    ] + [
        _drop_none({
            "credit_id": job["credit_id"], "kind": "crew", **_person_fields(crew),
            "department": crew.get("department"), "job": job.get("job"),
            "episode_count": job.get("episode_count"),
        })
        for crew in aggregate.get("crew") or []
        for job in crew.get("jobs") or []
    ]
    return flat, credits


def project_person(raw: dict) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """A person detail response as ``(flat row, [])`` — pure. People carry no credits here:
    credits come from the title side."""
    flat = _spread(raw, "person")
    _merge_external_ids(flat, raw.get("external_ids"))
    return flat, []


PROJECTORS = {"movie": project_movie, "tv": project_tv, "person": project_person}
"""Feed name → projector, the catalog stage's dispatch table."""


def content_hash(flat: dict[str, Any], credits: list[dict[str, Any]]) -> str:
    """The first 16 hex of the SHA-256 of the canonical JSON of ``[flat, credits]`` — pure.

    Key order cannot matter (``sort_keys``). Computed before the version fields are added, so the
    same content always hashes the same. 16 hex ≈ 1.8 × 10¹⁹ values — ample for a per-entity
    equality gate."""
    canonical = json.dumps([flat, credits], sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def day_windows(start: date, today: date) -> list[date]:
    """Every calendar day from ``start`` to ``today`` inclusive — empty when ``start > today``."""
    return [start + timedelta(days=offset) for offset in range((today - start).days + 1)]


def prune_seen(seen: dict[str, list[int]], keep_from: date, *,
               hard_cap: int = SEEN_HARD_CAP) -> dict[str, list[int]]:
    """The open-day seen-set trimmed to the live day and the changelog budget — pure.

    Two independent bounds: buckets dated before ``keep_from`` go whole (a closed day is never
    listed as open again), and if the survivors still exceed ``hard_cap``, whole oldest buckets go
    first, then the oldest ids of the last bucket, with a WARNING naming what went. The cap is
    unconditional because each State is ONE changelog record with a hard byte ceiling; the cost of
    it biting is a re-fetch of some ids (the catalog's hash gate turns that into nothing), never a
    wedged stage."""
    cutoff = keep_from.isoformat()
    pruned = {day: ids for day, ids in seen.items() if day >= cutoff}
    total = sum(len(ids) for ids in pruned.values())
    dropped: list[str] = []
    while total > hard_cap and len(pruned) > 1:
        oldest = min(pruned)
        total -= len(pruned.pop(oldest))
        dropped.append(oldest)
    trimmed = 0
    if total > hard_cap:  # exactly one bucket left, and it alone busts the cap
        (day, ids), = pruned.items()
        trimmed = total - hard_cap
        pruned[day] = ids[trimmed:]
        total = hard_cap
    if dropped or trimmed:
        went = [f"dropped date bucket(s) {', '.join(dropped)}"] if dropped else []
        if trimmed:
            went.append(f"trimmed the {trimmed} oldest id(s) from the remaining bucket")
        log.warning(
            "seen-set over the %d-id cap: %s; %d ids kept. Affected ids may be re-fetched "
            "(harmless: the catalog's hash gate drops an unchanged copy)",
            hard_cap, "; ".join(went), total)
    return pruned

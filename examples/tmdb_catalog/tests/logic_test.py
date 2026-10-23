"""Tier 1 — the pure logic, no framework runtime, no mocks.

Everything the stages decide is a plain function in ``projection.py``: build a path, parse a change
page, project a detail response, hash it, window the days, prune the seen-set. The fixtures are
real TMDB captures (``fixtures/PROVENANCE.md``), and every number that depends on them is computed
from the fixture here — a re-capture changes cast sizes and popularity, never these assertions.
"""
import json
import logging
from datetime import date
from pathlib import Path

import pytest

from examples.tmdb_catalog.projection import (
    PROJECTORS,
    content_hash,
    day_windows,
    details_path,
    changes_path,
    parse_changes_page,
    project_movie,
    project_person,
    project_tv,
    prune_seen,
)

FIXTURES = Path(__file__).parent / "fixtures"
MOVIE = json.loads((FIXTURES / "movie_550.json").read_text())
TV = json.loads((FIXTURES / "tv_87108.json").read_text())
PERSON = json.loads((FIXTURES / "person_287.json").read_text())
CHANGES = json.loads((FIXTURES / "changes_movie_p1.json").read_text())


# --- paths ---

def test_paths() -> None:
    assert details_path("movie", 550) == (
        "/movie/550?append_to_response=credits,keywords,release_dates,external_ids")
    assert details_path("tv", 87108).endswith("aggregate_credits,keywords,content_ratings,external_ids")
    assert details_path("person", 287) == "/person/287?append_to_response=external_ids"
    assert changes_path("tv", date(2026, 10, 1), 3) == (
        "/tv/changes?start_date=2026-10-01&end_date=2026-10-01&page=3")


# --- parse_changes_page ---

def test_the_real_change_page_parses_in_page_order() -> None:
    ids, total_pages = parse_changes_page(CHANGES)
    expected = [entry["id"] for entry in CHANGES["results"] if entry.get("adult") is not True]
    assert ids == expected                     # page order, not sorted — the real page is unsorted
    assert ids != sorted(ids)
    assert len(set(ids)) == len(ids)
    assert total_pages == CHANGES["total_pages"]


def test_a_null_adult_passes_on_the_real_page() -> None:
    null_adult = [entry["id"] for entry in CHANGES["results"] if entry["adult"] is None]
    assert null_adult, "the fixture is meant to carry an adult: null entry"
    ids, _ = parse_changes_page(CHANGES)
    assert null_adult[0] in ids


def test_hand_built_pages() -> None:
    page = {"results": [{"id": 1, "adult": True}, {"id": 2, "adult": None}, {"id": 3, "adult": False},
                        {"id": 2, "adult": False}, {"id": 4, "softcore": True}],
            "page": 1, "total_pages": 7}
    assert parse_changes_page(page) == ([2, 3, 4], 7)       # adult dropped, null passes, dup collapses
    assert parse_changes_page({"results": [], "total_pages": 0}) == ([], 0)


def test_a_body_without_results_is_not_a_page() -> None:
    with pytest.raises(ValueError):
        parse_changes_page({"status_code": 22, "status_message": "Invalid page"})


# --- project_movie ---

def test_project_movie_flattens_and_spreads() -> None:
    flat, _ = project_movie(MOVIE)
    assert flat["title"] == "Fight Club"
    assert "Drama" in flat["genres"] and all(isinstance(name, str) for name in flat["genres"])
    assert flat["imdb_id"] == "tt0137523"
    assert flat["certifications"]["US"] == "R"
    assert flat["keywords"] and all(isinstance(name, str) for name in flat["keywords"])
    assert flat["production_countries"] and all(len(code) == 2 for code in flat["production_countries"])
    assert flat["spoken_languages"]
    for appended in ("credits", "release_dates", "external_ids", "belongs_to_collection"):
        assert appended not in flat
    assert "softcore" in flat                                # unknown fields spread through
    assert flat["popularity"] == MOVIE["popularity"]         # ...and so do the ones we don't touch


def test_project_movie_credits_fan_out() -> None:
    _, credits = project_movie(MOVIE)
    cast, crew = MOVIE["credits"]["cast"], MOVIE["credits"]["crew"]
    assert len(credits) == len(cast) + len(crew)
    first = credits[0]
    assert first["kind"] == "cast" and first["billing_order"] == 0
    assert first["character"] == cast[0]["character"]
    assert first["person_id"] == cast[0]["id"] and first["person_popularity"] == cast[0]["popularity"]
    crew_rows = [row for row in credits if row["kind"] == "crew"]
    assert len(crew_rows) == len(crew)
    assert all("job" in row and "department" in row and "character" not in row for row in crew_rows)
    assert all(None not in row.values() for row in credits)


def test_project_movie_collection_only_when_present() -> None:
    flat, _ = project_movie({**MOVIE, "belongs_to_collection": None})
    assert "collection" not in flat
    flat, _ = project_movie({**MOVIE, "belongs_to_collection": {"id": 1, "name": "A Collection"}})
    assert flat["collection"] == "A Collection"


# --- project_tv ---

def test_project_tv() -> None:
    flat, credits = project_tv(TV)
    assert flat["name"] == "Chernobyl"
    assert "HBO" in flat["networks"]
    assert flat["keywords"] == [item["name"] for item in TV["keywords"]["results"]]   # `results`, not `keywords`
    assert isinstance(flat["tvdb_id"], int)
    for dropped in ("seasons", "last_episode_to_air", "next_episode_to_air", "aggregate_credits",
                    "external_ids"):
        assert dropped not in flat
    assert flat["content_ratings"] == {r["iso_3166_1"]: r["rating"]
                                       for r in TV["content_ratings"]["results"] if r["rating"]}
    assert "languages" in flat and "origin_country" in flat
    aggregate = TV["aggregate_credits"]
    assert len(credits) == (sum(len(c["roles"]) for c in aggregate["cast"])
                            + sum(len(c["jobs"]) for c in aggregate["crew"]))
    assert all("episode_count" in row for row in credits)
    assert {row["kind"] for row in credits} == {"cast", "crew"}


# --- project_person ---

def test_project_person() -> None:
    flat, credits = project_person(PERSON)
    assert flat["name"] == "Brad Pitt" and flat["gender"] == 2
    assert credits == []
    assert "external_ids" not in flat
    assert PROJECTORS["person"] is project_person


# --- content_hash ---

def test_content_hash_ignores_key_order_and_sees_content() -> None:
    flat, credits = project_movie(MOVIE)
    digest = content_hash(flat, credits)
    assert len(digest) == 16 and int(digest, 16) >= 0
    assert content_hash(dict(reversed(list(flat.items()))), credits) == digest

    assert content_hash({**flat, "popularity": flat["popularity"] + 0.5}, credits) != digest
    assert content_hash(flat, credits[:-1] + [{**credits[-1], "job": "Something Else"}]) != digest


# --- day_windows ---

def test_day_windows_are_inclusive() -> None:
    assert day_windows(date(2026, 10, 21), date(2026, 10, 23)) == [
        date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)]
    assert day_windows(date(2026, 10, 23), date(2026, 10, 23)) == [date(2026, 10, 23)]
    assert day_windows(date(2026, 10, 24), date(2026, 10, 23)) == []


# --- prune_seen ---

def test_prune_seen_drops_closed_days() -> None:
    seen = {"2026-10-21": [1], "2026-10-22": [2], "2026-10-23": [3]}
    assert prune_seen(seen, date(2026, 10, 23)) == {"2026-10-23": [3]}
    assert prune_seen({}, date(2026, 10, 23)) == {}


def test_prune_seen_caps_whole_buckets_first_then_the_oldest_ids(caplog: pytest.LogCaptureFixture) -> None:
    seen = {"2026-10-22": [1, 2, 3], "2026-10-23": [4, 5, 6, 7]}
    with caplog.at_level(logging.WARNING):
        assert prune_seen(seen, date(2026, 10, 22), hard_cap=5) == {"2026-10-23": [4, 5, 6, 7]}
    assert "dropped date bucket(s) 2026-10-22" in caplog.text

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        assert prune_seen({"2026-10-23": [4, 5, 6, 7]}, date(2026, 10, 23), hard_cap=2) == {
            "2026-10-23": [6, 7]}                                  # the newest ids win
    assert "trimmed the 2 oldest" in caplog.text

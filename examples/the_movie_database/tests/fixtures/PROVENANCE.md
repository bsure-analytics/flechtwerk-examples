# Test fixtures — provenance

**Real** captures from the live TMDB API v3, taken **2026-10-23** with a free *API Read Access
Token* (`Authorization: Bearer [TOKEN]`; the token is redacted as `[TOKEN]` below and appears in
no file). Get your own at <https://www.themoviedb.org/settings/api> to re-capture.

| file | endpoint | bytes |
|---|---|---|
| `movie_550.json` | `GET https://api.themoviedb.org/3/movie/550?append_to_response=credits,keywords,release_dates,external_ids` | 133 457 |
| `tv_87108.json` | `GET https://api.themoviedb.org/3/tv/87108?append_to_response=aggregate_credits,keywords,content_ratings,external_ids` | 207 901 |
| `person_287.json` | `GET https://api.themoviedb.org/3/person/287?append_to_response=external_ids` | 3 202 |
| `changes_movie_p1.json` | `GET https://api.themoviedb.org/3/movie/changes?start_date=2026-10-01&end_date=2026-10-01&page=1` | 8 074 |

Each command is `curl -s -H "Authorization: Bearer [TOKEN]" "<endpoint>" | jq .` (pretty-printed
for readable diffs; the byte sizes are of the pretty-printed files).

## Bodies are unmodified

Every body is exactly what TMDB delivered, only re-indented by `jq`. Nothing was trimmed.

`changes_movie_p1.json` is page 1 of a **closed** day (2026-10-01), so a re-capture returns the
same entries. It is a real 100-id page and carries what the projection has to survive: every entry
has the undocumented `softcore` key, one entry has `adult: null`, none has `adult: true`, and the
ids are unique but **not sorted** — page order is TMDB's, not ascending.

The detail fixtures are *live entities*: a re-capture will differ (popularity moves daily, casts
grow). The tests therefore derive every fixture-dependent number (cast and crew counts, the first
cast member's character) from the fixture itself and never hard-code it.

## Attribution

This product uses TMDB and the TMDB APIs but is not endorsed, certified, or otherwise approved by
TMDB. Non-commercial use only; see <https://www.themoviedb.org/api-terms-of-use>.

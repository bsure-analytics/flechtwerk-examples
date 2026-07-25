# Test fixtures — provenance

Trimmed **real** captures. Taken **2026-07-25** from the live NASA FIRMS area API and public
Nominatim, for a genuinely burning region: southern Portugal / south-west Spain, requested as the
bounding box `-9.5,36.0,-6.0,42.0` (west, south, east, north) with `DAY_RANGE = 2`.

| file | source | endpoint |
|---|---|---|
| `firms_n20.csv` | FIRMS area API, NOAA-20 | `GET https://firms.modaps.eosdis.nasa.gov/api/area/csv/[MAP_KEY]/VIIRS_NOAA20_NRT/-9.5,36.0,-6.0,42.0/2` |
| `firms_n20_later.csv` | the same request | as above — the *later poll's* view of the same window |
| `firms_n21.csv` | FIRMS area API, NOAA-21 | `GET https://firms.modaps.eosdis.nasa.gov/api/area/csv/[MAP_KEY]/VIIRS_NOAA21_NRT/-9.5,36.0,-6.0,42.0/2` |
| `firms_error.txt` | FIRMS area API, bad key | `GET …/api/area/csv/invalidkey123/VIIRS_NOAA20_NRT/…` → **HTTP 400**, this body |
| `nominatim_region.json` | Nominatim `/search` | `GET https://nominatim.openstreetmap.org/search?q=Alentejo,+Portugal&format=jsonv2&limit=1` |

The MAP_KEY is redacted as `[MAP_KEY]`. Get your own (free, instant) at
<https://firms.modaps.eosdis.nasa.gov/api/map_key/> to re-capture.

## Trimming

**Rows are unmodified** — every field is exactly as NASA delivered it. Only *which* rows are
present was chosen, and only in one way: the capture spans two acquisition dates
(`2026-07-24` and `2026-07-25`), so

- `firms_n20.csv` holds the **21 rows dated 2026-07-24** — one poll's worth, and
- `firms_n20_later.csv` holds **those same 21 rows plus the 11 dated 2026-07-25**.

The second file is therefore a genuine **superset** of the first, which is exactly the shape a
later poll of a rolling day window returns, and it is what the dedupe tests are built on: poll 1
emits 21 detections, poll 2 emits only the 11 new ones. (Constructing the pair this way rather
than by waiting an hour between two live captures keeps the fixtures reproducible while still
using nothing but real rows.) Rows in all three CSVs are sorted by
`(acq_date, acq_time, latitude, longitude)` for readable diffs.

`firms_n21.csv` is the NOAA-21 capture in full (24 rows, both dates) — the *second source* every
poll fetches. It sees the same big fire as NOAA-20 on a different pass, which is what makes
cross-satellite confirmation testable.

## What these rows happen to contain (and why that's useful)

The captured window has real structure worth knowing about, because several tests lean on it:

- a **tight 12-pixel cluster** around `37.54–37.56 N, -7.06 W` — one large fire, which the
  tracker must collapse into a *single* fire object rather than twelve;
- several **isolated single pixels** (e.g. `38.92, -9.01`) and two small 2–3 pixel groups;
- a **new fire that appears only in the later poll** (`38.63 N, -7.25 W`, dated 2026-07-25) — an
  ignition discovered between polls;
- `bright_ti4` values of exactly `367` — the VIIRS I-4 saturation ceiling, delivered as a bare
  integer, which is why the parser wraps every number in `float()` (the `FLOAT` codec rejects
  `int`);
- `acq_time` values like `230` and `137` — the unpadded HHMM integers (02:30, 01:37);
- `confidence` letters `l`, `n`, and `h` all present.

**No field is ever empty** in these captures, and none was in the ~3 600 rows surveyed while
writing the example: FIRMS populates all 14 columns. `frp`, `bright_ti4`, and `bright_ti5` are
nonetheless *optional* attributes, and the logic tier tests the empty-field path with a
synthesized row — because the real hazard is the opposite one: a genuine `frp` of `0.0` **does**
occur, so absence must be decided by the empty string and never by falsiness.

`nominatim_region.json` is one `jsonv2` hit kept whole (it is already small), including fields
the geocoder ignores so the tests prove tolerance of them. Note this particular hit is backed by
an OSM **node**, not a boundary relation, so its `boundingbox` is a synthetic ±1° box around the
point — a real and common case worth having in the fixtures. Order is
`[south, north, west, east]`, all strings.

These are shape/round-trip fixtures, not live data. Fires move; re-capture with the curls above
(pick a region that is burning today from <https://firms.modaps.eosdis.nasa.gov/map/>) if you
need fresh ones.

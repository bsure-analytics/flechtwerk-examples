# GDELT test fixtures

One real GDELT 2.0 15-minute trio per feed, captured **2026-07-21** straight from
`data.gdeltproject.org`. Committed so every test tier runs offline and pinned —
the same discipline as the framework's own fixtures.

| File | Feed | Table | Kept |
|---|---|---|---|
| `20260721083000.export.CSV.zip` | english | Events (61 cols) | whole (898 rows) |
| `20260721083000.mentions.CSV.zip` | english | Mentions (16 cols) | whole (2415 rows) |
| `20260721083000.gkg.csv.zip` | english | GKG (27 cols) | **truncated to first 300 rows** |
| `20260721081500.translation.export.CSV.zip` | translation | Events | whole (825 rows) |
| `20260721081500.translation.mentions.CSV.zip` | translation | Mentions | whole (1475 rows) |
| `20260721081500.translation.gkg.csv.zip` | translation | GKG | **truncated to first 300 rows** |
| `lastupdate.txt` | english | pointer | regenerated |
| `lastupdate-translation.txt` | translation | pointer | regenerated |

The GKG files are the heavy ones (whole they are ~4 MB / ~11 MB zipped), so each is
truncated to its first 300 rows and re-zipped — enough distinct outlets to exercise
clustering while keeping the fixture directory to ~3 MB total. Export and mentions
are small, so they stay whole.

**Pointer files are regenerated, not the originals.** Because the GKG zips were
truncated, `lastupdate.txt` / `lastupdate-translation.txt` carry the `size md5 url`
of the *local* files (all six, GKG included), so the ingest stage's size+MD5
verification is self-consistent against these fixtures. The URLs are the original
GDELT URLs; a test's HTTP stub maps each URL's basename to the local zip.

## Quirk regression rows (encoded as tests)

- **`SQLDATE` ≠ `DATEADDED`** — in `20260721083000.export.CSV.zip`, GlobalEventID
  **1314546129** and **1314546130** carry `SQLDATE=20250721` (a year stale) while
  `DATEADDED=20260721083000` is current. This is exactly the machine-coding noise
  the pipeline guards against: **event time is the file timestamp / `DATEADDED`,
  never `SQLDATE`.**
- `.gkg.csv.zip` is **tab-delimited** despite the `.csv` name.
- UTF-8 with occasional garbage bytes — decoded with `errors="replace"`.

Re-capture with `scratchpad/capture_fixtures.py` (the feed only keeps the last few
months of 15-minute slices; the full history lives behind `masterfilelist.txt`).

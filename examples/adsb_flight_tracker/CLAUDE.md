# ADS-B Flight Tracker (`adsb`)

The ADS-B example is a three-stage
data pipeline (ingest extractor → enrich transformer → conflict transformer) plus a
companion **boundary-loader extractor** (`boundaries.py`, `CountryLoader`) — four host
processes. Reverse geocoding is **staged and traffic-driven** over a stack of ClickHouse
`POLYGON` dictionaries (no Nominatim/PostGIS on the reverse path): the loader downloads a
global ADM0 **world map** at startup (`__aenter__`, Natural Earth admin-0 — geoBoundaries'
own global ADM0/CGAZ is ~400 MB, too heavy), and enrich detects each aircraft's country
against it, writing that ISO-3 to the compacted `adsb-countries` topic; the loader consumes
those as its poll targets and downloads **all** admin levels that country publishes
(geoBoundaries ADM1…ADM5) into one `adsb_region_adm{n}_dict` each (all from the single
`adsb_region_boundaries` table filtered by level), just-in-time. enrich `dictGet`s every level
for a point and concatenates the hits into a hierarchical label (`Le Bourget; Marne; Grand
Est`) — one dict per level because a polygon dict returns only the finest containing
polygon. **Nothing is seeded** — `setup.py` only creates topics + schema; a user requests a
poll region with `uv run poe request-region "<name>"` (→ `request.py` → `adsb-regions`),
and forward geocoding of that name→centre uses public Nominatim (`ingest`).

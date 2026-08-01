# Wildfire Watch (`wildfire`)

**Wildfire Watch**
(`wildfire`) is the spatiotemporal-sessionization case and the **only example needing an API key**:
`ingest` polls NASA FIRMS' area API for two VIIRS satellites per watch region (a compacted
`wildfire-regions` config record carrying a place name **and its cached bounding box** — `request.py`
geocodes at request time via Nominatim's `boundingbox`, the one field no other example reads, and
writes all four edges into the record so it is self-describing and the tool can warn about
overlapping regions by intersecting rectangles; `enrich_config` is the **fallback** that geocodes
**once** for a name-only record, e.g. one hand-produced in Kafbat), emits the new
375 m hotspot pixels **then one `sweep` marker** to `wildfire-detections`, and keeps a **bounded,
event-time-pruned seen-set** as its cursor (FIRMS has no row id and nothing monotonic: identity is
a 12-hex hash of the raw CSV strings, bucketed by `acq_date`, whole buckets dropped as the day
window rolls, hard-capped at 20 k ids for the ~1 MB changelog ceiling — enforced even *within* a
single day bucket by trimming its oldest ids, because one monster bucket once produced an
oversized state record that crashlooped the stage). The `tracker` transformer
clusters those points into **persistent fire objects** in keyed state (`FIRES = {fire_id: entry}`,
2 km link distance, `F_*` raw keys, pure core in `tracking.py`, bucket hard-capped at
`MAX_FIRES = 2000` by stalest-first forced extinction — the bucket is one ~1 MB changelog record
too, and the worst savanna cell clusters 6 k+ fires/day) with a full lifecycle: ignition,
growth, merge when one detection bridges two fires, and **extinction after 12 h of event time** —
driven *entirely* by the sweep marker, which is emitted even on a quiet poll because the framework
has no timers (so status heartbeats are sweep-paced, not detection-paced, and no input means no
extinction). The region's bucket is tombstoned when its last fire dies. Outputs: `wildfire-status`
(continuous per-fire snapshots → `wildfire_status` history + `wildfire_active` ReplacingMergeTree) and
`wildfire-events` (sparse ignition/merged/extinguished, no TTL). `wildfire-detections` feeds **two**
ClickHouse tables by kind — `wildfire_detections` and `wildfire_sweeps`, the latter existing so a
freshness panel can tell "nothing burning" from "poller stopped". A false extinction self-heals as
a new ignition — documented, not hidden. `request.py` prints what Nominatim *matched*
(`display_name` + `addresstype` — a typo like "Bordeux, France" silently matches a street in
Picardy) and **refuses geocoded or explicit boxes wider than 60° a side** (`REFUSE_BBOX_DEG`;
"France" resolves to the whole Republic, Kerguelen to Polynesia, and one region that size
crashloops both stages' single-record state). "Everything" is the **world watch**:
`request-wildfire world` (retire with `retire world`) tiles the planet in `tiles.py` — a 10°
Natural-Earth land grid (267 cells, nothing south of 60°S) quadtree-split above ~6 k
detections/24 h down to 1.25°, counted from FIRMS' *keyless public* daily global CSVs so tiling
spends no quota — into a few hundred ordinary `world-*` config records; the stages have no
world-specific code, re-running re-tiles and tombstones stale tiles, and the seasonal burn-belt
drift is handled by re-running, not by the stages.

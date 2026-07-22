"""Tier 3 — integration. The staged, hierarchical reverse geocoder on real ClickHouse.

Proves the reverse geocoder end to end against an ephemeral ClickHouse (testcontainer):
create the world map + per-level region dictionaries (the shipped DDL shape), load a
country into the world map and two nested admin areas (an ADM1 containing an ADM2), then
reverse-geocode with the real :meth:`WikidataClickHouseEnricher.geocode` — a point inside
both resolves to country + a hierarchical ``"ADM2; ADM1"`` label, a point in only the ADM1
gets just that, a point where the two levels share a name collapses to one (``arrayDistinct``,
no ``"Twinvale; Twinvale"``), and a point outside everything is a graceful miss (empty
Record). This is the piece the runner tier can only fake: the actual per-level ``dictGet``
point-in-polygon lookups + the ClickHouse-side de-dup and concatenation on a live server.

Only the boundary DDL is applied here (not the whole ``clickhouse.sql``, whose Kafka
engine tables need a broker) — this tier isolates the geocoding maps.
"""
import json

import httpx
import pytest

from flechtwerk.attribute import Record

from examples.adsb_flight_tracker.attributes import ISO3, NEAREST_PLACE, OVER_COUNTRY
from examples.adsb_flight_tracker.boundaries import ADMIN_LEVELS, WORLD_DICT, region_dict
from examples.adsb_flight_tracker.enrich import WikidataClickHouseEnricher

pytestmark = pytest.mark.integration

_GEOM = "Array(Array(Array(Tuple(Float64, Float64))))"


def _ddl(user: str, password: str) -> list[str]:
    """The shipped boundary DDL (``clickhouse.sql``), reshaped for the testcontainer.

    One deviation from production: each POLYGON dictionary's ``CLICKHOUSE`` source names
    ``USER``/``PASSWORD`` explicitly. A dictionary reload opens a fresh connection back to
    the server to read its source table, and with no user named it falls back to ``default``.
    The compose stack keeps a passwordless ``default`` (so ``clickhouse.sql`` omits the
    clause), but the testcontainer's ``CLICKHOUSE_USER`` removes ``default`` outright, leaving
    only this dedicated user — so the reload must authenticate as it, or it fails REQUIRED_PASSWORD.
    """
    src = f"USER '{user}' PASSWORD '{password}'"
    return [
        f"CREATE TABLE flechtwerk.adsb_world_boundaries (geometry {_GEOM}, country String, iso3 String, "
        "loaded_at DateTime) ENGINE = MergeTree ORDER BY iso3",
        f"CREATE DICTIONARY flechtwerk.adsb_world_boundaries_dict (geometry {_GEOM}, country String, iso3 String) "
        f"PRIMARY KEY geometry SOURCE(CLICKHOUSE(TABLE 'adsb_world_boundaries' DB 'flechtwerk' {src})) "
        "LIFETIME(0) LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))",
        f"CREATE TABLE flechtwerk.adsb_region_boundaries (geometry {_GEOM}, name String, iso3 String, "
        "admin_level LowCardinality(String), loaded_at DateTime) ENGINE = MergeTree ORDER BY (iso3, name)",
        *(f"CREATE DICTIONARY {region_dict(level)} (geometry {_GEOM}, name String) PRIMARY KEY geometry "
          f"SOURCE(CLICKHOUSE(QUERY 'SELECT geometry, name FROM flechtwerk.adsb_region_boundaries WHERE admin_level = ''{level}''' {src})) "
          "LIFETIME(0) LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))"
          for level in ADMIN_LEVELS),
    ]

_BIG = [[[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]]]   # country + ADM1
_SMALL = [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]]     # ADM2 inside it
_TWIN1 = [[[[7.0, 7.0], [9.0, 7.0], [9.0, 9.0], [7.0, 9.0], [7.0, 7.0]]]]     # ADM1 "Twinvale"
_TWIN2 = [[[[7.0, 7.0], [8.0, 7.0], [8.0, 8.0], [7.0, 8.0], [7.0, 7.0]]]]     # ADM2 "Twinvale" inside it


async def test_staged_hierarchical_reverse_geocoder_on_real_clickhouse(clickhouse: dict) -> None:
    auth = httpx.BasicAuth(clickhouse["user"], clickhouse["password"])
    async with httpx.AsyncClient(base_url=clickhouse["base_url"], auth=auth) as ch:
        for statement in _ddl(clickhouse["user"], clickhouse["password"]):
            (await ch.post("/", content=statement)).raise_for_status()
        rows = [
            ("adsb_world_boundaries", {"geometry": _BIG, "country": "Testland", "iso3": "TST", "loaded_at": 1_700_000_000}),
            ("adsb_region_boundaries", {"geometry": _BIG, "name": "Bigland", "iso3": "TST",
                                   "admin_level": "ADM1", "loaded_at": 1_700_000_000}),
            ("adsb_region_boundaries", {"geometry": _SMALL, "name": "Smalltown", "iso3": "TST",
                                   "admin_level": "ADM2", "loaded_at": 1_700_000_000}),
            ("adsb_region_boundaries", {"geometry": _TWIN1, "name": "Twinvale", "iso3": "TST",
                                   "admin_level": "ADM1", "loaded_at": 1_700_000_000}),
            ("adsb_region_boundaries", {"geometry": _TWIN2, "name": "Twinvale", "iso3": "TST",
                                   "admin_level": "ADM2", "loaded_at": 1_700_000_000}),
        ]
        for table, row in rows:
            (await ch.post("/", content=f"INSERT INTO flechtwerk.{table} FORMAT JSONEachRow\n"
                           + json.dumps(row))).raise_for_status()
        for dict_name in [WORLD_DICT, *(region_dict(level) for level in ADMIN_LEVELS)]:
            (await ch.post("/", content=f"SYSTEM RELOAD DICTIONARY {dict_name}")).raise_for_status()

    enricher = WikidataClickHouseEnricher(
        clickhouse_url=clickhouse["base_url"] + "/",
        clickhouse_auth=(clickhouse["user"], clickhouse["password"]),
    )
    try:
        (deep,) = await enricher.geocode([(0.5, 0.5)])    # in country + ADM1 + ADM2
        assert deep[OVER_COUNTRY] == "Testland" and deep[ISO3] == "TST"
        assert deep[NEAREST_PLACE] == "Smalltown; Bigland"  # finest → coarsest, "; "-joined
        (shallow,) = await enricher.geocode([(5.0, 5.0)])  # in country + ADM1 only
        assert shallow[NEAREST_PLACE] == "Bigland"
        (twin,) = await enricher.geocode([(7.2, 7.8)])     # ADM1 + ADM2 both named "Twinvale"
        assert twin[NEAREST_PLACE] == "Twinvale"           # arrayDistinct collapses the repeat
        assert await enricher.geocode([(50.0, 50.0)]) == [Record()]  # outside everything → miss
    finally:
        await enricher.aclose()

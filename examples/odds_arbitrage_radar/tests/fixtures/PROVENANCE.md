# Test fixtures — provenance

Trimmed **real** captures (the GTFS provenance pattern — the venues' JSON is too irregular to
synthesize convincingly, unlike SMARD's `[ms, value]` shape). Captured **2026-07-23** from
the two keyless public APIs for the same real-world matchup — a Colorado Rockies vs. Milwaukee
Brewers MLB game — so the pairing is coherent end to end:

| file | source | endpoint |
|---|---|---|
| `poly_market.json` | Polymarket Gamma | `GET https://gamma-api.polymarket.com/markets?slug=mlb-col-mil-2026-07-24` |
| `poly_book_colorado.json` | Polymarket CLOB | `GET https://clob.polymarket.com/book?token_id=<outcome 0>` |
| `poly_book_milwaukee.json` | Polymarket CLOB | `GET https://clob.polymarket.com/book?token_id=<outcome 1>` |
| `kalshi_market.json` | Kalshi | `GET https://api.elections.kalshi.com/trade-api/v2/markets/KXMLBGAME-26JUL261410COLMIL-MIL` |

**Trimming.** Each market response is cut to the fields the normalizers read **plus a few
extras** (e.g. `orderPriceMinTickSize`, `rules_primary`) so the tests prove tolerance of
fields we don't promote. The two CLOB books are cut to their best five bid and ask levels,
and those levels are **deliberately left unsorted** (stored worst-to-best) so the tests
exercise `best_of_book`'s max/min reduction rather than trusting array order.

`outcomes` and `clobTokenIds` are kept exactly as delivered — JSON arrays **encoded as
strings** (double-encoded), the quirk the normalizer must `json.loads`.

These are shape/round-trip fixtures, not live data. The markets are long resolved; re-capture
fresh ones (any game listed on both venues) with the discovery curls in the example README
if you need to regenerate.

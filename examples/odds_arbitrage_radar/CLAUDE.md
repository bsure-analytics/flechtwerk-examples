# Odds Arbitrage Radar (`odds`)

The
**Odds Arbitrage Radar** (`odds`) is the N-source fan-in case: two *independent* extractor
processes (`polymarket`, `kalshi`) share the one compacted config topic `odds-pairs` and both
produce to the one partitioned topic `odds-quotes` keyed by pair, so the `radar` transformer
sees both venues' quotes co-partitioned onto one task; it holds the latest quote per venue in
its join state, recomputes the cross-venue arbitrage on every update (emitting a continuous
`odds-margins` stream + a sparse `odds-signals` stream), gates signals on event-time freshness
(the two legs' `fetched_at`), and tombstones the pair's state when a venue reports the market
closed. Both extractors are stateless snapshot pollers (no cursor). Keyless public data
(Polymarket Gamma/CLOB + Kalshi), read-only — it never places an order.

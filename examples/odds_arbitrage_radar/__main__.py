"""Run an Odds Arbitrage Radar stage against the shared stack.

    uv run poe setup-odds           # config topic + three data topics + ClickHouse schema
    uv run poe request-pair …      # curate a pair (validated); see request.py
    uv run poe run-odds-polymarket  # stage 1a: poll Polymarket -> odds-quotes
    uv run poe run-odds-kalshi      # stage 1b: poll Kalshi -> odds-quotes
    uv run poe run-odds-radar       # stage 2: fan-in quotes -> odds-margins / odds-signals

Each target selects a stage by name (``python -m examples.odds_arbitrage_radar <stage>``) and
runs it through the shared ``examples._runner``. The demo constants live here, in the ops
caller — the framework reads nothing from the environment. The ``metrics_port``s match the
odds targets in ``prometheus/prometheus.yml``.
"""
from datetime import timedelta

from examples._runner import dispatch, run

from .kalshi import stage as kalshi_stage
from .polymarket import stage as polymarket_stage
from .radar import stage as radar_stage

# 30 s keeps the board live and catches in-game divergence, at ≤4 keyless GETs per pair per
# poll (Polymarket: 1 gamma + 2 books; Kalshi: 1 market) — far under either venue's public
# limits even with a handful of pairs.
POLL_INTERVAL = timedelta(seconds=30)


if __name__ == "__main__":
    dispatch({
        "polymarket": lambda: run(polymarket_stage, application_id="odds-polymarket",
                                  client_id="odds-polymarket-0", metrics_port=9117,
                                  poll_interval=POLL_INTERVAL),
        "kalshi": lambda: run(kalshi_stage, application_id="odds-kalshi",
                              client_id="odds-kalshi-0", metrics_port=9118,
                              poll_interval=POLL_INTERVAL),
        "radar": lambda: run(radar_stage, application_id="odds-radar",
                             client_id="odds-radar-0", metrics_port=9119),
    })

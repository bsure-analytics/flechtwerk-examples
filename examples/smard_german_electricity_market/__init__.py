"""SMARD German Electricity Market — a late-data / revisions monitor.

A two-stage pipeline over the Bundesnetzagentur's public SMARD.de JSON API (German
electricity generation by source, grid load, and day-ahead prices):

* ``ingest`` — an Extractor that polls, per configured series, SMARD's week-file index
  and the week files it points at, **diffs each snapshot against a 48 h revision window
  held in state**, and emits one observation per new-or-corrected quarter-hour to
  ``smard-observations`` (keyed by the interval instant). It also emits ``settled``
  punctuation markers as intervals age out of the window — the poller owns the clock,
  so the poller emits the punctuation.
* ``mix`` — a Transformer that co-partition-joins every series *at the same instant*
  (the join key is time) into one generation-mix record per interval: total generation,
  renewables share, a CO₂ intensity estimate, load, residual load, and price. A
  ``settled`` marker flips the record to ``is_final`` and tombstones the interval's join
  state, keeping the store bounded to the live window.

The output lands in ClickHouse (Kafka-engine queue → materialized views) and drives a
Grafana board: the generation mix over time, the day-ahead price (into tomorrow),
renewables share and CO₂ intensity, and a **corrections feed** built from the revisions
the ingest diff detects — the panel that makes the late-data theme visible.

Why this example exists: it teaches the one shape the others don't — **a source that
revises already-published data**, and the preliminary → revised → final lifecycle that
absorbs it. SMARD data is CC BY 4.0 (``Bundesnetzagentur | SMARD.de``). See ``README.md``.
"""

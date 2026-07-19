"""ADS-B flight tracker — a three-stage Extractor→Transformer pipeline.

``ingest`` (the package entry point, ``python -m examples.adsb_flight_tracker``)
wraps the raw adsb.lol feed onto ``adsb-raw``; ``enrich``
(``python -m examples.adsb_flight_tracker.enrich``) unrolls and live-enriches it
onto ``adsb-aircraft``/``adsb-events``/``adsb-cells``; ``conflict``
(``python -m examples.adsb_flight_tracker.conflict``) self-joins the cells into
near-miss events. To avoid a runpy double-import warning, the runnable ``enrich``
and ``conflict`` submodules are imported from their own modules, not re-exported
here.
"""
from .ingest import CONFIG_TOPIC, RAW_TOPIC, AdsbIngest, wrap_response

__all__ = ["AdsbIngest", "CONFIG_TOPIC", "RAW_TOPIC", "wrap_response"]

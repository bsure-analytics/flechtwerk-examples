"""GDELT news stories — a multi-stage pipeline over the GDELT 2.0 15-minute feed.

``ingest`` (``GdeltIngest``, an ``Extractor``) polls GDELT's pointer file, verifies
each announced file's size + MD5, unzips it in memory, and emits every row of the
three tables to ``gdelt-events-raw`` / ``gdelt-mentions-raw`` / ``gdelt-gkg-raw``
(raw capture, ADS-B style: the row nested under its own namespace + provenance).
``GdeltEventCoverage`` co-partition-joins Events ⋈ Mentions on ``GlobalEventID``
(buffering orphan mentions until the event lands); ``GdeltStories`` clusters GKG
articles into stories in keyed state and annotates their coverage from the
``gdelt-outlets`` config table. A ClickHouse sink lands both, and Grafana shows
breaking-news velocity, top stories, and coverage spread.

Run one stage with ``python -m examples.gdelt_news_stories <stage>`` (see
``__main__.py``); ``uv run poe gdelt`` sets up and runs the whole pipeline.
"""

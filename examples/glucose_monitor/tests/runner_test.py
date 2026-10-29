"""Tier 2 — the runner, with the shipped ``flechtwerk.testing`` fakes.

Each stage runs through the framework's real runner over the shipped doubles — no broker, no
network — with LibreLinkUp served by an ``httpx.MockTransport`` that follows the protocol the
client speaks (a sign-in that redirects to a region, the ``account-id`` header, envelopes that
renew the ticket):

- **ingest** drives the real ``ExtractorRunner``/``poll_one`` under a pinned clock: every
  ``graph`` response is backed up as delivered with metadata beside it, the renewed ticket never
  leaves the stage, the session rests on the changelog as ciphertext only, one sign-in serves
  many polls and a restart until the session nears its expiry, the stored credentials change or
  LibreView rejects it, and LibreView's refusals name their remedy.
- **readings** consumes those very records (extractor and transformer wired back to back): the
  first poll emits the whole window, a repeated window emits nothing, a new live reading and a
  history point filled in late each come through once.

The credentials are **ciphertext on the wire**: every test runs inside ``installed_keyring()``
and the config record is built with a typed write, so the stage proves it decrypts them.
"""
import asyncio
import copy
import hashlib
import json
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from flechtwerk import Config, Transformer
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import ExtractorRunner, TokenTask
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import (
    FakeKafkaConsumer,
    FakeKafkaProducer,
    InMemoryStateStore,
    installed_keyring,
    make_record,
)
from flechtwerk.transformer import Task

from examples.glucose_monitor.attributes import (
    ACCOUNT,
    ACCOUNTS_TOPIC,
    EMAIL,
    PASSWORD,
    RAW_TOPIC,
    READINGS_TOPIC,
    REGION,
    SEEN,
    SESSION,
)
from examples.glucose_monitor.client import LoginRefused, fingerprint
from examples.glucose_monitor.ingest import GlucoseIngest
from examples.glucose_monitor.readings import GlucoseReadings

FIXTURES = Path(__file__).parent / "fixtures"
GRAPH = json.loads((FIXTURES / "graph.json").read_text())
PATIENT = GRAPH["data"]["connection"]["patientId"]

UTC = timezone.utc
NOW = datetime(2026, 10, 29, 7, 14, 0, tzinfo=UTC)
EMAIL_ADDRESS = "follower@example.org"
SECRET = "correct horse battery staple"
USER_ID = "4a5b6c7d-0000-4000-8000-0123456789ab"


@pytest.fixture(autouse=True)
def keyring() -> Iterator[None]:
    with installed_keyring():
        yield


def _account(name: str = "me", *, email: str = EMAIL_ADDRESS, password: str = SECRET,
             region: str | None = None) -> dict:
    """An account's config record as it travels on the wire: email and password are ciphertext."""
    record: dict[Any, Any] = {ACCOUNT: name, EMAIL: email, PASSWORD: password}
    if region is not None:
        record[REGION] = region
    return Config(record).raw


def _sent(producer: FakeKafkaProducer, topic: str) -> list[dict]:
    return [json.loads(payload["value"]) for sent_topic, payload in producer.sent if sent_topic == topic]


def _keys(producer: FakeKafkaProducer, topic: str) -> list[str]:
    return [payload["key"].decode() for sent_topic, payload in producer.sent if sent_topic == topic]


def test_the_config_record_carries_ciphertext_not_the_credentials() -> None:
    raw = _account(region="de")
    assert raw["email"].startswith("flenc:jwe:") and raw["password"].startswith("flenc:jwe:")
    assert EMAIL_ADDRESS not in json.dumps(raw) and SECRET not in json.dumps(raw)
    assert raw["region"] == "de"
    assert Config.wrap(raw)[PASSWORD] == SECRET


# --- a LibreLinkUp double ---

class Libre:
    """A MockTransport LibreLinkUp: the global host redirects to ``de``, the ``de`` host signs in,
    and every authenticated call checks the headers. Envelopes carry a renewed ticket the stage
    must neither use nor back up. Records every request and every sign-in's credentials."""

    def __init__(self, *, login: dict | None = None, graph_status: int = 200,
                 connections: list[dict] | None = None) -> None:
        self.login_body = login
        self.graph_status = graph_status
        self.connections = [GRAPH["data"]["connection"]] if connections is None else connections
        self.graph = copy.deepcopy(GRAPH)
        self.requests: list[httpx.Request] = []
        self.sign_ins: list[dict] = []
        self.token: str | None = None
        self.revoked: set[str] = set()
        self.clock = {"now": NOW}

    @property
    def logins(self) -> int:
        return len(self.sign_ins)

    def _renewed(self) -> dict:
        return {"token": f"renewed-{len(self.requests)}", "expires": 0, "duration": 0}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        headers = {"Date": format_datetime(self.clock["now"], usegmt=True)}
        assert request.headers["product"] and request.headers["version"]
        if request.url.path == "/llu/auth/login":
            if request.url.host == "api.libreview.io":
                return httpx.Response(200, json={"status": 0, "data": {"redirect": True, "region": "de"}})
            assert request.url.host == "api-de.libreview.io"
            if self.login_body is not None:
                return httpx.Response(200, json=self.login_body)
            self.sign_ins.append(json.loads(request.content))
            self.token = f"token-{self.logins}"
            return httpx.Response(200, json={"status": 0, "data": {
                "user": {"id": USER_ID, "firstName": "Max"},
                "authTicket": {"token": self.token, "duration": 21_600_000,
                               "expires": int((self.clock["now"] + timedelta(hours=6)).timestamp())}}})
        if request.headers["authorization"] in {f"Bearer {token}" for token in self.revoked}:
            return httpx.Response(401, json={"status": 401, "error": {"message": "unauthorized"}})
        assert request.headers["authorization"] == f"Bearer {self.token}"  # the sign-in's, not a renewal
        assert request.headers["account-id"] == hashlib.sha256(USER_ID.encode()).hexdigest()
        if request.url.path == "/llu/connections":
            return httpx.Response(200, headers=headers, json={
                "status": 0, "data": self.connections, "ticket": self._renewed()})
        assert request.url.path == f"/llu/connections/{PATIENT}/graph"
        if self.graph_status != 200:
            return httpx.Response(self.graph_status, headers=headers, json={})
        return httpx.Response(200, headers=headers, json={**self.graph, "ticket": self._renewed()})

    def stage(self) -> GlucoseIngest:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))
        return GlucoseIngest(client, clock=lambda: self.clock["now"])


# --- ingest: the ExtractorRunner over the fakes ---

def _extractor_runner(stage: GlucoseIngest, config: dict,
                      inner: InMemoryStateStore) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    """Wire a single-token ExtractorRunner over the shipped fakes, seeding one config. ``inner``
    is the local state store — pass the previous run's to model a restart."""
    producer = FakeKafkaProducer()
    runner = ExtractorRunner()
    runner.changelog_topic = "glucose-ingest-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = FakeKafkaConsumer(
        [make_record(key=config["account"], value=json.dumps(config), topic=ACCOUNTS_TOPIC)])
    runner.create_restore_consumer = lambda: FakeKafkaConsumer()
    runner.create_token_producer = lambda token: producer
    runner.extractor = stage
    runner.inner_store = inner
    runner.observer = Observer()
    runner.poll_interval = timedelta(0)
    runner.num_tokens = 1
    runner.tokens = frozenset({0})
    store = ChangelogStateStore()
    store.inner = inner
    store.producer = FakeKafkaProducer()
    store.topic = runner.changelog_topic
    runner.tasks[0] = TokenTask(asyncio.Lock(), producer, store)
    return runner, producer


async def _runner(stage: GlucoseIngest, config: dict | None = None, *,
                  inner: InMemoryStateStore | None = None) -> tuple[ExtractorRunner, FakeKafkaProducer]:
    runner, producer = _extractor_runner(stage, config or _account(), inner or InMemoryStateStore())
    await runner.load_initial_configs()
    return runner, producer


async def _poll(runner: ExtractorRunner, account: str = "me") -> None:
    await runner.poll_one(runner.entries[account])


def _changelog(runner: ExtractorRunner) -> list[str]:
    """Every changelog value the run wrote, as the bytes-turned-text that rest on the topic."""
    return [payload["value"].decode() for _, payload in runner.tasks[0].store.producer.sent
            if payload["value"] is not None]


async def test_ingest_backs_up_each_graph_as_delivered_with_metadata() -> None:
    libre = Libre()
    runner, producer = await _runner(libre.stage())
    await _poll(runner)

    [record] = _sent(producer, RAW_TOPIC)
    assert record["response"] == GRAPH["data"]                    # the payload, untouched
    assert "ticket" not in json.dumps(record)                     # the renewed credential never leaves
    assert "renewed-" not in json.dumps(record)
    assert record["metadata"] == {"account": "me", "patient_id": PATIENT,
                                  "fetched_at": "2026-10-29T07:14:00Z"}  # the server's Date
    assert _keys(producer, RAW_TOPIC) == [PATIENT]                # one patient's polls stay in order
    assert [(r.method, r.url.host, r.url.path) for r in libre.requests] == [
        ("POST", "api.libreview.io", "/llu/auth/login"),          # the global host redirects …
        ("POST", "api-de.libreview.io", "/llu/auth/login"),       # … to the account's region
        ("GET", "api-de.libreview.io", "/llu/connections"),
        ("GET", "api-de.libreview.io", f"/llu/connections/{PATIENT}/graph"),
    ]
    assert libre.sign_ins == [{"email": EMAIL_ADDRESS, "password": SECRET}]  # decrypted on read


async def test_the_session_rests_encrypted_on_the_changelog() -> None:
    libre = Libre()
    runner, _ = await _runner(libre.stage())
    await _poll(runner)

    [stored] = _changelog(runner)
    assert json.loads(stored)["session"].startswith("flenc:jwe:")  # one token, the session whole
    for secret in ("token-1", hashlib.sha256(USER_ID.encode()).hexdigest(),
                   fingerprint(EMAIL_ADDRESS, SECRET), "api-de.libreview.io"):
        assert secret not in stored
    session = (await runner.tasks[0].store.get("me"))[SESSION]     # decrypted on read
    assert session.token == "token-1" and session.host == "api-de.libreview.io"


async def test_ingest_signs_in_at_the_stored_region_directly() -> None:
    libre = Libre()
    runner, _ = await _runner(libre.stage(), _account(region="de"))
    await _poll(runner)
    assert libre.requests[0].url.host == "api-de.libreview.io"


async def test_one_sign_in_serves_many_polls_and_survives_a_restart() -> None:
    libre = Libre()
    inner = InMemoryStateStore()
    runner, producer = await _runner(libre.stage(), inner=inner)
    for minute in range(3):
        libre.clock["now"] = NOW + timedelta(minutes=minute)
        await _poll(runner)
    assert libre.logins == 1                                      # not a login a minute
    assert len(_sent(producer, RAW_TOPIC)) == 3
    assert len(_changelog(runner)) == 1                           # the session is written once

    restarted, _ = await _runner(libre.stage(), inner=inner)      # a new process, the same state
    await _poll(restarted)
    assert libre.logins == 1                                      # the stored session still serves
    assert _changelog(restarted) == []


async def test_ingest_signs_in_again_before_the_session_expires() -> None:
    libre = Libre()
    runner, _ = await _runner(libre.stage())
    await _poll(runner)
    libre.clock["now"] = NOW + timedelta(hours=5, minutes=55)    # inside RENEW_BEFORE of 6 h
    await _poll(runner)
    assert libre.logins == 2
    assert len(_changelog(runner)) == 2


async def test_ingest_signs_in_again_when_the_credentials_change() -> None:
    libre = Libre()
    inner = InMemoryStateStore()
    runner, _ = await _runner(libre.stage(), _account(password="an old password"), inner=inner)
    await _poll(runner)
    runner, _ = await _runner(libre.stage(), _account(), inner=inner)  # request-glucose stored a new one
    await _poll(runner)
    assert [s["password"] for s in libre.sign_ins] == ["an old password", SECRET]


async def test_ingest_signs_in_again_when_libreview_rejects_the_stored_session() -> None:
    libre = Libre()
    inner = InMemoryStateStore()
    runner, _ = await _runner(libre.stage(), inner=inner)
    await _poll(runner)
    libre.revoked.add("token-1")                                  # LibreView revoked it meanwhile
    restarted, producer = await _runner(libre.stage(), inner=inner)
    await _poll(restarted)                                        # remedied, not a crash loop
    assert libre.logins == 2
    assert len(_sent(producer, RAW_TOPIC)) == 1
    assert (await restarted.tasks[0].store.get("me"))[SESSION].token == "token-2"


async def test_ingest_names_the_remedy_when_libreview_refuses_the_credentials() -> None:
    libre = Libre(login={"status": 2, "error": {"message": "notAuthenticated"}})
    runner, _ = await _runner(libre.stage())
    with pytest.raises(LoginRefused, match=r"notAuthenticated.*request-glucose account me") as refused:
        await _poll(runner)
    assert SECRET not in str(refused.value) and EMAIL_ADDRESS not in str(refused.value)


async def test_ingest_says_which_step_the_app_wants_done() -> None:
    libre = Libre(login={"status": 4, "data": {"step": {"type": "tou", "componentName": "AcceptDocument"}}})
    runner, _ = await _runner(libre.stage())
    with pytest.raises(LoginRefused, match=r"'tou' step.*LibreLinkUp app"):
        await _poll(runner)


async def test_ingest_a_500_crashes_the_poll() -> None:
    libre = Libre(graph_status=500)
    runner, _ = await _runner(libre.stage())
    with pytest.raises(httpx.HTTPStatusError):
        await _poll(runner)


async def test_ingest_warns_when_the_account_follows_no_one(caplog: pytest.LogCaptureFixture) -> None:
    libre = Libre(connections=[])
    runner, producer = await _runner(libre.stage())
    with caplog.at_level(logging.WARNING):
        await _poll(runner)
    assert _sent(producer, RAW_TOPIC) == []
    assert "follows no one" in caplog.text


# --- readings: the extractor's records through the TransformerRunner ---

def _make_module(stage: Transformer) -> _FlechtwerkModule:
    module = _FlechtwerkModule()
    module.application_id = "glucose-readings"
    module.client_id = "glucose-readings"
    module.bootstrap_servers = "localhost:9092"
    module.metrics_labels = {}
    module.metrics_port = 0
    module.mqtt = None
    module.stage = stage
    module.consumer = FakeKafkaConsumer([])
    module.runner.tasks[0] = Task(0, FakeKafkaProducer(), InMemoryStateStore())
    return module


class Pipeline:
    """``ingest`` and ``readings`` wired back to back over the fakes: each poll's raw records are
    fed to the readings runner as one batch, so the dedupe runs on what the backup holds."""

    def __init__(self) -> None:
        self.libre = Libre()
        self.readings = _make_module(GlucoseReadings())
        self.offset = 0

    async def start(self) -> "Pipeline":
        self.runner, self.producer = await _runner(self.libre.stage())
        return self

    async def poll(self) -> list[dict]:
        await _poll(self.runner)
        records = []
        for topic, payload in self.producer.sent:
            records.append(make_record(key=payload["key"], value=payload["value"], topic=topic,
                                       partition=0, offset=self.offset))
            self.offset += 1
        self.producer.sent.clear()
        out = self.readings.runner.tasks[0].producer
        out.sent.clear()
        self.readings.runner.consumer = FakeKafkaConsumer(records)
        await self.readings.runner.process_batch(
            await self.readings.runner.consumer.getmany(timeout_ms=1000))
        return _sent(out, READINGS_TOPIC)

    def keys(self) -> list[str]:
        return _keys(self.readings.runner.tasks[0].producer, READINGS_TOPIC)

    async def seen(self) -> dict[str, list[int]]:
        state = await self.readings.runner.tasks[0].store.get(PATIENT)
        return state.get(SEEN) if state else {}


async def test_readings_emits_the_whole_window_once_then_only_news() -> None:
    pipeline = await Pipeline().start()
    first = await pipeline.poll()
    history = GRAPH["data"]["graphData"]
    assert [r["kind"] for r in first].count("history") == len(history)
    assert [r["kind"] for r in first].count("live") == 1
    assert set(pipeline.keys()) == {PATIENT}

    assert await pipeline.poll() == []                            # the same window again: no news

    live = pipeline.libre.graph["data"]["connection"]["glucoseMeasurement"]
    live.update({"FactoryTimestamp": "10/29/2026 7:14:41 AM", "Timestamp": "10/29/2026 8:14:41 AM",
                 "ValueInMgPerDl": 206, "Value": 206})
    late = {**history[0], "FactoryTimestamp": "10/29/2026 6:59:07 AM",
            "Timestamp": "10/29/2026 7:59:07 AM", "ValueInMgPerDl": 201, "Value": 201}
    pipeline.libre.graph["data"]["graphData"] = [*history[:-1], late, history[-1]]
    third = await pipeline.poll()
    assert [(r["kind"], r["ValueInMgPerDl"]) for r in third] == [("history", 201), ("live", 206)]
    assert len((await pipeline.seen())["history"]) == len(history) + 1


async def test_a_reading_is_the_item_spread_through_with_the_computed_fields() -> None:
    pipeline = await Pipeline().start()
    live = next(r for r in await pipeline.poll() if r["kind"] == "live")
    assert live == {
        **GRAPH["data"]["connection"]["glucoseMeasurement"],      # as delivered, PascalCase and all
        "patient_id": PATIENT,
        "kind": "live",
        "measured_at": "2026-10-29T07:13:41Z",                    # FactoryTimestamp, in UTC
        "utc_offset_min": 60,                                     # the phone was on CET
        "fetched_at": "2026-10-29T07:14:00Z",
    }

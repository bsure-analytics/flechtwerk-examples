"""Glucose ingest — an ``Extractor`` that backs up what LibreLinkUp shows a follower, every minute.

Stage 1. A ``glucose-accounts`` config record per LibreLinkUp follower account carries the
account's email and password **encrypted**. Each poll signs in if it must, lists the patients the
account follows (``/llu/connections``) and fetches each one's ``graph`` — the latest measurement
plus the last ~12 hours of history — and emits that response's ``data`` **exactly as delivered**
to ``glucose-raw``, nested under ``RESPONSE`` beside a ``METADATA`` record of what was asked and
when. Keyed by the patient id, so one patient's polls stay in order.

**An extractor is a backup, not a parser.** This stage never interprets a reading: nothing is
parsed, deduped or reshaped, and no ``Attribute`` describes the body. Turning a repeating 12-hour
window into a stream of new readings is the ``readings`` stage's job, so a change of rule there is
a replay of this topic, not a re-fetch (LibreLinkUp can't be re-fetched: it only ever shows the
last 12 hours). The one read of a body is about the *request*, not the data: the patient ids the
``graph`` calls are addressed by. The response envelope's renewed ``ticket`` is a credential and
is **never** backed up — only ``data`` is.

**The state is the session, encrypted.** LibreLinkUp has no "since", so there is no cursor: every
poll replays the same window. What the stage keeps instead is the account's signed-in
:class:`~.client.Session` (``SESSION``, an ``ENCRYPTED`` attribute — ciphertext on the changelog),
because signing in on every poll, or on every restart of a crash loop, is how a LibreView account
gets locked. It signs in again only when the session is about to expire, when the stored
credentials changed (the session carries their digest), or when LibreView rejects it with a 401 —
the one failure this stage remedies itself. A poll that signed in yields the new session after its
messages; any other poll yields no ``State`` at all and commits as one trailing transaction.

**Let it crash.** A 5xx, a timeout or a 429 propagates; the supervisor restarts the stage, which
resumes with the stored session. The one failure an operator must remedy — LibreView refusing the
credentials or wanting new terms accepted — says so (:class:`~.client.LoginRefused`).
"""
import logging
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone

import httpx
from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Record

from .attributes import (
    ACCOUNT,
    ACCOUNTS_TOPIC,
    EMAIL,
    FETCHED_AT,
    METADATA,
    PASSWORD,
    PATIENT_ID,
    RAW_TOPIC,
    REGION,
    RESPONSE,
    SESSION,
)
from .client import LibreLinkUp, LoginRefused, Session, SessionRejected, fingerprint

log = logging.getLogger(__name__)

RENEW_BEFORE = timedelta(minutes=10)
"""Sign in again this long before a session expires rather than racing the expiry mid-poll."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class GlucoseIngest(Extractor):
    """Backs up every followed patient's ``graph`` response, per poll → ``glucose-raw``.

    Subclasses ``Extractor`` to own the ``httpx`` client (built in ``__aenter__``, closed in
    ``__aexit__``); tests inject a ``MockTransport`` client and a pinned clock. The credentials
    are on the config record and the session in the state, so a module-level ``stage``
    singleton is fine.
    """

    config_topics = [ACCOUNTS_TOPIC]

    def __init__(self, client: httpx.AsyncClient | None = None, *, raw_topic: str = RAW_TOPIC,
                 clock: Callable[[], datetime] | None = None, **protocol: str) -> None:
        super().__init__()
        self._client = client
        self._topic = raw_topic
        self._clock = clock or utc_now
        self._protocol = protocol  # PRODUCT/VERSION overrides for the client

    async def __aenter__(self) -> "GlucoseIngest":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))  # pragma: no cover — live path
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()  # pragma: no cover — live path

    def _libre(self) -> LibreLinkUp:
        assert self._client is not None, "client is opened in __aenter__ or injected"
        return LibreLinkUp(self._client, **self._protocol)

    async def _sign_in(self, config: Config) -> Session:
        account = config[ACCOUNT]
        try:
            session = await self._libre().login(config[EMAIL], config[PASSWORD],
                                                region=config.get(REGION))
        except LoginRefused as refused:
            raise LoginRefused(f"LibreView refused the sign-in of account {account!r} ({refused}); "
                               f"fix it, then re-run: uv run poe request-glucose account {account}") from None
        log.info("%s: signed in at %s", account, session.host)
        return session

    def _usable(self, session: Session | None, config: Config) -> bool:
        """A stored session serves this poll unless it is about to expire or was opened with
        credentials other than the ones stored now."""
        renew_at = (self._clock() + RENEW_BEFORE).timestamp()
        return (session is not None and session.expires > renew_at
                and session.fingerprint == fingerprint(config[EMAIL], config[PASSWORD]))

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """One ``graph`` response per followed patient, backed up raw; then the session."""
        account = config[ACCOUNT]
        libre = self._libre()
        session = state.get(SESSION)  # decrypted by the codec on read
        signed_in = not self._usable(session, config)
        if signed_in:
            session = await self._sign_in(config)
        try:
            connections = await libre.get(session, "/llu/connections")
        except SessionRejected:
            log.info("%s: LibreView rejected the stored session; signing in again", account)
            session, signed_in = await self._sign_in(config), True
            connections = await libre.get(session, "/llu/connections")
        # The one read of a body: the ids the graph calls are addressed by.
        patient_ids = [connection["patientId"] for connection in connections.data or []]
        if not patient_ids:
            log.warning("%s: the account follows no one — accept a LibreLinkUp invitation first", account)
        for patient_id in patient_ids:
            graph = await libre.get(session, f"/llu/connections/{patient_id}/graph")
            yield Message(key=patient_id, topic=self._topic, value=Event({
                METADATA: Record({ACCOUNT: account, PATIENT_ID: patient_id,
                                  FETCHED_AT: graph.date or self._clock()}),
                RESPONSE: Record.wrap(graph.data),
            }))
        if signed_in:
            # Only a new session is yielded: encryption is randomized, so re-yielding the same
            # session would be fresh ciphertext — a changelog write every poll for nothing.
            yield State({SESSION: session})


stage = GlucoseIngest()

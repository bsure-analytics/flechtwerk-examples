"""The LibreLinkUp client — sign-in with its region redirect, and the authenticated GET.

LibreLinkUp is the *follower* side of Abbott's FreeStyle Libre cloud: a Libre 3 user invites a
follower, and the follower's app reads the patient's glucose from LibreView. Abbott publishes no
API for it. The protocol here is the one the community clients speak — ``nightscout-librelink-up``
(MIT) was the reference for the paths, the headers and the response shapes — so it can change
without notice, and ``PRODUCT``/``VERSION`` must track a LibreLinkUp app version LibreView still
accepts.

The protocol, as used here:

* ``POST /llu/auth/login`` with ``{email, password}``. An account outside the host's region gets
  ``{"data": {"redirect": true, "region": "de"}}`` instead of a session; the client signs in again
  at that region's host. A session is an ``authTicket`` (a bearer token and its ``expires`` in
  epoch seconds) plus the user's id.
* Every later request carries the bearer token and an ``account-id`` header — the SHA-256 hex of
  the user id (LibreView started demanding it in 2025 and broke every client that didn't send it).
* Every response is an envelope ``{status, data, ticket}``: ``status`` 0 is success, ``data`` is
  the payload, and ``ticket`` is a renewed auth ticket. The client **ignores** the renewal and
  keeps the sign-in's ticket until its ``expires`` — the reference client works that way — so a
  session changes only when the account signs in. The renewed ticket is a credential all the
  same: it never leaves this module.

Shared by ``ingest`` (which polls through it) and ``request`` (which signs in once to check the
credentials and learn the region before it stores them). It holds no state of its own — a
:class:`Session` is a value the caller keeps; ``ingest`` keeps it **encrypted** in its state.
"""
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

GLOBAL_HOST = "api.libreview.io"
"""Where a sign-in starts when the account's region is unknown; it redirects to the region."""

REGION_HOSTS = {
    "ae": "api-ae.libreview.io",
    "ap": "api-ap.libreview.io",
    "au": "api-au.libreview.io",
    "ca": "api-ca.libreview.io",
    "de": "api-de.libreview.io",
    "eu": "api-eu.libreview.io",
    "eu2": "api-eu2.libreview.io",
    "fr": "api-fr.libreview.io",
    "jp": "api-jp.libreview.io",
    "la": "api-la.libreview.io",
    "us": "api-us.libreview.io",
    "ru": "api.libreview.ru",
    "cn": "api-cn.myfreestyle.cn",
}
"""LibreView's regional API hosts, keyed by the region a login redirect names."""

PRODUCT = "llu.android"
VERSION = "4.16.0"
"""The ``product``/``version`` headers LibreView expects from its own app. LibreView refuses
versions it considers too old, so this is the constant to bump when sign-in starts failing."""


class LoginRefused(RuntimeError):
    """LibreView answered the sign-in, but not with a session — wrong credentials, or a step the
    LibreLinkUp app wants done first (accepting new terms, typically)."""


class SessionRejected(RuntimeError):
    """LibreView answered an authenticated call with 401: the session is no longer valid, and
    signing in again is the remedy."""


@dataclass(frozen=True)
class Session:
    """A signed-in LibreLinkUp session — a value, kept by the caller. Every field is either a
    credential or derived from one, so it is only ever stored encrypted, whole."""

    host: str
    """The regional API host the account signed in at."""
    token: str
    """The bearer token of the sign-in's auth ticket."""
    expires: int
    """When ``token`` expires, in epoch seconds."""
    account_id: str
    """The ``account-id`` header: SHA-256 hex of the user id."""
    fingerprint: str
    """A digest of the credentials the session was opened with, so a caller can tell that the
    stored credentials have changed since (see :func:`fingerprint`)."""


@dataclass(frozen=True)
class Reply:
    """An authenticated GET's payload, with the server's clock."""

    data: Any
    """The envelope's ``data`` exactly as delivered — never the envelope's ``ticket``."""
    date: datetime | None
    """The response's ``Date`` header as aware UTC, or ``None`` when it had none."""


def host_for(region: str | None) -> str:
    """The API host for a region, or the global host for none."""
    if region is None:
        return GLOBAL_HOST
    try:
        return REGION_HOSTS[region.lower()]
    except KeyError:
        raise ValueError(f"unknown LibreView region {region!r} — one of {', '.join(REGION_HOSTS)}") from None


def fingerprint(email: str, password: str) -> str:
    """A digest of the credentials. Unsalted, so it is a secret in its own right — a weak password
    could be guessed back from it — and it is stored only inside the encrypted session."""
    return hashlib.sha256(f"{email}\0{password}".encode()).hexdigest()


class LibreLinkUp:
    """The two operations the pipeline needs, over an injected ``httpx`` client."""

    def __init__(self, client: httpx.AsyncClient, *, product: str = PRODUCT,
                 version: str = VERSION) -> None:
        self._client = client
        self._headers = {"product": product, "version": version, "accept": "application/json"}

    async def login(self, email: str, password: str, *, region: str | None = None) -> Session:
        """Sign in, following at most one region redirect.

        A 401/403, or an answer that is neither a redirect nor a session, raises
        :class:`LoginRefused` with LibreView's own reason. Anything else that fails propagates —
        a 5xx, or the 429 LibreView answers a lockout after repeated failures with."""
        host = host_for(region)
        for _ in range(2):
            response = await self._client.post(f"https://{host}/llu/auth/login",
                                               json={"email": email, "password": password},
                                               headers=self._headers)
            if response.status_code in (401, 403):
                raise LoginRefused(f"HTTP {response.status_code}: {_reason(response)}")
            response.raise_for_status()
            body = response.json()
            data = body.get("data") or {}
            if data.get("redirect"):
                host = host_for(data["region"])
                continue
            if body.get("status") != 0 or "authTicket" not in data:
                raise LoginRefused(_refusal(body))
            ticket = data["authTicket"]
            return Session(host=host, token=ticket["token"], expires=int(ticket["expires"]),
                           account_id=hashlib.sha256(data["user"]["id"].encode()).hexdigest(),
                           fingerprint=fingerprint(email, password))
        raise LoginRefused(f"LibreView redirected the sign-in twice (last to {host})")

    async def get(self, session: Session, path: str) -> Reply:
        """An authenticated GET, unwrapped from its envelope.

        A 401 raises :class:`SessionRejected` (the caller can sign in again); any other 4xx/5xx
        propagates — let it crash."""
        response = await self._client.get(f"https://{session.host}{path}", headers={
            **self._headers, "authorization": f"Bearer {session.token}",
            "account-id": session.account_id})
        if response.status_code == 401:
            raise SessionRejected(f"LibreLinkUp rejected the session on {path} (401)")
        response.raise_for_status()
        body = response.json()
        if body.get("status") != 0:
            raise RuntimeError(f"LibreLinkUp answered {path} with {_refusal(body)}")
        raw_date = response.headers.get("Date")
        date = parsedate_to_datetime(raw_date).astimezone(timezone.utc) if raw_date else None
        return Reply(data=body.get("data"), date=date)


def _reason(response: httpx.Response) -> str:
    """An error response's reason: the envelope's when it has one, else the start of its text."""
    try:
        return _refusal(response.json())
    except ValueError:
        return response.text[:200] or "no reason given"


def _refusal(body: dict[str, Any]) -> str:
    """LibreView's reason for a non-zero status, as legible as the envelope allows.

    Status 4 is a *step* the LibreLinkUp app wants done (``data.step.type`` — ``tou``/``pp`` are
    new terms of use and privacy policy); everything else carries ``error.message``. Never echoes
    the request, so no credential reaches a log."""
    status = body.get("status")
    step = ((body.get("data") or {}).get("step") or {}).get("type")
    if step:
        return (f"status {status}: LibreLinkUp wants the {step!r} step done first — sign in to "
                "the LibreLinkUp app with this account once and accept what it asks")
    message = (body.get("error") or {}).get("message") or "no reason given"
    return f"status {status}: {message}"

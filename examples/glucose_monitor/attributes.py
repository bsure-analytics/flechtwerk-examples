"""Typed attributes, topic names, and state keys for the Glucose Monitor example.

Two schemas meet here, owned by different parties. A **reading** is LibreLinkUp's own glucose item
(``FactoryTimestamp``, ``ValueInMgPerDl``, ``TrendArrow``, …), spread through as delivered, so
only the fields ``readings`` *computes* are declared below (``PATIENT_ID``, ``KIND``,
``MEASURED_AT``, ``UTC_OFFSET_MIN``, ``FETCHED_AT``). Declaring Abbott's fields would re-declare a
schema we don't own, from a reverse-engineered API that changes without notice. The **config
record** is ours, so it is fully declared.

**The credentials ride on every account record.** The config store merges every config topic a
stage declares into one namespace, and the extractor polls every entry of it, so a separate
"credentials" record would be polled as if it were an account. LibreLinkUp has no API keys: a
follower signs in with an email and a password, both ``ENCRYPTED(STR)`` here — ciphertext
(``flenc:jwe:…``) on the wire, plaintext only inside a stage that holds the keyring. The email is
encrypted too, because it is personal data and nothing but the login reads it.

**Every secret is encrypted wherever it rests.** The credentials on the config topic, and the
LibreLinkUp session the extractor keeps as its state — a bearer token, plus a digest of the
credentials — are both ``ENCRYPTED``, so neither the config topic nor the changelog ever holds one
in the clear.

**Every clock is the sensor's or the server's, never a wall clock read mid-logic.**
``MEASURED_AT`` is the sensor's UTC ``FactoryTimestamp``; ``FETCHED_AT`` is the server ``Date`` of
the response that carried the reading.
"""
from dataclasses import asdict
from typing import Final

from flechtwerk.attribute import Attribute, Codec, DATETIME, DICT, INT, LIST, RECORD, STR
from flechtwerk.secrets import ENCRYPTED

from .client import Session

# --- Topics (the wire contract) ---

ACCOUNTS_TOPIC: Final = "glucose-accounts"
"""Compacted config topic, one record per LibreLinkUp follower account, keyed by a slug the
operator picks (``me``). Written by ``request-glucose`` (the credentials go in encrypted), never
seeded by setup."""
RAW_TOPIC: Final = "glucose-raw"
"""The extractor's backup: one record per followed patient per poll, the ``graph`` response's
``data`` **exactly as LibreLinkUp delivered it**, nested under ``RESPONSE`` with the request's
provenance under ``METADATA`` (the ``adsb-raw`` shape). Keyed by the patient id, so one patient's
polls stay in order on one ``readings`` task."""
READINGS_TOPIC: Final = "glucose-readings"
"""One record per new reading — a ``live`` one or a ``history`` one — keyed by the patient id."""

# --- Config record (glucose-accounts; wire key = account slug) ---

ACCOUNT: Final = Attribute("account", STR)
"""The account slug — duplicated into the value so a stage reads it without decoding the key."""
EMAIL: Final = Attribute("email", ENCRYPTED(STR))
"""The LibreLinkUp follower account's sign-in email."""
PASSWORD: Final = Attribute("password", ENCRYPTED(STR))
"""The LibreLinkUp follower account's password. ``ENCRYPTED`` makes a typed write produce
ciphertext and a typed read decrypt it with the process keyring — see ``request.py`` for the
write, ``ingest.py`` for the read."""
REGION: Final = Attribute("region", STR, optional=True)
"""LibreView's region for the account (``de``, ``eu``, ``us``, …), learned from the login redirect
by ``request-glucose``. Optional: without it the extractor signs in at the global host and follows
the redirect."""

# --- Extractor state (one record per account) ---

SESSION_CODEC: Final[Codec[Session]] = Codec(decode=lambda raw: Session(**raw), encode=asdict)
"""A :class:`~.client.Session` as a JSON object of its five fields — strict on decode: an unknown
or missing field raises instead of being guessed."""
SESSION: Final = Attribute("session", ENCRYPTED(SESSION_CODEC))
"""The account's signed-in LibreLinkUp session, **encrypted whole** — one ``flenc:jwe:…`` token on
the changelog, so the bearer token, the account id and the credentials' digest never rest in the
clear. It is the extractor's only state: not listing progress (LibreLinkUp has none) but the one
thing worth surviving a restart, because signing in again on every start — or on every crash of a
crash loop — is how a LibreView account gets locked."""

# --- Raw record (glucose-raw; wire key = patient id) ---

METADATA: Final = Attribute("metadata", RECORD)
"""The request's provenance, nested: ``ACCOUNT``, ``PATIENT_ID`` and ``FETCHED_AT``. Everything
here is ours — what we asked for and when — never read out of the body."""
RESPONSE: Final = Attribute("response", RECORD)
"""The ``graph`` response's ``data`` as delivered, ``Record.wrap``-ed at the HTTP edge and never
inspected by the extractor."""

# --- Reading record (glucose-readings; wire key = patient id) — the computed fields ---

PATIENT_ID: Final = Attribute("patient_id", STR)
"""LibreLinkUp's id of the followed patient (a UUID) — the key every downstream table groups by."""
KIND: Final = Attribute("kind", STR)
"""``live`` for the connection's latest measurement (about one a minute from a Libre 3),
``history`` for a point of the ``graphData`` series (the last ~12 hours, on a coarser grid)."""
MEASURED_AT: Final = Attribute("measured_at", DATETIME)
"""When the sensor measured, in UTC — parsed from ``FactoryTimestamp``."""
UTC_OFFSET_MIN: Final = Attribute("utc_offset_min", INT, optional=True)
"""The phone's UTC offset at the time, in minutes: ``Timestamp`` (the phone's local clock) minus
``FactoryTimestamp``, rounded to a quarter hour. It is what lets a daily profile use the local hour
of day even across a time-zone change. Absent when the item carries no ``Timestamp``."""
FETCHED_AT: Final = Attribute("fetched_at", DATETIME)
"""The server ``Date`` of the poll that carried the reading."""

# --- Readings state (one record per patient) ---

SEEN: Final = Attribute("seen", DICT(LIST(INT)))
"""``{kind: [epoch seconds]}`` of every reading already emitted within the dedupe horizon. Each
poll repeats the whole ``graphData`` window, so this set is what turns a repeating snapshot into a
stream of new readings. Pruned by event time (see ``projection.prune_seen``)."""

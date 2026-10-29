"""Store a LibreLinkUp follower account (encrypted), or retire one — written to ``glucose-accounts``.

    uv run poe request-glucose account me              # asks for the email and the password
    uv run poe request-glucose account me de           # … starting at a region's host
    uv run poe request-glucose account me < FILE       # or: email and password as two lines on stdin
    uv run poe request-glucose retire me
    uv run poe request-glucose                         # list what is stored

The ops step that replaces a hard-coded seed — and the one place plaintext meets key material.

**``account`` checks, then encrypts.** It reads the email and password from the terminal (the
password without echo) or from stdin — **never** argv, so they stay out of shell history and
``ps`` — signs in to LibreLinkUp once, and refuses to store credentials LibreView refuses. That one
sign-in also learns the account's **region** from LibreView's redirect, so the stored record lets
the extractor sign in at the right host directly, and it shows whom the account follows: a
follower that follows no one is the usual first-time mistake (the invitation was never accepted).
Then it loads the keyring (``examples._setup.load_keyring``), installs it, and writes the record
with a typed write: ``email`` and ``password`` are ``ENCRYPTED(STR)`` attributes, so they land as
``flenc:jwe:…`` ciphertext and only a process holding the keyring can read them back. The output
shows the account, its region, whom it follows and the ``kid`` — never a credential. Re-running
``account`` is how a changed password or a rotated keyring reaches the topic; the extractor notices
the change and signs in again.

**``retire``** writes a tombstone, which removes the account's record from every store. The topic
is log-compacted, so the latest record per account (or a tombstone) wins.
"""
import asyncio
import getpass
import json
import re
import sys

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from flechtwerk import Config
from flechtwerk.secrets import install_keyring, kid_of

from examples._setup import load_keyring, quiet_fresh_topic_produce_race

from .attributes import ACCOUNT, ACCOUNTS_TOPIC, EMAIL, PASSWORD, REGION
from .client import REGION_HOSTS, LibreLinkUp, LoginRefused

BOOTSTRAP_SERVERS = "localhost:9092"

_SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")


async def stored_accounts(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> dict[str, dict]:
    """Every account currently on the compacted config topic, as ``account -> raw record``.

    Reads the topic from the beginning with **no group id** (a read-only peek — it must never
    commit offsets or disturb the extractor's own consumption) and keeps the last value per key,
    so compaction semantics are reproduced: a later record wins and a tombstone removes the
    account. The records stay **raw** — ciphertext in ``email`` and ``password`` — because nothing
    here needs them. An absent topic (setup not run yet) yields ``{}``."""
    consumer = AIOKafkaConsumer(ACCOUNTS_TOPIC, bootstrap_servers=bootstrap_servers,
                                group_id=None, enable_auto_commit=False,
                                auto_offset_reset="earliest")
    await consumer.start()
    try:
        assignment = list(consumer.assignment())
        if not assignment:
            return {}  # topic absent — setup has not run yet
        starts, ends = (await consumer.beginning_offsets(assignment),
                        await consumer.end_offsets(assignment))
        pending = sum(ends[tp] - starts[tp] for tp in assignment)
        latest: dict[str, dict | None] = {}
        while pending > 0:
            batch = await consumer.getmany(timeout_ms=2000)
            if not batch:
                break  # nothing more forthcoming; take what we have
            for records in batch.values():
                for record in records:
                    pending -= 1
                    latest[record.key.decode()] = json.loads(record.value) if record.value else None
        return {account: raw for account, raw in latest.items() if raw is not None}
    finally:
        await consumer.stop()


def describe(raw: dict) -> str:
    """One account record as ``region=… kid=…`` — the credentials are never shown."""
    return f"region={raw.get('region') or 'global'}  kid={kid_of(raw['password'])}"


async def _write(records: dict[str, dict | None]) -> None:
    """Produce each ``account -> raw record`` (``None`` = tombstone) to the config topic."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for account, raw in records.items():
                await producer.send_and_wait(
                    ACCOUNTS_TOPIC, key=account.encode(),
                    value=None if raw is None else json.dumps(raw).encode())
    finally:
        await producer.stop()


async def check(email: str, password: str, region: str | None) -> tuple[str | None, list[str]]:
    """Sign in once: the account's region (``None`` for the global host) and whom it follows,
    as first names with a last initial. Exits with LibreView's reason when it refuses."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        libre = LibreLinkUp(client)
        try:
            session = await libre.login(email, password, region=region)
        except LoginRefused as refused:
            sys.exit(f"LibreView refused the sign-in ({refused}) — nothing stored")
        connections = await libre.get(session, "/llu/connections")
    hosts = {host: name for name, host in REGION_HOSTS.items()}
    follows = [f"{c.get('firstName', '')} {c.get('lastName', '')[:1]}.".strip()
               for c in connections.data or []]
    return hosts.get(session.host), follows


async def request_account(account: str, email: str, password: str, region: str | None) -> None:
    """Check the credentials, then write them encrypted onto the account's record."""
    keyring = load_keyring()  # before the sign-in: a missing keyring should cost no login
    region, follows = await check(email, password, region)
    install_keyring(keyring)
    record: dict = {ACCOUNT: account, EMAIL: email, PASSWORD: password}
    if region is not None:
        record[REGION] = region
    raw = Config(record).raw  # the typed write encrypts email and password
    await _write({account: raw})
    print(f"{account:8}{describe(raw)}  follows={', '.join(follows) or 'no one'}")
    if not follows:
        print("Note: this account follows no one — accept the LibreLinkUp invitation from the "
              "Libre 3 app's sharing settings, or the extractor will poll an empty list.")


async def retire(account: str) -> None:
    await _write({account: None})
    print(f"Retired: {account}")


async def list_accounts() -> None:
    existing = await stored_accounts()
    if not existing:
        print("No account stored — run: uv run poe request-glucose account <name>")
    for account, raw in sorted(existing.items()):
        print(f"{account:8}{describe(raw)}")


def read_credentials() -> tuple[str, str]:
    """The email and password from the terminal (the password unechoed), or as two lines on
    stdin. The password is taken verbatim — only the line break is removed."""
    if sys.stdin.isatty():
        email = input("LibreLinkUp follower email: ")
        password = getpass.getpass("LibreLinkUp password: ")
    else:
        lines = sys.stdin.read().splitlines()
        email, password = (lines + ["", ""])[:2]
    if not email.strip() or not password:
        print("need an email and a password — type them when asked, or pipe two lines in",
              file=sys.stderr)
        sys.exit(2)
    return email.strip(), password


def usage() -> None:
    sys.exit("usage: python -m examples.glucose_monitor.request account NAME [REGION]\n"
             "   or: python -m examples.glucose_monitor.request retire NAME\n"
             "   or: python -m examples.glucose_monitor.request            (list)")


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        asyncio.run(list_accounts())
    elif argv[0] == "account" and 2 <= len(argv) <= 3 and _SLUG.fullmatch(argv[1]):
        region = argv[2].lower() if len(argv) == 3 else None
        if region is not None and region not in REGION_HOSTS:
            sys.exit(f"unknown region {region!r} — one of {', '.join(REGION_HOSTS)}")
        email, password = read_credentials()
        asyncio.run(request_account(argv[1], email, password, region))
    elif argv[0] == "retire" and len(argv) == 2:
        asyncio.run(retire(argv[1]))
    else:
        usage()


if __name__ == "__main__":
    main()

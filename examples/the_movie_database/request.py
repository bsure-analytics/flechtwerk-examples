"""Store the TMDB token (encrypted), set an epoch, or retire a feed — written to ``tmdb-feeds``.

    uv run poe request-tmdb token < ~/.config/flechtwerk-examples/tmdb-token
    uv run poe request-tmdb since 2026-07-25            # all three feeds
    uv run poe request-tmdb since 2026-07-25 movie      # one feed
    uv run poe request-tmdb retire movie                # or: retire  (all three)
    uv run poe request-tmdb                             # list what is requested

The ops step that replaces a hard-coded seed — and the one place plaintext meets key material.

**``token`` encrypts.** It reads the token from **stdin** (never argv, so it stays out of shell
history and ``ps``), loads the keyring (``examples._setup.load_keyring``), installs it, and
writes one record per feed whose ``access_token`` is an ``ENCRYPTED(STR)`` attribute: the typed
write produces ``flenc:jwe:…`` ciphertext, and only a process holding the keyring can read it
back. The output shows each feed, its epoch and the token's ``kid`` — never the token. Re-running
``token`` is how a rotated TMDB token or a rotated keyring reaches the topic; a feed's existing
``since`` is carried over so the epoch survives it.

**``since`` edits around the ciphertext.** It adds the epoch to the *stored raw record* and
writes it back as-is — no typed ``Config``, no decrypt, no re-encrypt: the stored ciphertext
string travels unchanged, so setting an epoch needs no keyring at all. The epoch only *seeds* a
feed that has never listed (the extractor's cursor is monotonic and never rewinds), so it is
meant to be set before the first poll.

**``retire``** writes a tombstone, which removes the feed's record from every store. The topic is
log-compacted, so the latest record per feed (or a tombstone) wins. Any producer works too
(Kafbat UI included) — but only this tool can write the token encrypted.
"""
import asyncio
import json
import sys
from datetime import date

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from flechtwerk import Config
from flechtwerk.secrets import install_keyring, kid_of

from examples._setup import load_keyring, quiet_fresh_topic_produce_race

from .attributes import ACCESS_TOKEN, FEED, FEEDS, FEEDS_TOPIC, SINCE
from .projection import DEFAULT_LOOKBACK_DAYS

BOOTSTRAP_SERVERS = "localhost:9092"


async def stored_feeds(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> dict[str, dict]:
    """Every feed currently on the compacted config topic, as ``feed -> raw record``.

    Reads the topic from the beginning with **no group id** (a read-only peek — it must never
    commit offsets or disturb the extractor's own consumption) and keeps the last value per key,
    so compaction semantics are reproduced: a later record wins and a tombstone removes the feed.
    The records stay **raw** — ciphertext in ``access_token`` — because nothing here needs the
    token. An absent topic (setup not run yet) yields ``{}``."""
    consumer = AIOKafkaConsumer(FEEDS_TOPIC, bootstrap_servers=bootstrap_servers,
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
        return {feed: raw for feed, raw in latest.items() if raw is not None}
    finally:
        await consumer.stop()


def describe(raw: dict) -> str:
    """One feed record as ``since=… kid=…`` — the token itself is never shown."""
    since = raw.get("since") or f"default: last {DEFAULT_LOOKBACK_DAYS} days"
    return f"since={since}  kid={kid_of(raw['access_token'])}"


async def _write(records: dict[str, dict | None]) -> None:
    """Produce each ``feed -> raw record`` (``None`` = tombstone) to the config topic."""
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        with quiet_fresh_topic_produce_race():
            for feed, raw in records.items():
                await producer.send_and_wait(
                    FEEDS_TOPIC, key=feed.encode(),
                    value=None if raw is None else json.dumps(raw).encode())
    finally:
        await producer.stop()


async def request_token(token: str) -> None:
    """Write the encrypted token onto every feed's record, keeping each feed's epoch."""
    install_keyring(load_keyring())
    existing = await stored_feeds()
    records: dict[str, dict | None] = {}
    for feed in FEEDS:
        record: dict = {FEED: feed, ACCESS_TOKEN: token}
        if since := (existing.get(feed) or {}).get("since"):
            record[SINCE] = date.fromisoformat(since)  # optional: only when one was stored
        records[feed] = Config(record).raw  # the typed write encrypts access_token
    await _write(records)
    for feed, raw in records.items():
        print(f"{feed:7}{describe(raw)}")


async def request_since(day: date, feeds: tuple[str, ...]) -> None:
    """Add the epoch to each stored record, ciphertext untouched."""
    existing = await stored_feeds()
    missing = [feed for feed in feeds if feed not in existing]
    if missing:
        sys.exit(f"no record for {', '.join(missing)} — run first: "
                 f"uv run poe request-tmdb token < <token file>")
    await _write({feed: {**existing[feed], "since": day.isoformat()} for feed in feeds})
    for feed in feeds:
        print(f"{feed:7}since={day.isoformat()}")
    print("Note: the cursor is monotonic — `since` only seeds a feed that has never listed.")


async def retire(feeds: tuple[str, ...]) -> None:
    await _write({feed: None for feed in feeds})
    print(f"Retired: {', '.join(feeds)}")


async def list_feeds() -> None:
    existing = await stored_feeds()
    if not existing:
        print("No feeds requested — run: uv run poe request-tmdb token < <token file>")
    for feed, raw in sorted(existing.items()):
        print(f"{feed:7}{describe(raw)}")


def _feeds(argv: list[str]) -> tuple[str, ...]:
    """The feed argument, or all three; anything else exits with the usage."""
    if not argv:
        return FEEDS
    if len(argv) == 1 and argv[0] in FEEDS:
        return (argv[0],)
    usage()


def usage() -> None:
    sys.exit("usage: python -m examples.the_movie_database.request token < FILE\n"
             "   or: python -m examples.the_movie_database.request since YYYY-MM-DD [movie|tv|person]\n"
             "   or: python -m examples.the_movie_database.request retire [movie|tv|person]\n"
             "   or: python -m examples.the_movie_database.request            (list)")


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        asyncio.run(list_feeds())
    elif argv == ["token"]:
        token = sys.stdin.read().strip()
        if not token:
            print("no token on stdin — pipe it in: "
                  "uv run poe request-tmdb token < <token file>", file=sys.stderr)
            sys.exit(2)
        asyncio.run(request_token(token))
    elif argv[0] == "since" and 2 <= len(argv) <= 3:
        try:
            day = date.fromisoformat(argv[1])
        except ValueError:
            sys.exit(f"{argv[1]!r} is not a YYYY-MM-DD date")
        asyncio.run(request_since(day, _feeds(argv[2:])))
    elif argv[0] == "retire" and len(argv) <= 2:
        asyncio.run(retire(_feeds(argv[1:])))
    else:
        usage()


if __name__ == "__main__":
    main()

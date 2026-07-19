"""Verifier — the executable exactly-once claim.

Reads `chaos-output` with a **read_committed** consumer (so aborted transactions
from SIGKILLed runs are invisible) and checks the two things EOS promises:

- **zero duplicates** — every ``n`` appears exactly once (``total == distinct``),
- **zero gaps** — the counter ``seq`` is exactly ``1..N`` and every input ``n``
  is present.

`main()` also runs the one ClickHouse query over the sunk `chaos_output` table,
the second, independent confirmation the plan calls for.
"""
import asyncio
import json

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition

from .transformer import OUTPUT_TOPIC

BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_URL = "http://localhost:8123"


async def committed_values(bootstrap: str = BOOTSTRAP_SERVERS, topic: str = OUTPUT_TOPIC) -> list[dict]:
    """Every committed record on ``topic`` (partition 0), decoded — read_committed.

    Terminates only when the fetch position reaches the end offset captured at
    entry — never on an empty poll. That distinction matters here more than
    anywhere: the output topic is riddled with aborted-transaction gaps from the
    SIGKILLed runs that a read_committed consumer skips over, and fetch backoff
    yields empty polls too, so an empty poll is emphatically *not* end-of-log —
    breaking on one would silently under-read and report a false FAIL. Reading the
    last stable offset (the read_committed end offset) is what terminates the read.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        await consumer.seek_to_beginning(tp)
        end = (await consumer.end_offsets([tp]))[tp]
        values: list[dict] = []
        while await consumer.position(tp) < end:
            batch = await consumer.getmany(tp, timeout_ms=2000)
            for records in batch.values():
                values.extend(json.loads(record.value) for record in records)
        return values
    finally:
        await consumer.stop()


async def verify(bootstrap: str, target: int) -> dict:
    """Summarize the committed output against ``target`` inputs (0..target-1)."""
    values = await committed_values(bootstrap)
    ns = [v["n"] for v in values]
    seqs = [v["seq"] for v in values]
    result = {
        "total": len(values),
        "distinct_n": len(set(ns)),
        "duplicates": len(ns) != len(set(ns)),
        "complete": set(ns) == set(range(target)),
        "seq_exact": set(seqs) == set(range(1, target + 1)),
    }
    result["ok"] = not result["duplicates"] and result["complete"] and result["seq_exact"]
    return result


def _as_int(value: object) -> int | None:
    """Coerce a ClickHouse JSON scalar to int, or None if absent.

    ClickHouse serializes 64-bit integers as JSON *strings* when
    ``output_format_json_quote_64bit_integers`` is on (it is off by default on the
    pinned image, but a differently-configured server would flip it), so compare on
    ``int`` rather than trusting the wire type.
    """
    return int(value) if value is not None else None


async def clickhouse_check(target: int, base_url: str = CLICKHOUSE_URL,
                           *, attempts: int = 30, interval_s: float = 1.0) -> dict:
    """One ClickHouse query over the (read_committed) sunk output.

    The Kafka-engine → materialized-view → table path is asynchronous, so the sunk
    rows trail the committed Kafka output. Poll until the row count catches up to
    ``target`` (or the attempts run out), then assert the identity in one query.
    """
    query = ("SELECT count() AS total, uniqExact(n) AS uniq_n, uniqExact(seq) AS uniq_seq, "
             "max(n) - min(n) + 1 AS span FROM flechtwerk.chaos_output FORMAT JSONEachRow")
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        row: dict = {}
        for attempt in range(attempts):
            response = await client.post("/", content=query)
            response.raise_for_status()
            row = json.loads(response.text.strip() or "{}")
            if _as_int(row.get("total")) == target:
                break
            if attempt < attempts - 1:
                await asyncio.sleep(interval_s)
    row["ok"] = target == _as_int(row.get("total")) == _as_int(row.get("uniq_n")) \
        == _as_int(row.get("uniq_seq")) == _as_int(row.get("span"))
    return row


async def main() -> None:
    from .setup import RECORD_COUNT

    kafka = await verify(BOOTSTRAP_SERVERS, RECORD_COUNT)
    print(f"Kafka (read_committed): {kafka}")
    clickhouse = await clickhouse_check(RECORD_COUNT)
    print(f"ClickHouse: {clickhouse}")
    verdict = "PASS — exactly once despite the kills" if kafka["ok"] and clickhouse["ok"] else "FAIL"
    print(f"\n{verdict}")


if __name__ == "__main__":
    asyncio.run(main())

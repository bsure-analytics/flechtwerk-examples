# Chaos Harness — an Exactly-Once Proof

The repo's strongest claim, made executable: **SIGKILL the transformer mid-batch,
on a loop, and downstream still shows zero duplicates and zero gaps.**

```
chaos-input ──▶ sequencer (Transformer) ──▶ chaos-output ──▶ ClickHouse: chaos_output
   0..N-1        state = a running counter      {n, seq}         (read_committed)
                        ▲
                 chaos.py: SIGKILL, restart, SIGKILL, …
```

## What it proves

The `sequencer` transformer stamps every input `n` with a `seq` taken from a
stateful running counter. Output message, counter (changelog), and input-offset
commit ride **one Kafka transaction**, so a SIGKILL leaves a page either fully
applied or fully aborted. On restart, the new instance's transactional producer
**fences** the killed one (`InitProducerId`), restores the counter from the
changelog, and resumes from the committed offset. The result, despite the kills:

- **zero duplicates** — every `n` appears in the output exactly once;
- **zero gaps** — the counter `seq` is exactly `1..N`, every `n` present.

The `chaos.py` sidecar spawns the transformer as a subprocess and `SIGKILL`s it
the instant it commits its first fresh page — squarely mid-run — repeatedly,
before a final copy drains the rest.

## Run it

With the [stack](../../README.md#the-stack) up:

```bash
uv run poe chaos          # quickstart: setup + SIGKILL loop + verify — the whole EOS proof
# ...or step by step:
uv run poe setup-chaos    # fresh topics + 100k input records + ClickHouse schema
uv run poe run-chaos      # SIGKILL the transformer mid-batch, on a loop, until drained
uv run poe verify-chaos   # PASS/FAIL: zero duplicates, zero gaps
```

Run these in order, once per cycle: `setup-chaos` resets the topics, so re-run it
before another `run-chaos` — running `run-chaos` twice without it reprocesses the
input and appends a second copy that `verify-chaos` would (correctly) flag.

A real run looks like:

```
SIGKILL #1 — transformer at input offset 36539/100000
SIGKILL #2 — transformer at input offset 72699/100000
SIGKILL #3 — transformer at input offset 89442/100000
recovered — input fully consumed (100000/100000)
Kafka (read_committed): {total: 100000, distinct_n: 100000, duplicates: False, complete: True, seq_exact: True, ok: True}
ClickHouse:             {total: 100000, uniq_n: 100000, uniq_seq: 100000, span: 100000, ok: True}
PASS — exactly once despite the kills
```

The verifier makes the claim two independent ways: a **read_committed** Kafka
consumer over `chaos-output`, and one ClickHouse query over the sunk table (the
stack configures the Kafka engine to read committed too, so aborted transactions
are never even ingested).

## Tests — the three tiers

```bash
uv run pytest examples/chaos_harness                      # tiers 1 + 2 (Docker-free)
uv run pytest -m integration examples/chaos_harness       # tier 3 (needs Docker; ~30s)
```

1. **`tests/logic_test.py` — pure logic.** The sequencer as an async generator:
   feed the yielded `State` back and the counter continues — the same mechanism
   that, backed by the changelog, survives a crash.
2. **`tests/runner_test.py` — runner tier.** `TransformerRunner.process_batch`
   over the shipped fakes: a same-key batch is sequenced 1, 2, 3 and the counter
   is persisted in the task transaction.
3. **`tests/integration/` — the executable claim.** Against an ephemeral Kafka
   (testcontainers), runs the actual harness — real subprocess, real `SIGKILL`,
   real fencing and changelog restore — then asserts zero duplicates, zero gaps,
   and a gap-free counter.

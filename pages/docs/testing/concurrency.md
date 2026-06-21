---
title: Concurrency & isolation
icon: lucide/git-merge
summary: Test races deterministically — force an exact interleaving with forze.testing, and verify an adapter honors the isolation level it declares
---

A concurrency test that relies on real timing is flaky: the race you want to provoke fires once in a hundred runs, never in CI, and never the same way twice. The fix is to stop racing and start *scheduling* — drive the concurrent transactions through an exact interleaving you choose, so the outcome is the same every run. `forze.testing` ships the driver for that, and it's the basis for verifying an adapter is as honest as it claims.

## Force an interleaving

Write each participant as a coroutine that calls `await gate.checkpoint()` at every point where a context switch is allowed. The `Conductor`'s `schedule` is the global step order — list a participant once per checkpoint it should pass — and only one participant ever runs at a time:

```python
from forze.testing import Conductor, Gate

async def worker(gate: Gate, name: str):
    log.append(f"{name}:start")    # before the first checkpoint — setup
    await gate.checkpoint()
    log.append(f"{name}:work")
    await gate.checkpoint()
    log.append(f"{name}:commit")   # after the last checkpoint — runs to completion

await Conductor(schedule=("A", "A", "B", "B")).run({
    "A": lambda g: worker(g, "A"),
    "B": lambda g: worker(g, "B"),
})
```

The schedule *is* the interleaving: `("A","A","B","B")` runs A to completion then B; `("A","B","A","B")` steps them in lock-step. Same code, different schedule, deterministic outcome each time — the determinism a real race can't give. It's a general primitive (just `asyncio`), independent of the rest of the framework.

## Verify an adapter's isolation

The mock and a real database both *claim* `SNAPSHOT` / `SERIALIZABLE`, and your app's correctness rests on them agreeing. A forced interleaving lets you check it — drive a textbook anomaly and assert the adapter behaves as its declared level requires. **Write skew** is the canonical one: two transactions each read the same rows, decide it's safe to drop one, and write a *different* row, so optimistic-concurrency checks never conflict — only true serializable catches it.

```python
from forze.application.contracts.transaction import IsolationLevel

async def session(gate: Gate, ctx, mine):
    async with ctx.tx_ctx.scope("mock", isolation=IsolationLevel.SERIALIZABLE):
        d1 = await ctx.document.query(SPEC).get(id1)
        d2 = await ctx.document.query(SPEC).get(id2)
        await gate.checkpoint()                       # both read before either writes
        if d1.on_call and d2.on_call:
            await ctx.document.command(SPEC).update(mine, ..., OffCall())
        await gate.checkpoint()
    # commit on scope exit — serializable raises on the read-write conflict
```

Two concurrent transactions are two coroutines, each with its **own context over one shared `MockState`**; the schedule forces both reads before either write, then one commit then the other. At `SNAPSHOT` both commit (write skew permitted) and the invariant breaks; at `SERIALIZABLE` one aborts and it holds. Run the *same* scenario against a real adapter (via testcontainers) and assert it reaches the same verdict, and you've turned "trust the mock" into "verified the mock."

This is one anomaly at one pair of levels, driven by hand. For exhaustive, seed-driven exploration of concurrency, faults, and crashes across your *whole* app — with a minimized, replayable counterexample when something breaks — reach past this to [Deterministic simulation](../dst/overview.md).

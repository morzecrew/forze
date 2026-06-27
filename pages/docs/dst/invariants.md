---
title: Invariants & reachability
icon: lucide/shield-check
summary: Declare what must always hold (the invariant toolkit) and what must sometimes happen (reachability) — DST reads them off the recorded run, no handler instrumentation
---

DST drives your app through thousands of hostile interleavings — concurrent writes, injected faults, reordered effects. The question that's left is the one that matters: through all of that, did your app stay correct? Invariants are how you answer it.

Two shapes of claim matter, and you need both. An **invariant** says a property must *always* hold — *did this survive the interleaving?* A **reachability target** says a state must *sometimes* be reached, because a sweep that runs ten thousand seeds and never drives the dangerous interleaving proves nothing — a green invariant over a fault that never bit is false confidence. You pass both when you build the `Simulation`, and every strategy in the harness checks the same set, reading them off the recorded history — an empty result means the run held, with no handler instrumentation required. This page covers both.

![DST checks each recorded run against invariants that must always hold and reachability targets that must sometimes fire; only when both are green is the confidence real](../_diagrams/light/dst-invariants.svg#only-light){ data-src="../_diagrams/light/dst-invariants.svg#only-light" }
![DST checks each recorded run against invariants that must always hold and reachability targets that must sometimes fire; only when both are green is the confidence real](../_diagrams/dark/dst-invariants.svg#only-dark){ data-src="../_diagrams/dark/dst-invariants.svg#only-dark" }

## What must always hold

The generic `expect(kind, predicate, message=...)` covers most domain rules: it reads the facts your `observe` hook recorded and asserts a predicate over each. The built-ins go further — they read the engine trace *directly*, so they need no handler instrumentation at all:

<div class="grid cards" markdown>

-   :lucide-shield-alert: **`no_unexpected_error()`**

    The zero-instrumentation safety net: any operation that raised a non-domain exception (a bug — `KeyError`, `TypeError`) is a violation; declared `CoreException` domain failures pass. Point DST at *any* app and immediately catch crashes under concurrency.

-   :lucide-copy: **`no_duplicate_effect(kind, by=...)`**

    Each recorded effect is unique on a key — exactly-once. Catches a non-idempotent consumer applying a redelivered message twice.

-   :lucide-lock: **`mutual_exclusion(kind, resource=, start=, end=)`**

    No two holds of a resource overlap — a distributed lock or critical section held correctly, no split-brain across nodes.

-   :lucide-list-checks: **`operation_succeeds(*ops)` · `completes_within(op, seconds)` · `single_key_per_operation(op)`**

    Named ops must reach `ok`; an op must finish within a virtual-time budget; an op must touch one entity key (the *wrong-entity* guard) — all from the trace alone.

-   :lucide-eye: **`read_your_writes(surface, value_field=…)` · `expect_value(surface, predicate)`**

    Value-level (opt into `capture_values`): a keyed read must observe the last value written to it (stale-read guard); every captured write/read value must satisfy a predicate (the *wrong-value* guard).

-   :lucide-unlock: **`no_unclosed_transaction()` · `no_resource_leak(open_op=, close_op=)`**

    Every resource opened must close by end of run: a transaction `enter` with no matching `exit` is an abandoned scope. The general form pairs any open/close ops on the trace — the bug class nobody writes assertions for. (Don't pair with a crash policy — a crash legitimately abandons a scope.)

</div>

These live in the `invariants` namespace, and each is just a factory returning a plain callable:

```python
from forze_dst import Simulation
from forze_dst.invariants import no_unexpected_error, no_duplicate_effect

simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(),
    invariants=[
        no_unexpected_error(),                       # any non-domain crash is a bug
        no_duplicate_effect("charge", by="order"),   # at most one charge per order
    ],
)
```

### Consistency models

Record an object's operations with `record_operation(key, op, args, session=…)` and DST can check the history against a **model**, not just a hand-written rule — the same recorded history, read against a stronger or weaker guarantee:

- **`linearizable(spec)`** — the strongest single-object guarantee: every concurrent execution must be equivalent to some legal serial order *consistent with real time* (Wing & Gong, per key). A read may never see a value an earlier-completed write superseded.
- **`sequential(spec)`** — drops the real-time constraint and keeps only per-session program order. A replica that serves a slightly stale but program-order-respecting read is *sequentially* consistent though not linearizable — `sequential` accepts it, `linearizable` flags it.
- **`monotonic_reads()`** — a session guarantee: within one session, successive reads of a key never go backward in version (the lagging-replica bug). Sound and incomplete — it flags only a *definitive* regression, so it never cries wolf on concurrent writes.

This is the spectrum from strongest to weakest, and it lines up with what an operation *declares*: a handler that runs at `IsolationLevel.SERIALIZABLE` should pass `linearizable`; a weaker level admits the weaker models. Snapshot isolation and serializability are checked separately — across a transaction's read and write *sets* rather than one object's history — in the next subsection.

### Transactional isolation

Consistency models read one object's history; the transactional oracles read *across* a transaction's read and write sets — grouped by transaction from the trace — to catch the multi-object anomalies isolation levels exist to prevent.

- **`snapshot_isolation()`** flags a **lost update** among the committed transactions; write skew is permitted under SI, so it stays clean. Capture-free.
- **`serializable()`** is, by default, a capture-free **pairwise** check — sound but incomplete, catching lost update and two-transaction write skew. **`serializable(complete=True)`** upgrades to a **dependency-graph** check that is sound *and* complete for conflict-serializability over the captured history: it catches anti-dependency cycles spanning three or more transactions (the read-only anomaly) and predicate **phantoms** — a scan that would have seen a concurrent insert. Complete mode reads the value trace, so it needs `capture_values` and fails closed without it.

Let the level an operation *declares* pick the oracle. `isolation_oracle_for(level)` maps `SERIALIZABLE` → `serializable(complete=True)` and `SNAPSHOT` → `snapshot_isolation()`; `READ_COMMITTED` has no serialization-graph guarantee, so it raises rather than hand back a check that passes on anything — the run is judged at the level it claims:

```python
from forze.application.contracts.transaction import IsolationLevel
from forze_dst.invariants import isolation_oracle_for

invariants = [isolation_oracle_for(IsolationLevel.SERIALIZABLE)]
```

An isolation oracle has a trap the others don't: a green result is meaningless if the run never **contended**. `had_isolation_conflict(history)` answers the prerequisite — did two concurrent committed transactions actually touch the same key, the teeth the oracle needs? Assert `any(had_isolation_conflict(h) for h in histories)` across the sweep, the same way you assert [reachability](#what-must-sometimes-happen), so a clean isolation result rests on a run that genuinely stressed it.

One failure needs no invariant at all: a **deadlock**. If an interleaving drives the workload to a standstill — every task blocked, with no ready work and no timer to advance — that is always a bug, so DST reports it on its own. It minimises to the smallest workload that still stalls and comes back as a `no_deadlock` violation, exactly like an asserted one, instead of aborting the sweep. You catch it for free, even with no invariants declared.

A last cross-cutting claim is **convergence**. If an operation declares itself order-independent, `commutative_convergence(build, final_state=…, schedule_seeds=…)` reruns the workload across a band of schedules and flags a declared-commutative op whose interleavings reach *different* end states, naming the seed that diverged — a cross-history check, like reachability, not a per-run invariant.

!!! note "Value-level invariants need `capture_values`"

    By default the trace is **id-only** — it records *which* key and outcome, never the value (no
    PII, no cost; the production posture). Set `SimulationConfig(capture_values=True)` and the trace
    additionally carries a redaction-applied view of each write payload and read result, so
    `read_your_writes` / `expect_value` can assert on the actual values. Capture is sim-only (the
    data is synthetic), and any field a spec marks sensitive (`encryption.encrypted`/`.searchable`)
    is masked to `<redacted>` even when captured.

## What must sometimes happen

The dual of an invariant is a *reachability* target. Mark a notable state with `reached(label)` from inside code under simulation — it is ambient and a no-op outside a recorded run, exactly like `record_event` — then assert across the sweep that every declared target actually fired:

```python
from forze_dst.markers import reached
from forze_dst.invariants import assess_reachability

# in the scenario, where the hard state happens:
reached("lock-contended")     # a peer held the lock — genuine contention occurred
reached("write-partitioned")  # the partition struck during the guarded write

# after the sweep — cluster.histories() runs every seed (no short-circuit):
report = assess_reachability(cluster.histories(config), targets={
    "lock-contended", "write-partitioned",
})
assert report.satisfied, report.format()   # a target no seed reached is a failure
```

A declared target that *no* seed ever reaches is a reachability failure — the safety result was never tested against that case. `SimulationConfig(reachability_targets=...)` folds the same check into a `coverage()` sweep (the outcome rides on `CoverageStats.reachability`), and `sometimes(histories, predicate)` is the general per-sweep form over an arbitrary predicate.

Invariants decide *whether* a run is a bug; the next step is making bugs likely — [inject the environment](environment.md) a production system actually runs in.

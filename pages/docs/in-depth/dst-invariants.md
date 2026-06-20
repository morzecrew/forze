---
title: Invariants & reachability
icon: lucide/shield-check
summary: Declare what must always hold (the invariant toolkit) and what must sometimes happen (reachability) — DST reads them off the recorded run, no handler instrumentation
---

An invariant is the question DST asks of every run: *did this property survive the interleaving?* It reads the recorded history and returns the violations it found — an empty list means the run held. You pass invariants when you build the `Simulation`, and every strategy in the harness checks against the same set.

Two shapes of claim matter, and you need both. An **invariant** says a property must *always* hold. A **reachability target** says a state must *sometimes* be reached. A sweep that runs ten thousand seeds and never drives the dangerous interleaving proves nothing — a green invariant over a fault that never bit is false confidence. This page covers both.

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

For the strongest single-object guarantee, `linearizable(spec)` checks a recorded operation history against a sequential specification (Wing-Gong, per key) — every concurrent execution must be equivalent to *some* legal serial order.

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

Invariants decide *whether* a run is a bug; the next step is making bugs likely — [inject the environment](dst-environment.md) a production system actually runs in.

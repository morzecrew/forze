---
title: Deterministic simulation
icon: lucide/dice-6
summary: Point DST at a real app — one seed reproduces the whole run (schedule, faults, latency, crashes, partitions) and hands back a minimal counterexample
---

[Testing](testing.md) swaps real adapters for in-memory ones so a handler runs without Docker. Deterministic Simulation Testing (DST) goes further: it takes your **real operations**, runs them concurrently on a virtual-time event loop, and explores the interleavings, faults, and delays a production system would hit — then, when an invariant breaks, hands back the **smallest workload that still breaks it**, reproducible from a single seed.

Nothing in your app changes. Handlers talk to ports exactly as they do in production; the simulation lives entirely on the test side.

!!! tip "The one promise"

    One master seed parametrises **every** source of nondeterminism — the operation
    interleaving, injected faults, simulated latency, generated inputs, crash points, and
    network partitions. `(your app, seed)` is a pure function: the same seed replays the
    exact same run, byte for byte. That is what makes a found bug reproducible instead of
    "flaky."

![One master seed derives independent sub-seeds — schedule, faults, latency, inputs — that drive a deterministic, replayable run](../_diagrams/light/dst-seed.svg#only-light){ data-src="../_diagrams/light/dst-seed.svg#only-light" }
![One master seed derives independent sub-seeds — schedule, faults, latency, inputs — that drive a deterministic, replayable run](../_diagrams/dark/dst-seed.svg#only-dark){ data-src="../_diagrams/dark/dst-seed.svg#only-dark" }

## Point it at your app

A `Simulation` needs three things: your operation **registry**, a **deps factory** (one `MockDepsModule()` auto-mocks every port, built fresh per run so each starts clean), and the **invariants** that must hold. An optional `observe` hook records domain facts the invariants assert over.

```python
--8<-- "recipes/dst_payments/app.py:simulation"
```

Then sweep seeds:

```python
from forze_dst import SimulationConfig

report = simulation.run(SimulationConfig(seeds=range(64)))

if report is not None:
    print(report.format())  # a readable, reproducible counterexample
```

`run` generates a meaningful workload (it reads your operation catalog to build an arrange→act scenario — `forze dst derive` below prints it), runs it under perturbed interleavings, and checks the invariants. On the first violating seed it **minimises** the workload to a 1-minimal set that still fails and returns a `ViolationReport`; a clean sweep returns `None`. There is nothing to assert about *how* — point it at the app and go.

The app under test here is ordinary Forze code. `pay_order` charges, then flips the order to paid — but it charges *before* the optimistic-concurrency-guarded transition, so two concurrent payments both charge:

```python
--8<-- "recipes/dst_payments/app.py:handler"
```

DST finds the race, shrinks it to **two** contending payments, and reports the seed that reproduces it.

## Invariants — what must hold

An invariant reads the recorded history and returns the violations it found. The generic `expect(kind, predicate, message=...)` (above) covers most domain rules. The built-ins read the engine trace directly, so they need no handler instrumentation:

<div class="grid cards" markdown>

-   :lucide-shield-alert: **`no_unexpected_error()`**

    The zero-instrumentation safety net: any operation that raised a non-domain exception (a bug — `KeyError`, `TypeError`) is a violation; declared `CoreException` domain failures pass. Point DST at *any* app and immediately catch crashes under concurrency.

-   :lucide-copy: **`no_duplicate_effect(kind, by=...)`**

    Each recorded effect is unique on a key — exactly-once. Catches a non-idempotent consumer applying a redelivered message twice.

-   :lucide-lock: **`mutual_exclusion(kind, resource=, start=, end=)`**

    No two holds of a resource overlap — a distributed lock / critical section, or no split-brain across nodes.

-   :lucide-list-checks: **`operation_succeeds(*ops)` · `completes_within(op, seconds)` · `single_key_per_operation(op)`**

    Named ops must reach `ok`; an op must finish within a virtual-time budget; an op must touch one entity key (the *wrong-entity* guard) — all from the trace alone.

</div>

`linearizable(spec)` checks a recorded operation history against a sequential specification (Wing-Gong, per key) for the strongest single-object consistency guarantee.

## Sometimes — prove the dangerous case fired

An invariant says a property must **always** hold. Its dual — a *reachability* target — says a state must **sometimes** be reached. You need both: a sweep that runs 10 000 seeds and never drives the dangerous interleaving proves nothing, and a green invariant over a fault that never bit is *false confidence*.

Mark a notable state with `reached(label)` from inside code under simulation (ambient and a no-op outside a recorded run, like `record_event`), then assert across the sweep that every declared target actually fired:

```python
from forze_dst import reached, assess_reachability

# in the scenario, where the hard state happens:
reached("lock-contended")    # a peer held the lock — genuine contention occurred
reached("write-partitioned")  # the partition struck during the guarded write

# after the sweep — Cluster.histories() runs every seed (no short-circuit):
report = assess_reachability(cluster.histories(config), targets={
    "lock-contended", "write-partitioned",
})
assert report.satisfied, report.format()   # a target no seed reached is a failure
```

A declared target that *no* seed ever reaches is a reachability failure — the safety result was never tested against that case. `SimulationConfig(reachability_targets=...)` folds the same check into a `coverage()` sweep (the outcome rides on `CoverageStats.reachability`), and `sometimes(histories, predicate)` is the general per-sweep form over an arbitrary predicate.

## Inject the environment

A real system errors, times out, and is slow. Declare that environment on the config and DST applies it at the port boundary — over **any** resolved port, seeded from the master seed, with the app untouched.

```python
from forze_dst import FaultPolicy, FaultRule, LatencyProfile, LatencyRule, Exponential

report = simulation.run(SimulationConfig(
    seeds=range(200),
    faults=FaultPolicy(rules=(
        FaultRule(surface="document_command", error=0.2),   # 20% transient failures
    )),
    latency=LatencyProfile(rules=(
        LatencyRule(dist=Exponential(mean=0.05), surface="document_command"),
    )),
))
```

A `FaultRule` matches `(surface, route, op)` and rolls, per matched call: `error` (a retryable failure), `timeout`, `crash`, and the transport behaviours `drop` (silent loss), `duplicate` (redelivery), and `delay` (advance virtual time before the call). A `LatencyProfile` samples a per-route distribution — `Constant`, `Uniform`, `Exponential`, or the **heavy-tailed** `LogNormal` and `Pareto`, whose long right tail produces the p99 blowups that expose timeout and deadline bugs a fixed delay never reaches. Both fault and latency are **seeded by construction** — you never pass an RNG, so the run stays reproducible.

!!! note "Virtual time is free"

    A `delay` or latency advances a *virtual* clock, so a workload spanning minutes of
    simulated time runs in real-wall milliseconds — and time-dependent bugs (a TTL that
    expires mid-operation) surface with no `sleep` in your handlers.

## Crash and restart

Set a `CrashPolicy` and a run becomes a crash → restart → recovery scenario. The process *dies* at a matched port boundary (the in-flight transaction rolls back, committed state persists), then a fresh runtime restarts over the **same persisted store**, an optional `recover` pass redrives interrupted work, and the invariants check the post-recovery world.

```python
from forze_dst import CrashPolicy

report = simulation.run(SimulationConfig(
    seeds=range(64),
    crash=CrashPolicy(surface="document_command", route="orders", op="update"),
))
```

This is how you catch recovery bugs: a charge committed before a crashed follow-up write leaves an orphan that survives the restart when the operation is non-transactional — while the same operation routed through a transaction rolls the partial write back atomically.

## Across N nodes

A `Cluster` runs *N* real runtimes over one shared store, from one master seed, under **network partitions** — the distributed capstone. A partition cuts a node-group off from the gated port surfaces for a window of virtual time (modelled as *unreachable* — a retryable error), so a correct retry/outbox flow heals while a fire-and-forget one loses work. The distributed invariants are the same ones (`mutual_exclusion` for no split-brain, `no_duplicate_effect` for exactly-once).

```python
from forze_dst import Cluster, ClusterConfig, Partition, PartitionSchedule
from forze_mock.state import MockState

async def node(node_id: int, ctx) -> None:
    ...  # this node's work, over ctx ports on the shared store

report = Cluster(deps=lambda state: MockDepsModule(state=state), node=node,
                 state_factory=MockState, invariants=[...]).run(
    SimulationConfig(seeds=range(32), cluster=ClusterConfig(
        nodes=3,
        partitions=PartitionSchedule(
            windows=(Partition(start=0.5, end=1.5, isolated=frozenset({1})),),
            surfaces=frozenset({"queue_command"}),
        ),
    )),
)
```

On a violation the cluster minimises by **dropping nodes** — the smallest cluster that still breaks, usually two.

A window's `loss` turns a clean cut into a **flaky link**: `Partition(start=0.5, end=1.5, isolated=frozenset({1}), loss=0.3)` drops 30 % of node 1's gated calls (seeded) instead of all of them, and overlapping windows on different node groups give an **asymmetric** split. `loss=1.0` (the default) is the hard partition above.

## Guided exploration — fuzz toward new behaviour

A uniform seed sweep knows when to *stop* (coverage plateaus) but not *where to look* — every seed is independent, so behaviour gated behind a rare combination of operations is found only by luck. `Simulation.coverage_guided(config, cases=…)` is **feedback-directed** instead: it keeps a corpus of inputs that each unlocked new behavioural coverage and *mutates* the productive ones (tweak an op, grow or shrink the workload, re-roll the schedule + faults), under an AFL-style power schedule that pushes the newest coverage frontier.

```python
stats = simulation.coverage_guided(
    SimulationConfig(seeds=range(1), count=8, concurrency=4, guided_budget=512),
    cases=[OperationCase(op="deposit"), OperationCase(op="withdraw")],
)
print(stats.format())          # behaviours covered, corpus size, any violation
report = stats.violation       # minimized + reproducible, if a run tripped an invariant
```

The whole run is one seed-derived lineage — corpus, mutations, and all — rooted at the first seed and bounded by `guided_budget`, so it reproduces exactly. It reaches behaviour a uniform sweep misses at equal budget, and stops on the first violation with the same minimized report the other strategies produce.

## The loop — find, reproduce, minimise, regress

![A sweep finds a violation, minimizes the workload, produces a reproducible report, saves the seed to a regression corpus, and replay re-checks it forever](../_diagrams/light/dst-loop.svg#only-light){ data-src="../_diagrams/light/dst-loop.svg#only-light" }
![A sweep finds a violation, minimizes the workload, produces a reproducible report, saves the seed to a regression corpus, and replay re-checks it forever](../_diagrams/dark/dst-loop.svg#only-dark){ data-src="../_diagrams/dark/dst-loop.svg#only-dark" }

A `ViolationReport.format()` renders the whole counterexample: the minimised workload, the concurrency that triggered it, the causal trace (each operation and the port calls it caused), an **injected-environment timeline** (the faults, latency, and partitions the simulator applied, in virtual-time order), and the violated invariant. Everything needed to understand *and* reproduce the failure.

The command line wires the loop end to end against an import string pointing at your `Simulation`:

```bash
# explore — prints the counterexample, exits 1 if one is found (CI-friendly)
forze dst run examples.recipes.dst_payments.app:simulation --seeds 0-200

# inject a broad environment and lock a found seed into a regression corpus
forze dst run app:simulation --fault-error 0.2 --latency 0.05 --save-regression

# re-run every saved seed — the regression guard (exits 1 if any still violates)
forze dst replay

# explore until behaviour saturates, then report what was covered
forze dst coverage app:simulation

# inspect the auto-derived workload and the reactive cascade topology
forze dst derive    app:simulation
forze dst topology  app:simulation
```

`--save-regression` appends the found seed (with the registry fingerprint and the exploration knobs) to a JSON-Lines corpus; `replay` reproduces each saved seed under the configuration it was found with, so a fixed bug stays fixed.

The registry fingerprint catches a *structural* change (a contract or plan fact moved). For a stricter guard, `entry_from_report(…, strict_behavior=True)` also records a `behavioral_fingerprint` — an ordered, PII-free digest of the run's execution-trace shape — so `RegressionEntry.behavior_drifted(history)` flags a replay whose handler *logic* drifted even when its contracts didn't. Opt-in; the default stays structural.

## Scale and portable replay

Because each seed is fully deterministic **in its own process**, inter-seed parallelism is free. `parallel_sweep` fans disjoint seeds across a process pool and folds every worker's result into one picture — violating seeds, the union of behaviours covered, and a throughput metric — so a nightly fuzz explores thousands of timelines per wall-hour:

```python
from forze_dst import parallel_sweep, SimulationSeedRunner

result = parallel_sweep(
    SimulationSeedRunner(target="app:simulation", fault_error=0.1),
    seeds=range(10_000),
    workers=16,
)
print(result.format())          # seeds/s, behaviours, first violating seed
```

The runner holds only a `module:attr` string and primitives, so it pickles across processes where a live `Simulation` could not — each worker re-imports the app and runs one seed.

And a found bug travels: `FailureBundle` serialises the seed *and the full config that produced it* (faults, latency, partitions, crash, scheduler) to one JSON file, and `replay_bundle` re-runs it anywhere.

```python
from forze_dst import bundle_from_report, replay_bundle, FailureBundle

bundle_from_report(report, config, target="app:simulation").save("bug.json")
# … on another machine, another day …
report = replay_bundle(FailureBundle.load("bug.json"))   # reproduces, from one command
```

!!! warning "DST is only as honest as the mock"

    DST trusts that the in-memory transaction manager rolls back faithfully. The default
    (`MockDepsModule(transactions="journal")`) is atomic without serialising, so a found
    race is real. The legacy no-op manager would report *false* double-charges — see
    [Transactions](transactions.md).

## Forze passes its own simulation

DST is judged by the bugs it finds in *real* systems — so Forze runs its own distributed machinery through it. Each scenario pairs a safety invariant (must always hold) with a reachability target (must sometimes fire), so a green result means the property was tested against the hard case, not a quiet run:

| Primitive | Invariant (always) | Reachability (sometimes) |
| --- | --- | --- |
| **Distributed lock** | `mutual_exclusion` + no lost update across N runtimes | a contender spun on the held lock *while* a partition isolated a node mid-write |
| **Hybrid logical clock** | per-replica `monotonic_per` + every merge's HLC exceeds its cause | a replica merged a remote stamp that was ahead of its own |
| **Outbox / crash-restart** | `no_duplicate_effect` (exactly-once) survives a crash mid-flush | the crash landed between the flush and the relay |

Each scenario also keeps a *broken* twin — drop the lock, ignore the remote stamp — that the oracle catches, minimises, and reproduces, so the test proves it can still fail. The unguarded lock races to a lost update; the naive clock breaks causality. That is the bar: the framework's own concurrency code is continuously simulation-tested, and app authors inherit the same harness for free.

## Extending it

DST is built from small, documented seams, so you extend it without forking. Everything plugs in as a plain callable or protocol:

<div class="grid cards" markdown>

-   :lucide-shield-check: **Invariants**

    An `Invariant` is any `Callable[[History], list[Violation]]` — write a function, pass it in `invariants=`. The built-ins are just factories returning one.

-   :lucide-shuffle: **Schedulers**

    `Scheduler` is a `Protocol`; supply a `scheduler_factory` (like `pct_scheduler_factory`) to drive interleavings your own way — the engines call it per run.

-   :lucide-bug: **Environment**

    Faults and latency are declarative data (`FaultPolicy`, `LatencyProfile`); for anything custom, the `interceptors` factory adds a seeded `PortInterceptor` chain at the port seam.

-   :lucide-boxes: **Engines**

    Each strategy is a free function under `forze_dst.engines` (op_case, scenario, crash_restart, guided) taking the `Simulation` as its context — call one directly, or compose your own search over the `forze_dst.context` substrate.

</div>

The `Simulation` class is a thin facade: `run` / `coverage` / `coverage_guided` bind the config and delegate to an engine. The trace seam is deliberately layered — the engine `RuntimeTrace` is the production tracer (id-only, PII-free), and the DST `History` is the oracle's richer view that folds it in *and* adds DST-only events (op_start anchors, `reached` markers, observe facts, crash/partition markers). They stay separate by design: keeping DST concerns out of the production trace is what lets the same tracer run in production untouched.

## See also

- [Testing](testing.md) — unit and integration testing with mocks
- [Concurrency & conflicts](concurrency-conflicts.md) — the optimistic-concurrency model DST exercises
- [Transactions](transactions.md) — why faithful rollback keeps DST findings trustworthy

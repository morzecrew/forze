---
title: Overview
icon: lucide/dice-6
summary: Point DST at a real app — one seed reproduces the whole run (schedule, faults, latency, crashes, partitions) and hands back a minimal counterexample
---

A concurrency bug reproduces once in a thousand runs, never in the debugger, and never the same way twice. The race that double-charges a customer, the retry that applies a message twice, the timeout that fires mid-transaction — they live in the *interleavings* and *failures* a test suite never schedules. You cannot fix what you cannot reproduce.

Deterministic Simulation Testing (DST) makes those runs reproducible. It takes your **real operations**, runs them concurrently on a virtual-time event loop, and explores the orderings, faults, and delays a production system would hit — then, when an invariant breaks, hands back the **smallest workload that still breaks it**, replayable forever from a single integer.

Nothing in your app changes. Handlers talk to ports exactly as they do in production; the simulation lives entirely on the test side.

!!! tip "The one promise"

    One master seed parametrises **every** source of nondeterminism — the operation
    interleaving, injected faults, simulated latency, generated inputs, crash points, and
    network partitions. `(your app, seed)` is a pure function: the same seed replays the
    exact same run, byte for byte. That is what turns a "flaky" failure into a fixed,
    reproducible one.

![One master seed derives independent sub-seeds — schedule, faults, latency, inputs — that drive a deterministic, replayable run](../_diagrams/light/dst-seed.svg#only-light){ data-src="../_diagrams/light/dst-seed.svg#only-light" }
![One master seed derives independent sub-seeds — schedule, faults, latency, inputs — that drive a deterministic, replayable run](../_diagrams/dark/dst-seed.svg#only-dark){ data-src="../_diagrams/dark/dst-seed.svg#only-dark" }

## Point it at your app

A `Simulation` needs three things: your operation **registry**, a **deps factory** (one `MockDepsModule()` auto-mocks every port, built fresh per run so each starts clean), and the **invariants** that must hold. An optional `observe` hook records the domain facts the invariants read.

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

`run` builds a meaningful workload from your operation catalog (an arrange→act scenario — `forze dst derive` prints it), runs it under perturbed interleavings, and checks the invariants. On the first violating seed it **minimises** the workload to a 1-minimal set that still fails and returns a `ViolationReport`; a clean sweep returns `None`. There is nothing to assert about *how* — point it at the app and go.

Reach for a preset instead of hand-tuning the config — `SimulationConfig.quick()` while iterating, `.thorough()` before you ship (see [Exploration strategies](dst-exploration.md#presets-dial-intensity-in-one-call)).

The app under test here is ordinary Forze code. `pay_order` charges, then flips the order to paid — but it charges *before* the optimistic-concurrency-guarded transition, so two concurrent payments both charge:

```python
--8<-- "recipes/dst_payments/app.py:handler"
```

DST finds the race, shrinks it to **two** contending payments, and reports the seed that reproduces it — no `sleep`, no thread choreography, no luck.

!!! warning "DST is only as honest as the mock"

    DST trusts that the in-memory transaction manager rolls back faithfully. The default
    (`MockDepsModule(transactions="journal")`) is atomic without serialising, so a found
    race is real. The legacy no-op manager would report *false* double-charges — see
    [Transactions](transactions.md).

## What DST gives you

The harness is one small facade — `run`, `coverage`, `coverage_guided` — over a layered engine. Each page below takes one capability from "I pointed it at my app" to "I understand and can operate it":

<div class="grid cards" markdown>

-   :lucide-shield-check: **[Invariants & reachability](dst-invariants.md)**

    What must *always* hold (the assertion toolkit) and what must *sometimes* happen (prove the dangerous interleaving actually fired).

-   :lucide-bug: **[Faults, latency & time](dst-environment.md)**

    Inject the environment a production system hits — transient errors, timeouts, heavy-tailed latency — and fast-forward virtual time to catch time-dependent bugs.

-   :lucide-power-off: **[Crashes & partitions](dst-crashes-and-clusters.md)**

    The harder failure modes: kill the process mid-flush and restart over the persisted store, or split *N* real runtimes apart with a network partition.

-   :lucide-radar: **[Exploration strategies](dst-exploration.md)**

    How `run` searches the interleaving space — the schedulers, coverage-plateau sweeps, and the feedback-directed fuzzer that hunts new behaviour.

-   :lucide-repeat: **[Find, reproduce, regress](dst-the-loop.md)**

    The day-to-day loop: read the counterexample, replay it from the command line, lock the seed into a regression corpus, and carry a bug to another machine.

</div>

## Forze passes its own simulation

DST is judged by the bugs it finds in *real* systems — so Forze runs its own distributed machinery through it. Each scenario pairs a safety invariant (must always hold) with a reachability target (must sometimes fire), so a green result means the property was tested against the hard case, not a quiet run:

| Primitive | Invariant (always) | Reachability (sometimes) |
| --- | --- | --- |
| **Distributed lock** | `mutual_exclusion` + no lost update across N runtimes | a contender spun on the held lock *while* a partition isolated a node mid-write |
| **Hybrid logical clock** | per-replica `monotonic_per` + every merge's stamp exceeds its cause | a replica merged a remote stamp ahead of its own |
| **Outbox / crash-restart** | `no_duplicate_effect` (exactly-once) survives a crash mid-flush | the crash landed between the flush and the relay |

Each scenario also keeps a *broken* twin — drop the lock, ignore the remote stamp — that the oracle catches, minimises, and reproduces, so the test proves it can still fail. That is the bar: the framework's own concurrency code is continuously simulation-tested, and app authors inherit the same harness for free.

Start with [what must hold](dst-invariants.md) — invariants are the lens through which every other capability reports a bug.

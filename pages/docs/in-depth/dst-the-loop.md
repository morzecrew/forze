---
title: Find, reproduce, regress
icon: lucide/repeat
summary: The day-to-day DST loop — read the counterexample, replay it from the command line, lock the seed into a regression corpus, and carry a bug to another machine
---

Finding a bug is the start of the loop, not the end. A `ViolationReport` is a *reproducible artifact*: it carries the seed, the minimized workload, the causal trace, and the violated invariant — everything needed to understand the failure *and* to replay it on demand. This page is the workflow that closes the loop: read it, reproduce it from the command line, lock it into a regression corpus so it stays fixed, and carry it to another machine.

![A sweep finds a violation, minimizes the workload, produces a reproducible report, saves the seed to a regression corpus, and replay re-checks it forever](../_diagrams/light/dst-loop.svg#only-light){ data-src="../_diagrams/light/dst-loop.svg#only-light" }
![A sweep finds a violation, minimizes the workload, produces a reproducible report, saves the seed to a regression corpus, and replay re-checks it forever](../_diagrams/dark/dst-loop.svg#only-dark){ data-src="../_diagrams/dark/dst-loop.svg#only-dark" }

## Read the counterexample

`report.format()` renders the whole failure: the minimised workload, the concurrency that triggered it, the causal trace (each operation and the port calls it caused — with the values they wrote and read back when `capture_values` is on), an **injected-environment timeline** (the faults, latency, and partitions the simulator applied, in virtual-time order), and the violated invariant.

For a **time-travel** view, `report.timeline()` flattens the run into a virtual-time-ordered stream of steps — operations, port calls with their value flow, injected environment, recorded facts — that you scroll through like a debugger. `render_timeline(history)` prints it, and each `TimelineEntry.to_dict()` is JSON, so the timeline is a portable artifact a CLI or viewer steps through by virtual time:

```text
DST timeline (by virtual time):
  @t=0.000000  ▸ update → ok
  @t=0.000000  ↳ document_command[accounts].update key=42 wrote {'balance': 6}
  @t=0.100000  ↳ document_command[accounts].get key=42 read {'balance': 5}   ← stale read
```

## Reproduce and regress from the command line

The `forze dst` command wires the loop end to end against an import string pointing at your `Simulation`:

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
forze dst derive   app:simulation
forze dst topology app:simulation
```

`--save-regression` appends the found seed (with the registry fingerprint and the exploration knobs) to a JSON-Lines corpus; `replay` reproduces each saved seed under the configuration it was found with, so a fixed bug stays fixed.

The registry fingerprint catches a *structural* change (a contract or plan fact moved). For a stricter guard, `entry_from_report(…, strict_behavior=True)` also records a `behavioral_fingerprint` — an ordered, PII-free digest of the run's execution-trace shape — so `RegressionEntry.behavior_drifted(history)` flags a replay whose handler *logic* drifted even when its contracts didn't. Opt-in; the default stays structural.

## Carry a bug anywhere

A found bug travels. `FailureBundle` serialises the seed *and the full config that produced it* (faults, latency, partitions, crash, scheduler) to one JSON file, and `replay_bundle` re-runs it anywhere:

```python
from forze_dst.artifacts import bundle_from_report, replay_bundle, FailureBundle

bundle_from_report(report, config, target="app:simulation").save("bug.json")
# … on another machine, another day …
report = replay_bundle(FailureBundle.load("bug.json"))   # reproduces, from one command
```

## Extending it

DST is built from small, documented seams, so you extend it without forking. Everything plugs in as a plain callable or protocol:

<div class="grid cards fz-cards" markdown>

-   :lucide-shield-check: **Invariants**

    An `Invariant` is any `Callable[[History], list[Violation]]` — write a function, pass it in `invariants=`. The [built-ins](dst-invariants.md) are just factories returning one.

-   :lucide-shuffle: **Schedulers**

    `Scheduler` is a `Protocol`; supply a `scheduler_factory` to the low-level run path to drive interleavings your own way — the engines call it per run.

-   :lucide-bug: **Environment**

    Faults and latency are declarative data (`FaultPolicy`, `LatencyProfile`); for anything custom, the `interceptors` factory adds a seeded `PortInterceptor` chain at the port seam.

-   :lucide-boxes: **Engines**

    Each strategy is a free function under `forze_dst.engines` (op_case, scenario, crash_restart, guided) taking the `Simulation` as its context — call one directly, or compose your own search over the engine substrate.

</div>

The `Simulation` class is a thin facade: `run` / `coverage` / `coverage_guided` bind the config and delegate to an engine. The trace seam is deliberately layered — the engine `RuntimeTrace` is the production tracer (id-only, PII-free), and the DST `History` is the oracle's richer view that folds it in *and* adds DST-only events (op-start anchors, `reached` markers, observe facts, crash and partition markers). They stay separate by design: keeping DST concerns out of the production trace is what lets the same tracer run in production untouched.

## See also

- [Testing](testing.md) — unit and integration testing with mocks
- [Concurrency & conflicts](concurrency-conflicts.md) — the optimistic-concurrency model DST exercises
- [Transactions](transactions.md) — why faithful rollback keeps DST findings trustworthy

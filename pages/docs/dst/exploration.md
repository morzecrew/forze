---
title: Exploration strategies
icon: lucide/radar
summary: How DST searches the interleaving space — the schedulers that order concurrent work, plateau-aware coverage sweeps, and a feedback-directed fuzzer that hunts new behaviour
---

A bug lives in a specific interleaving, fault timing, and input. The space of all three is astronomically large, so `run`'s job is not to enumerate it but to *search* it well — bias toward the rare orderings that break things, know when to stop, and spend the next run where the last one found something new. This page is the dial behind `run`: the scheduler that orders concurrent work, the sweep that knows when coverage has saturated, and the fuzzer that steers toward unexplored behaviour.

## Presets — dial intensity in one call

`SimulationConfig` has many knobs, but most of the time you want one of a few intensity tiers, not a hand-tuned config. The presets name them:

```python
from forze_dst import SimulationConfig

simulation.run(SimulationConfig.quick())       # a few seeds, default shuffle — seconds, while iterating
simulation.run(SimulationConfig.thorough())    # broad seed range under PCT — the run before you ship
simulation.run(SimulationConfig.reproduce(7))  # sweep exactly seed 7 — re-drive one timeline from a report
```

| Preset | Scales to | Reach for it |
| --- | --- | --- |
| `quick()` | ~16 seeds, default shuffle, small workloads | the inner loop — fast feedback while you write |
| `thorough()` | ~256 seeds, `PCTScheduler(depth=3)`, higher concurrency | before merging — a serious search |
| `nightly()` | thousands of seeds, deeper PCT, sized for `parallel_sweep` | an overnight CI sweep |
| `reproduce(seed)` | exactly that one seed | debugging a single counterexample |

Each preset scales the *search* — seeds, scheduler, concurrency, workload size — and leaves the *environment* (faults, latency, crashes) explicit, since the right policy is app-specific. Every field stays overridable, so a preset is a starting point, not a cage:

```python
SimulationConfig.thorough(seeds=range(1000), concurrency=16)   # thorough, but bigger
```

The rest of this page is what those presets are made of — reach for the raw knobs when a preset doesn't fit.

## Schedulers — how concurrent work is ordered

At each tick the loop has a batch of ready continuations; the **scheduler** decides their order, and that order is where order-dependent races live. The interleaving strategy is a config variant — each carries only its own parameters, so an invalid combination is unrepresentable:

```python
from forze_dst import SimulationConfig, FIFOScheduler, RandomScheduler, PCTScheduler

SimulationConfig(scheduler=RandomScheduler())            # the default
SimulationConfig(scheduler=PCTScheduler(depth=3))        # targets depth-3 bugs
SimulationConfig(scheduler=FIFOScheduler())              # one fixed order, no perturbation
```

| Variant | Strategy | Use it for |
| --- | --- | --- |
| `FIFOScheduler()` | One deterministic order, no perturbation | A reproducible baseline; confirming a bug is order-*dependent* (it vanishes under FIFO) |
| `RandomScheduler()` | Seeded shuffle each tick | The default — a broad, cheap walk over interleavings |
| `PCTScheduler(depth, steps)` | Probabilistic Concurrency Testing | Deep, specific races — provably finds a depth-`d` bug with a useful per-run probability |

`RandomScheduler()` is a uniform walk: broad, but with no bias toward the rare orderings deep bugs need. `PCTScheduler` ([Probabilistic Concurrency Testing](https://www.microsoft.com/en-us/research/publication/a-randomized-scheduler-with-probabilistic-guarantees-of-finding-bugs/ "Burckhardt et al., ASPLOS 2010")) gives each task a random priority and inserts `depth-1` priority-change points, so a depth-`d` interleaving becomes reachable with a real probability instead of by luck. Every variant is seeded — the same seed replays the same order.

## Coverage — sweep until behaviour saturates

A uniform seed sweep has a problem at the other end: how many seeds is *enough*? `coverage()` answers it by watching behavioural coverage and stopping once it plateaus — once `coverage_plateau` consecutive seeds add nothing new, the exploration has saturated:

```python
stats = simulation.coverage(SimulationConfig(seeds=range(500), coverage_plateau=8))
print(stats.format())   # behaviours covered, seeds run, whether it plateaued
report = stats.violation  # the minimized counterexample, if a seed tripped an invariant
```

The sweep right-sizes itself instead of guessing a seed count, and a bug still beats coverage — the first violating seed stops it with the same minimized report `run` produces.

## Confidence — make a green run mean something

A clean sweep is the most dangerous thing DST prints, because it looks like safety. But "no violation" only proves what was *tried* — a sweep can pass every invariant while never driving the dangerous case, and then green is not safety, it is silence. This is the same trap [reachability](invariants.md#what-must-sometimes-happen) guards against, turned on the sweep itself.

`audit()` runs the full seed range and reports what it actually exercised:

```python
stats = simulation.audit(SimulationConfig.thorough())

if stats.violation is None:
    print(stats.confidence.format())   # what a green run did — and didn't — test
```

The `ConfidenceReport` names the gaps a clean sweep still left:

- **Operations that ran but never raced** — an operation that always ran alone had its concurrency checked against nothing. Its happy path passed; its interleavings were never explored.
- **Declared faults that never fired** — a fault rule no seed ever triggered means that failure path was never exercised, so the green result says nothing about it.

```text
DST confidence
  seeds run:    256
  raced:        2/3 operations overlapped another
  faults fired: 0/1 declared rules
  ⚠ confidence gaps:
      • ran but never raced: refund — their concurrency was never tested
      • declared fault never fired: queue_command[*].* (drop) — that failure path was never exercised
```

`audit()` is `coverage()` with the plateau disabled, so every seed runs and the picture is honest — it is the CI-gate sweep that also tells you how much to trust the green. The command line prints this automatically on a clean `forze dst run` (pass `--no-confidence` to suppress it), so confidence is the default, not an extra step.

## Guided exploration — fuzz toward new behaviour

A uniform sweep knows when to *stop* but not *where to look* — every seed is independent, so behaviour gated behind a rare combination of operations is found only by chance. `coverage_guided` is **feedback-directed** instead: it keeps a corpus of inputs that each unlocked new coverage and *mutates* the productive ones (tweak an op, grow or shrink the workload, re-roll the schedule and faults), under an AFL-style power schedule that pushes the newest coverage frontier:

```python
from forze_dst import OperationCase

stats = simulation.coverage_guided(
    SimulationConfig(seeds=range(1), count=8, concurrency=4, guided_budget=512),
    cases=[OperationCase(op="deposit"), OperationCase(op="withdraw")],
)
print(stats.format())     # behaviours covered, corpus size, any violation
report = stats.violation  # minimized + reproducible, if a run tripped an invariant
```

The whole run is one seed-derived lineage — corpus, mutations, and all — rooted at the first seed and bounded by `guided_budget`, so it reproduces exactly. It reaches behaviour a uniform sweep misses at equal budget, and stops on the first violation with the same minimized report.

## Scale — parallel sweeps

Because each seed is fully deterministic **in its own process**, inter-seed parallelism is free. `parallel_sweep` fans disjoint seeds across a process pool and folds every worker's result into one picture — violating seeds, the union of behaviours covered, and a throughput metric — so a nightly fuzz explores thousands of timelines per wall-hour:

```python
from forze_dst.artifacts import parallel_sweep, SimulationSeedRunner

result = parallel_sweep(
    SimulationSeedRunner(target="app:simulation", fault_error=0.1),
    seeds=range(10_000),
    workers=16,
)
print(result.format())   # seeds/s, behaviours, first violating seed
```

The runner holds only a `module:attr` string and primitives, so it pickles across processes where a live `Simulation` could not — each worker re-imports the app and runs one seed.

Every strategy here ends the same way: on a violation, with a minimized, reproducible report. [Find, reproduce, regress](the-loop.md) is what you do with it.

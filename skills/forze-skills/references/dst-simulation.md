# Simulating your service under faults

Deterministic simulation testing runs **your real operations** concurrently on a virtual-time
event loop, exploring interleavings, faults and delays a test suite never schedules. Nothing in
your application changes: handlers talk to ports exactly as in production, and the simulation
lives entirely on the test side.

One master seed parametrises every source of nondeterminism — interleaving, injected faults,
simulated latency, generated inputs, crash points, partitions. `(your app, seed)` is a pure
function, which is what turns a flaky failure into a fixed, reproducible one.

Install with the `dst` extra; everything below is `forze_dst`.

## Point a simulation at your app

A `Simulation` needs your frozen operation registry, a **deps factory** called fresh per run so
each starts clean, and the invariants that must hold. The optional `observe` hook records the
domain facts invariants read.

```python
from forze_dst import Simulation, SimulationConfig
from forze_dst.invariants import no_unexpected_error
from forze_mock import MockDepsModule

simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(),
    invariants=[no_unexpected_error()],
)

report = simulation.run(SimulationConfig(seeds=range(64)))

if report is not None:
    print(report.format())
```

`run` builds a workload from your operation catalog, runs it under perturbed interleavings, and
checks the invariants. On the first violating seed it **minimises** the workload to a 1-minimal
set that still fails and returns a `ViolationReport`; a clean sweep returns `None`.

`deps` must be a factory, not an instance. A single shared `MockDepsModule` would carry state
from one seed into the next, and the run that "fails" would be the one that inherited it.

## Presets before hand-tuning

`SimulationConfig` has ~20 knobs. Reach for a preset instead of setting them individually:

| Preset | Use |
|---|---|
| `SimulationConfig.quick()` | while iterating on a handler |
| `SimulationConfig.thorough()` | before you ship |
| `SimulationConfig.nightly()` | the wide sweep nobody waits on |
| `SimulationConfig.reproduce(seed)` | replay one failing seed exactly |

## Choosing a scheduler

The scheduler decides which interleavings get explored, and the default is the right one.

| Scheduler | Behaviour |
|---|---|
| `PCTScheduler` | Probabilistic concurrency testing — biased toward *deep* bugs needing a specific ordering. The default, and what you want. |
| `RandomScheduler` | Uniform random ordering. A baseline for comparison, not a target. |
| `FIFOScheduler` | No perturbation. Use to confirm a failure is genuinely a concurrency bug: if it reproduces under FIFO, ordering was never the cause. |

```python
from forze_dst import PCTScheduler

config = SimulationConfig(seeds=range(256), scheduler=PCTScheduler())
```

## Injecting faults and latency

A green sweep over a fault that never fired proves nothing. Faults are declared as rules
selecting a port surface, and each rule names what goes wrong.

```python
from forze_dst import SimulationConfig
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.latency import Exponential, LatencyProfile, LatencyRule

config = SimulationConfig(
    seeds=range(256),
    faults=FaultPolicy(rules=[
        FaultRule(surface="document", error=0.05),
        FaultRule(surface="queue", duplicate=0.10),
    ]),
    latency=LatencyProfile(rules=[
        LatencyRule(surface="document", dist=Exponential(mean=0.01)),
    ]),
)
```

`FaultRule` selects with `surface` / `route` / `op` (any `None` matches anything) and every
injection knob — `error`, `timeout`, `crash`, `drop`, `duplicate`, `delay` — is a **probability per
eligible call**, not a flag. Each non-zero rate is rolled independently and the first kind that
fires wins (crash > error > timeout); all rolls draw from the policy's seeded RNG, so they replay.

For an exact placement rather than a rate, `at_call=n` fires at the *n*-th matching call —
`FaultRule(op="update", crash=1.0, at_call=2)` is "die at the second update".

Latency is virtual: a simulated 10-second delay costs no wall time, so a timeout-vs-retry race is
explorable at full sweep width.

## Reproducing a failure

A `ViolationReport` carries the `seed` that produced it. Replay it exactly:

```python
report = simulation.run(SimulationConfig.reproduce(report.seed))
```

The report also carries `workload` (the minimised operation sequence), `history` (the recorded
run) and `registry_fingerprint` — the last of which is why a seed stops reproducing when the
operation catalog changes. That is not flakiness; it is the fingerprint telling you the app
under test is no longer the same app.

## Anti-patterns

- **Passing a `MockDepsModule` instance instead of a factory** — state leaks across seeds and the failure lands on whichever run inherited it, not the one that caused it.
- **Sweeping with faults configured but no reachability target** — a green run over a fault that never fired is false confidence; declare what must *sometimes* happen (see [invariants](dst-invariants.md)).
- **Treating a changed `registry_fingerprint` as flakiness** — the seed stopped reproducing because the catalog changed; re-derive rather than chasing it.
- **Running DST against the legacy no-op transaction manager** — it reports false double-writes. The default journal manager rolls back faithfully, which is what makes a found race real.
- **Expecting DST to catch logic below a port** — database triggers, generated columns and `CHECK` constraints are not in the mock, and an invariant a trigger maintains will false-positive.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version.

- [DST overview](https://morzecrew.github.io/forze/latest/dst/overview/)
- [The simulation loop](https://morzecrew.github.io/forze/latest/dst/the-loop/)
- [Fault and latency environment](https://morzecrew.github.io/forze/latest/dst/environment/)
- [Testing with the mock](testing-with-mock.md)
- [Invariants and reachability](dst-invariants.md)

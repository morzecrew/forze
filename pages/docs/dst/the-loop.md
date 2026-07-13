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

For a **time-travel** view, `report.timeline()` flattens the run into a virtual-time-ordered stream of steps — operations, port calls with their value flow, injected environment, recorded facts — that you scroll through like a debugger. `render_timeline(history)` renders it as text, and each `TimelineEntry.to_dict()` is JSON, so the timeline is a portable artifact a CLI or viewer steps through by virtual time:

```text
DST timeline (by virtual time):
  @t=0.000000  ▸ update → ok
  @t=0.000000  ↳ document_command[accounts].update key=42 wrote {'balance': 6}
  @t=0.100000  ↳ document_command[accounts].get key=42 read {'balance': 5}   ← stale read
```

For an *interactive* scrub, `report.to_html("bug.html")` writes a self-contained viewer over that same timeline — open it in a browser and step through the run by virtual time, each step's structured detail beside it, the way a debugger walks a trace. It is one HTML file with no external assets, so it attaches to a CI artifact and a failed sweep ships its own debugger. From the command line, `forze dst run … --html bug.html` writes it on a violation.

And every `report.format()` ends with a copy-pasteable repro — the seed already filled in — so going from a CI failure to a local reproduction is a paste, not a hunt:

```text
  reproduce:
    simulation.run(SimulationConfig.reproduce(42))
    # as a test:  assert_no_violation(simulation, SimulationConfig.reproduce(42))
```

## Run it in your test suite

The loop belongs where your other tests live. `assert_no_violation` points a normal test at a `Simulation`: it sweeps, shrinks on failure, and fails the test with the minimized counterexample as the assertion message — so a DST failure reads like any other pytest failure, seed included.

```python
from forze_dst.testing import assert_no_violation

@pytest.mark.dst
def test_payments_have_no_race():
    assert_no_violation(payments_simulation)
```

No plugin is needed for that — it is a plain assertion. It defaults to a `thorough()` sweep, and a clean run passes silently like any green test.

The friction a DST test usually hits is that the right seed count differs by where it runs — a handful while you iterate, thousands in CI. Enable the optional plugin and one flag scales every sweep without touching the test:

```python
# conftest.py
pytest_plugins = ["forze_dst.testing.plugin"]
```

```bash
pytest -m dst --dst-seeds 16      # quick, while iterating
pytest -m dst --dst-seeds 2000    # exhaustive, in CI — same test, no code change
```

The plugin also registers the `dst` marker, so `-m dst` runs the simulation tests on their own (the heavy ones can be a nightly job) and `-m "not dst"` skips them in a fast inner loop. The plugin is opt-in by design — importing the DST machinery costs a moment, so it stays off until a project asks for it rather than taxing every test run.

When a sweep fails in CI, the seed that found it is the thing worth keeping. `--dst-save-bundle=DIR` drops a portable [bundle](#carry-a-bug-anywhere) — the seed and the full config that produced it — for every failing sweep:

```bash
pytest -m dst --dst-save-bundle ./dst-regressions
```

Commit those bundles, and `assert_no_regressions` turns them into a permanent guard: it replays each one at its seed under its own saved config, so the exact environment that found the bug is reproduced, not the current defaults. The day the bug comes back, the test fails again — with the original counterexample.

```python
from forze_dst.testing import assert_no_regressions

@pytest.mark.dst
def test_known_bugs_stay_fixed():
    assert_no_regressions(payments_simulation, bundles="./dst-regressions")
```

A bundle whose registry fingerprint no longer matches the simulation is flagged rather than passed silently — the catalog moved, so a clean replay can no longer be trusted to exercise the original path.

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

DST is built from small seams, so you extend it without forking — each plugs in as a
plain callable or protocol. An `Invariant` is any `Callable[[History], list[Violation]]`
(the [built-ins](invariants.md) are factories returning one); the interleaving strategy is a config
variant, and a custom order is a `Reorderer` protocol you supply via the engines'
`scheduler_factory`; faults and latency are declarative data
(`FaultPolicy`, `LatencyProfile`), with an `interceptors` factory for anything custom;
and each search strategy is a free function under `forze_dst.engines` you can call
directly or compose. The `Simulation` class is a thin facade that binds the config and
delegates to an engine.

## See also

- [Testing](../testing/overview.md) — unit and integration testing with mocks
- [Concurrency & conflicts](../writing-operation/concurrency-conflicts.md) — the optimistic-concurrency model DST exercises
- [Transactions](../writing-operation/transactions.md) — why faithful rollback keeps DST findings trustworthy

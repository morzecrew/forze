# Invariants and reading a violation

A simulation drives your app through thousands of hostile interleavings. Invariants are how you
answer the only question left: through all of that, did it stay correct?

Two shapes of claim matter and you need both. An **invariant** says a property must *always*
hold. A **reachability target** says a state must *sometimes* be reached — because a sweep that
runs ten thousand seeds and never drives the dangerous interleaving proves nothing, and a green
invariant over a fault that never bit is false confidence.

Both are passed when you build the `Simulation`; every strategy checks the same set, read off the
recorded history with no handler instrumentation.

## Invariants that need nothing from you

These read the engine trace directly, so they work against any app on day one.

| Invariant | Catches |
|---|---|
| `no_unexpected_error()` | Any operation that raised a non-domain exception — a `KeyError`, a `TypeError`. Declared `CoreException` domain failures pass. The zero-instrumentation safety net. |
| `no_duplicate_effect(kind, by=...)` | A non-idempotent consumer applying a redelivered message twice. |
| `mutual_exclusion(kind, resource=, start=, end=)` | Two holds of a resource overlapping — a lock that split-brained. |
| `operation_succeeds(*ops)` | A named op that never reached `ok`. |
| `completes_within(op, seconds)` | An op that blew its virtual-time budget. |
| `no_unclosed_transaction()` | A transaction `enter` with no matching `exit` — an abandoned scope. |
| `no_resource_leak(open_op=, close_op=)` | The general form: any open/close pair left unbalanced. |

```python
from forze_dst import Simulation
from forze_dst.invariants import no_duplicate_effect, no_unexpected_error
from forze_mock import MockDepsModule

simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(),
    invariants=[
        no_unexpected_error(),
        no_duplicate_effect("charge", by="order"),
    ],
)
```

Do not pair `no_resource_leak` with a crash policy: a crash legitimately abandons a scope, so the
invariant reports the fault you injected rather than a bug.

## Invariants over your own domain facts

`expect(kind, predicate, message=...)` reads the facts an `observe` hook recorded and asserts a
predicate over each. `observe` runs against the app's own ports, so the facts are whatever your
read model actually says.

```python
from forze_dst.invariants import expect

async def _observe(ctx: ExecutionContext) -> None:
    total = await ctx.document.query(PAYMENT_SPEC).count()
    record_event("payments", total=total)

simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(domain_events=_EVENTS),
    observe=_observe,
    invariants=[
        expect(
            "payments",
            lambda event: event.fields["total"] <= 1,
            message="an order was charged more than once",
        ),
    ],
)
```

Value-level claims need `capture_values=True` on the config: `read_your_writes(surface,
value_field=...)` is the stale-read guard, and `expect_value(surface, predicate, message=...)`
the wrong-value one — like `expect`, its `message` is required and keyword-only, because a
violation with no sentence in it is a failing assertion nobody can act on.

## Reachability — what must sometimes happen

Declare targets on the config. A sweep that never reaches one is reported as such, which is what
stops a green run over an unexercised fault from reading as proof.

```python
config = SimulationConfig(seeds=range(256), reachability_targets=["charge_failed"])
```

## Reading a ViolationReport

On the first violating seed the harness **minimises** the workload to a 1-minimal set that still
fails, then returns a report. A clean sweep returns `None`.

```python
report = simulation.run(SimulationConfig(seeds=range(256)))

if report is not None:
    print(report.format())
```

| Field | What it tells you |
|---|---|
| `seed` | Feed to `SimulationConfig.reproduce(seed)` for a byte-identical replay. |
| `violations` | Which invariants broke, each with the witnessing trace entry. |
| `workload` | The minimised operation sequence — usually two or three operations, and the actual bug report. |
| `history` | The full recorded run behind the violation. |
| `registry_fingerprint` | The catalog the run was made against. A changed fingerprint is why an old seed stops reproducing. |

Read `workload` first. Minimisation is what turns "something raced under 256 seeds" into "these
two operations, in this order", and that is the sentence a fix gets written against.

## Anti-patterns

- **Shipping only `no_unexpected_error()`** — it catches crashes, not wrong answers. A double-charge that raises nothing passes it.
- **Declaring invariants without reachability targets** — nothing then distinguishes "held under pressure" from "the pressure never arrived".
- **Pairing `no_resource_leak` with a crash policy** — a crash abandons scopes by definition, so the invariant reports your own fault injection.
- **Asserting on `history` in a test** — it is a recorded run, not a contract; assert on invariants and let minimisation produce the counterexample.
- **Using `expect` over a fact a trigger maintains** — the mock writes rows but fires no triggers, so the invariant false-positives against real behaviour.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version.

- [Invariants and reachability](https://morzecrew.github.io/forze/latest/dst/invariants/)
- [Crashes and clusters](https://morzecrew.github.io/forze/latest/dst/crashes-and-clusters/)
- [Simulating your service](dst-simulation.md)

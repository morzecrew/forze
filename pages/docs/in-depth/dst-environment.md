---
title: Faults, latency & time
icon: lucide/bug
summary: Declare the environment a production system runs in — transient errors, timeouts, heavy-tailed latency, virtual time — and DST applies it at the port boundary, seeded, app untouched
---

A handler that passes every happy-path test still meets a hostile world in production: downstreams error, calls time out, the slow tail of a latency distribution lands mid-transaction. DST lets you declare that environment as data on the config, and applies it at the **port boundary** — over any resolved port, seeded from the master seed, with the app untouched. The handler makes its ordinary `await` port calls; the simulator decides what happens at each one.

## Faults

A `FaultPolicy` is a set of rules, each matching `(surface, route, op)` and rolling — per matched call — one of the failure behaviours. Declare it on the config and every matching port call becomes a place a fault can strike:

```python
from forze_dst import SimulationConfig
from forze_dst.faults import FaultPolicy, FaultRule

report = simulation.run(SimulationConfig(
    seeds=range(200),
    faults=FaultPolicy(rules=(
        FaultRule(surface="document_command", error=0.2),   # 20% transient failures
    )),
))
```

A `FaultRule` rolls, per matched call: `error` (a retryable failure), `timeout`, and `crash`, plus the transport behaviours `drop` (silent loss), `duplicate` (redelivery), and `delay` (advance virtual time before the call). Faults are **seeded by construction** — you never pass an RNG, so a 20%-error sweep reproduces exactly. This is how you prove a retry loop actually converges, or that a consumer stays idempotent when the broker redelivers.

## Latency

A `LatencyProfile` samples a per-route distribution and advances the virtual clock by the result before each matched call — modelling the time a real downstream takes:

```python
from forze_dst.latency import LatencyProfile, LatencyRule, Exponential

latency = LatencyProfile(rules=(
    LatencyRule(dist=Exponential(mean=0.05), surface="document_command"),
))
```

The distributions are `Constant`, `Uniform`, `Exponential`, and the **heavy-tailed** `LogNormal` and `Pareto`. The heavy tails matter: their long right edge produces the p99 blow-ups that expose timeout and deadline bugs a fixed delay never reaches — the one call in five hundred that takes ten times the mean is exactly the one that outlives a TTL.

## Virtual time

Latency and `delay` advance a *virtual* clock, not a real one. A workload spanning minutes of simulated time runs in real-wall milliseconds, and time-dependent bugs surface with no `sleep` anywhere in your handlers.

The reservation example is a pure-time bug — no concurrency at all. `confirm` checks the hold is still valid, *then* charges through a slow payment downstream; the charge takes longer than the reservation's TTL, so the hold expires mid-flight yet the confirmation is still written:

```python
--8<-- "recipes/dst_reservation_ttl/app.py:handler"
```

The handler has no idea time is passing unusually. The simulation simply says the payment route is slow, and DST fast-forwards its clock through the charge:

```python
--8<-- "recipes/dst_reservation_ttl/app.py:simulation"
```

DST drives `confirm`, advances ten virtual minutes through the charge against a five-minute hold, and the `observe` invariant catches the confirmation written after expiry — a classic check-then-act-across-time mistake, found without waiting and without flakiness.

!!! note "Both are reproducible by construction"

    Fault rolls and latency samples both derive from the master seed — there is no RNG to pass
    in. The same seed replays the same errors at the same calls and the same delays at the same
    boundaries, so a found bug is a fixed input, not a probability.

Faults and latency perturb a single running process. The next step removes the process entirely — [crashes and partitions](dst-crashes-and-clusters.md), where the runtime dies or the network splits.

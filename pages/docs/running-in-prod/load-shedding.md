---
title: Load shedding under overload
icon: lucide/gauge
summary: Adaptive concurrency that learns its own limit, criticality-ordered shedding that protects the important requests, and the control plane to steer it live — provably, under simulation
---

When a dependency slows down, the dangerous failure is not the slow call — it is the pile-up behind
it. Requests queue, memory grows, latency climbs for *everyone*, and a degraded dependency becomes a
full outage. The answer is to admit only as much concurrent work as the dependency can actually absorb
right now, and when that ceiling is reached, to shed the **least important** work rather than fail
indiscriminately. This page is about how Forze finds that ceiling on its own and sheds the right
requests — and how that guarantee is checked under deterministic simulation, not just asserted.

## The limit you can't guess

A fixed concurrency cap is a guess, and a stale one: the number that protected the dependency at
2 a.m. over-admits during a latency spike and under-admits when it recovers. An **adaptive bulkhead**
learns the ceiling from the dependency's own latency instead.

Two controllers, both driven by completion latency:

- **`AdaptiveBulkheadStrategy`** — an AIMD controller (additive-increase, multiplicative-decrease):
  the limit creeps up while completions stay under `latency_threshold` and backs off multiplicatively
  when they breach it. In quantile mode (`latency_quantile`), the breach signal is a windowed
  percentile of recent latencies rather than one sample, so a single slow call doesn't collapse the
  limit.
- **`GradientBulkheadStrategy`** — a delay-based controller (Gradient2) with *no threshold to tune*:
  it learns the no-load baseline latency as a long-window average and contracts as the ratio of
  current-to-baseline latency inflates — catching the knee of the load curve before errors appear.
  "Slow up, fast down": increases ramp gently, contractions apply immediately.

Both keep state per `(policy, route)`, so distinct dependencies under one policy find their own limits
independently. With a shared latency digest store, the congestion signal reflects the *fleet's*
latency instead of one replica's.

## Shedding the right requests

Once the bulkhead is saturated, something has to give. A plain queue sheds by arrival order or length —
which drops whoever happens to be last, critical or not. Forze sheds by **criticality** instead.

Every request carries an ambient criticality tier — `BEST_EFFORT`, `DEGRADED`, `NORMAL`, or
`CRITICAL` — set with `bind_criticality`. Turn a bulkhead prioritized (`prioritized=True`, which
requires a `max_queue`) and the wait queue becomes a priority queue: when it is full and a
higher-criticality request arrives, it **displaces the lowest-criticality waiter** — that waiter is
shed with `bulkhead_queue_shed`, and the important request takes its place. An equal- or
lower-criticality arrival against a full queue is simply rejected (`bulkhead_reject`); it never evicts
work that matters as much or more.

Under sustained congestion the queue can additionally shed by *time* (a CoDel sojourn bound), and in
prioritized mode each tier gets a scaled grace period — a best-effort waiter is shed sooner, a critical
one is granted more slack. The net effect is the same: the load you drop is the load you can most
afford to lose.

!!! note "Criticality is a property of the request, not the policy"

    One policy serves every tier. The tier rides the call context, so the same wired dependency
    protects a checkout the operator marked `CRITICAL` while shedding a `BEST_EFFORT` prefetch behind
    it — no per-tier wiring, no duplicate policies.

## Proven under simulation

"Sheds the right requests" is a claim about *every* interleaving of a concurrent burst, not the one
your test happened to schedule. Forze pins it under [deterministic
simulation](../dst/overview.md): a saturating burst of mixed-criticality calls runs against a small
prioritized bulkhead on the simulation loop, which replays a different, fully reproducible scheduling
of those concurrent arrivals for each seed.

Across every explored interleaving, the invariant holds: under genuine overload, **no `CRITICAL`
request is ever shed** — nothing outranks it to displace it, and it keeps its sojourn grace — while
best-effort work *is* shed, so the shedding is provably criticality-ordered rather than incidentally
so. Because the run is deterministic, a violation would minimize to an exact, replayable
counterexample rather than a flaky failure you can't reproduce.

## Operating it live

Overload doesn't wait for a redeploy, so the resilience executor exposes a control plane —
`ctx.resilience.admin()` — for an operator surface to steer it at runtime:

| Call | What it does |
|------|--------------|
| `inspect()` | Snapshot the live state per `(policy, route)`: the current adaptive concurrency limit, in-flight and queued counts, the effective hedge delay, and any force-open flag. Filter to one `policy`. |
| `force_open(policy, route)` | A manual kill-switch — every call under the key is rejected (`resilience_forced_open`) until cleared, even for a policy with no circuit breaker. Shed a known-bad dependency without shipping code. |
| `clear_forced_open(policy, route)` | Release the kill-switch. |
| `retune(policy)` | Hot-swap a policy's parameters by name; the adaptive controllers rebuild against the new limits on the next call, while calls already in flight drain safely on the state they captured. |

`inspect()` returns a `ResilienceStateSnapshot` per key — the same numbers the resilience metrics
export, so what an operator reads by hand matches the dashboards.

!!! warning "`force_open` is an operator action, not configuration"

    It rejects live traffic immediately and stays in effect until `clear_forced_open`. Reach for it
    to shed a failing dependency in an incident; express steady-state policy through the strategy
    parameters instead.

Adaptive concurrency and criticality shedding are one part of the broader [resilience
policy](resilience.md) stack — compose them with retries, timeouts, and circuit breakers, and reach
for the [adaptive throttle](resilience.md#shedding-for-a-degraded-downstream) when a dependency is
degraded-but-alive rather than saturated. Every parameter and its default is in [resilience
tuning](../reference/resilience-tuning.md#bulkheads).

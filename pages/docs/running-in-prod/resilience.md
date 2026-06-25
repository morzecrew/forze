---
title: Resilience
icon: lucide/shield
summary: Rate limits, retries, timeouts, and circuit breakers — composed into named policies
---

Calls to other systems fail transiently. A **resilience policy** wraps an
operation (or a single call) with rate limits, retries, timeouts, and circuit
breakers — declaratively, by name — so fault tolerance lives in the wiring, not
scattered through handlers.

![A resilience policy is an ordered stack — rate limit, bulkhead, circuit breaker, retry, timeout — wrapping the dependency call, with the invocation deadline gating the whole chain](../_diagrams/light/resilience-stack.svg#only-light){ data-src="../_diagrams/light/resilience-stack.svg#only-light" }
![A resilience policy is an ordered stack — rate limit, bulkhead, circuit breaker, retry, timeout — wrapping the dependency call, with the invocation deadline gating the whole chain](../_diagrams/dark/resilience-stack.svg#only-dark){ data-src="../_diagrams/dark/resilience-stack.svg#only-dark" }

## Built-in policies

Two ship ready to use, no `ResilienceSpec` required:

- **`occ`** — retry on `concurrency` (optimistic-concurrency contention).
- **`transient`** — retry on `infrastructure`, with a 30 s per-attempt timeout.

## Applying a policy

Declaratively, wrap an operation on its registry with a named policy:

```python
ResilienceWrap(policy="transient").to_step()
```

Or imperatively, around a specific call:

```python
result = await ctx.resilience().run(
    lambda: charge_card(payment),
    policy="transient",
    route="payments",   # keys breaker/bulkhead state per dependency
)
```

!!! warning "A retry re-runs the whole operation"

    Each attempt opens a **fresh transaction**, so a failed attempt's writes roll
    back before the next one — retries never leave half-applied state. It also
    means retried work must be safe to repeat.

## A policy is composed strategies

When the built-ins aren't enough, compose your own. A `ResiliencePolicy` is an
ordered stack of strategy objects (outer → inner: rate limit → bulkhead →
circuit breaker → retry → timeout), plus optional fallback and hedge:

| Strategy | What it does |
|----------|--------------|
| **Rate limit** | token bucket — sustained `permits/per`, capacity `burst or permits`; an empty bucket **rejects immediately** with `throttled` |
| **Retry** | re-run on a [retryable](../writing-operation/errors.md) failure — `max_attempts`, `backoff` (base, max, multiplier, jitter) |
| **Timeout** | a per-attempt timeout |
| **Circuit breaker** | stop calling a failing dependency once a failure ratio trips, for a cool-off window |
| **Adaptive throttle** | [shed proportionally](#shedding-for-a-degraded-downstream) when the downstream stops accepting — the breaker's sibling for degraded-but-alive dependencies |
| **Bulkhead** | cap concurrent calls — fixed, or [adaptive / delay-based](#bulkheads) — with an optional managed queue |
| **Fallback / Hedge** | a fallback value on failure; or race staggered attempts |

Retry only fires on kinds that declare themselves **retryable** —
`concurrency`, `infrastructure`, and `throttled` (see
[Errors & failures](../writing-operation/errors.md)). You can't retry a `validation` or `domain`
failure, by design.

When the invocation carries a [deadline](deadlines.md), it gates the whole
strategy chain from the outside — and a retry abandons a backoff sleep that
would outlive the budget, surfacing the real error instead of a pointless
wait.

## Rate limiting

`RateLimitStrategy(permits=10, per=timedelta(seconds=1))` is a token bucket:
`permits/per` is the sustained rate, `burst or permits` the capacity, refilled
continuously from the monotonic clock. State is keyed `(policy, route)` like
the breaker's, so distinct backends under one policy get distinct buckets.

There is **no queuing**: a call that finds the bucket empty raises
`exc.throttled(code="rate_limited")` right away, before it can occupy a
bulkhead slot or count against the breaker.

To **wait** instead of failing fast, lean on the taxonomy: `throttled` is
retryable, so a retry-with-backoff policy *around* the rate-limited call turns
rejection into waiting:

```python
patient = ResiliencePolicy(
    name="patient",
    strategies=(
        RetryStrategy(
            max_attempts=4,
            backoff=BackoffStrategy(
                base=timedelta(milliseconds=100),
                max=timedelta(seconds=2),
            ),
            retry_on=frozenset({ExceptionKind.THROTTLED}),
        ),
    ),
)

# vendor calls run under a rate-limit policy; the call site waits it out.
result = await ctx.resilience().run(
    lambda: vendor.fetch(order_id),
    policy="patient",
)
```

The same `retry_on` also waits out backend-raised throttles (an upstream 429
mapped to `throttled`) — your limiter and the backend reject the same way. At
the FastAPI edge, an uncaught `throttled` becomes a **429**.

## Bulkheads

A bulkhead caps how many calls may be in flight at once, so one slow
dependency can't absorb every task in the process. The fixed form is one
number — `BulkheadStrategy(max_concurrency=8, max_queue=4)`: eight in flight,
four waiting, the rest rejected immediately.

A fixed cap has to be guessed, and the right number moves with downstream
health. **`AdaptiveBulkheadStrategy`** lets latency set it instead:

```python
AdaptiveBulkheadStrategy(
    latency_threshold=timedelta(milliseconds=300),
    max_concurrency=16,
)
```

It starts at `max_concurrency` and backs the limit off when a completion exceeds
the threshold, recovering it as calls return in budget — AIMD, the TCP-congestion
algorithm, so uncoordinated replicas sharing one downstream converge with no shared
state. A **`GradientBulkheadStrategy`** goes one further and needs no threshold at
all: it learns the no-load latency baseline and tracks the gradient between it and
recent latency, finding the load/latency knee on its own.

Both feed *only* on latency — errors are the circuit breaker's signal, and a
fast-failing downstream must not crater concurrency when failures are cheap — and
shrinking only gates admission, never evicting in-flight work. The three kinds are
mutually exclusive within a policy.

A queued bulkhead can additionally bound waiting by *time* (CoDel) and shed by
criticality rather than length. Every parameter — the AIMD and Gradient2
mechanics, the distributional `latency_quantile` signal, and the CoDel /
adaptive-LIFO / prioritized queue controls — is in
[resilience tuning](../reference/resilience-tuning.md#bulkheads).

## Hedging the tail

Even a healthy downstream has a slow tail. A **hedge** races it: if the
primary attempt hasn't completed after `delay`, fire a concurrent copy and
take whichever finishes first (losers are cancelled). Only safe on idempotent
reads — and `budget` caps the extra load it may add.

A fixed delay is always either too eager (wasted duplicate load) or too late (no
tail rescue) as the downstream's distribution moves. Set `adaptive_delay_quantile`
and the hedge fires after the *observed* p95 of recent primary latencies instead,
tracking the downstream as it shifts. The parameters and the streaming estimator
are in [resilience tuning](../reference/resilience-tuning.md#hedge); the effective
delay shows up as the `forze.resilience.hedge.delay` gauge once
[`instrument_resilience`](observability.md#resilience-metrics) is attached.

## Shedding for a degraded downstream

The circuit breaker is **binary**: full traffic, or a half-open trickle. At
50% downstream failure both answers are wrong — the right one is to send roughly
the traffic the downstream is still absorbing. That's the **adaptive client
throttle** (`AdaptiveThrottleStrategy`, Google's SRE book): it sheds
*proportionally* to the observed accept ratio, rising as the downstream degrades
and decaying to zero on its own as it recovers — a continuous probe with no
half-open ceremony. Shed calls fail with a retryable `throttled`
(`code="adaptive_throttle"`, 429 at the edge).

The throttle and the breaker are **mutually exclusive in one policy** (composed,
the throttle would read the breaker's own local rejections as overload). Pick per
dependency: the throttle for downstreams that degrade, the breaker for ones that
die outright. The shedding formula and its self-limiting steady state are in
[resilience tuning](../reference/resilience-tuning.md#adaptive-throttle).

## Port-level policies

Instead of wrapping individual calls, bind a policy to a **dependency key** —
every resolved port for that key gets its public coroutine methods run under
the policy, transparently:

```python
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.queue import QueueCommandDepKey
from forze.application.contracts.resilience import PortPolicy

ResilienceDepsModule(
    spec=my_policies,  # defines "vendor_rl"
    port_policies=(
        # every HTTP service method runs under "vendor_rl"
        PortPolicy(key=HttpServiceDepKey, policy="vendor_rl"),
        # only enqueues, under one shared bucket across queues
        PortPolicy(
            key=QueueCommandDepKey,
            policy="vendor_rl",
            route="all-queues",
            methods=("enqueue", "enqueue_many"),
        ),
    ),
)
```

`route` defaults to the route the port resolved under (its `spec.name`), so
each backend keys its own breaker/bucket state; set it explicitly to share.
`methods=None` wraps every public coroutine method. Async-generator methods
(`consume`, `tail`, `subscribe`-style streams) are never wrapped — a stream
can't run inside a single `run()` call; guard the consumption loop instead.

## Fleet-wide state

Breaker and rate-limit state is per-process by default — which means N
replicas each enforce `permits/per` independently (a fleet-effective rate of
`permits × replicas`), and a circuit opened on one replica protects only that
replica. `forze[redis]` makes both **shared**:

```python
from forze_redis import (
    redis_circuit_breaker_store,
    redis_latency_digest_store,
    redis_rate_limit_store,
)

ResilienceDepsModule(
    spec=my_policies,
    breaker_store=redis_circuit_breaker_store(redis),
    rate_limit_store=redis_rate_limit_store(redis),
    latency_digest_store=redis_latency_digest_store(redis),  # see below
)
```

The shared rate limiter keeps its token bucket in Redis, mutated atomically on
the server's clock — the declared rate becomes the *fleet's* rate. The shared
breaker store does the same for circuit state, so one replica tripping opens
the circuit for all. Both **fail open**: on a Redis error they fall back to
the process-local implementation (emitting a `*_store_degraded` trace event),
so a coordination-store hiccup degrades to per-replica behavior instead of
failing calls.

Bulkhead *capacity* stays process-local by design — fleet capacity is
`max_concurrency × replicas`, and the [adaptive bulkhead](#bulkheads) converges
across uncoordinated replicas like N TCP flows. Its congestion *signal* can be
shared, though: for a `latency_quantile` policy, `latency_digest_store` keeps
the latency sketch in Redis (a mergeable DDSketch), so every replica's adaptive
limit reacts to the *fleet's* p95 instead of its own — same fail-open posture
as the other two stores. The rest of the fleet story — drain, readiness,
singleton startup steps — is in [Shutdown & fleets](shutdown-and-fleets.md).

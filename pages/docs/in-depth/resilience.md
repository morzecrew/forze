---
title: Resilience
icon: lucide/shield
summary: Rate limits, retries, timeouts, and circuit breakers — composed into named policies
---

Calls to other systems fail transiently. A **resilience policy** wraps an
operation (or a single call) with rate limits, retries, timeouts, and circuit
breakers — declaratively, by name — so fault tolerance lives in the wiring, not
scattered through handlers.

## A policy is composed strategies

A `ResiliencePolicy` is an ordered stack of strategy objects (outer → inner:
rate limit → bulkhead → circuit breaker → retry → timeout), plus optional
fallback and hedge:

| Strategy | What it does |
|----------|--------------|
| **Rate limit** | token bucket — sustained `permits/per`, capacity `burst or permits`; an empty bucket **rejects immediately** with `throttled` |
| **Retry** | re-run on a [retryable](errors.md) failure — `max_attempts`, `backoff` (base, max, multiplier, jitter) |
| **Timeout** | a per-attempt deadline |
| **Circuit breaker** | stop calling a failing dependency once a failure ratio trips, for a cool-off window |
| **Bulkhead** | cap concurrent calls (and an optional queue) |
| **Fallback / Hedge** | a fallback value on failure; or race staggered attempts |

Retry only fires on kinds that declare themselves **retryable** —
`concurrency`, `infrastructure`, and `throttled` (see
[Errors & failures](errors.md)). You can't retry a `validation` or `domain`
failure, by design.

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

## Shared breakers across replicas

By default circuit-breaker state is per-process. `forze[redis]` provides a
**distributed** breaker store so an open circuit on one replica protects them
all.

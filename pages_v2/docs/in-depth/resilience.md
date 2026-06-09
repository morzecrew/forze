---
title: Resilience
icon: lucide/shield
summary: Retries, timeouts, and circuit breakers — composed into named policies
---

Calls to other systems fail transiently. A **resilience policy** wraps an
operation (or a single call) with retries, timeouts, and circuit breakers —
declaratively, by name — so fault tolerance lives in the wiring, not scattered
through handlers.

## A policy is composed strategies

A `ResiliencePolicy` is an ordered stack of strategy objects (outer → inner:
bulkhead → circuit breaker → retry → timeout), plus optional fallback and hedge:

| Strategy | What it does |
|----------|--------------|
| **Retry** | re-run on a [retryable](errors.md) failure — `max_attempts`, `backoff` (base, max, multiplier, jitter) |
| **Timeout** | a per-attempt deadline |
| **Circuit breaker** | stop calling a failing dependency once a failure ratio trips, for a cool-off window |
| **Bulkhead** | cap concurrent calls (and an optional queue) |
| **Fallback / Hedge** | a fallback value on failure; or race staggered attempts |

Retry only fires on kinds that declare themselves **retryable** —
`concurrency` and `infrastructure` (see [Errors & failures](errors.md)). You
can't retry a `validation` or `domain` failure, by design.

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

## Shared breakers across replicas

By default circuit-breaker state is per-process. `forze[redis]` provides a
**distributed** breaker store so an open circuit on one replica protects them
all.

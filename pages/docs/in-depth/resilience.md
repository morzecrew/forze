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
| **Timeout** | a per-attempt timeout |
| **Circuit breaker** | stop calling a failing dependency once a failure ratio trips, for a cool-off window |
| **Bulkhead** | cap concurrent calls, fixed or [adaptive](#bulkheads), with an optional managed queue |
| **Fallback / Hedge** | a fallback value on failure; or race staggered attempts |

Retry only fires on kinds that declare themselves **retryable** —
`concurrency`, `infrastructure`, and `throttled` (see
[Errors & failures](errors.md)). You can't retry a `validation` or `domain`
failure, by design.

When the invocation carries a [deadline](deadlines.md), it gates the whole
strategy chain from the outside — and a retry abandons a backoff sleep that
would outlive the budget, surfacing the real error instead of a pointless
wait.

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

It starts at `max_concurrency` and behaves exactly like a fixed bulkhead while
calls complete inside the threshold. A completion *over* the threshold backs
the limit off multiplicatively (`backoff_ratio`, at most once per `cooldown` —
a burst of slow completions backs off once, not to the floor); in-budget
completions recover it additively, about one slot per `limit` successes. This
is AIMD — the TCP congestion algorithm — and that's exactly why it suits a
fleet: N replicas' process-local limits sharing one downstream converge like N
TCP flows sharing a link, with no distributed state.

Two things it deliberately does *not* do. Errors don't shrink the limit —
fast failures are the circuit breaker's signal, and a fast-failing downstream
must not crater concurrency exactly when failures are cheap. And shrinking
never evicts in-flight work — the limit only gates admission.

### Managing the queue

When a bulkhead has a queue (`max_queue >= 1`), a size bound alone isn't
enough under sustained overload — a short queue that never empties still adds
its full length of latency to every call. Both bulkhead kinds take two opt-in
controls for that, straight from Facebook's *Fail at Scale*:

- **`queue_target=`** (CoDel) — bound queueing by the *time* a waiter
  experiences. While the queue has recently been empty, a waiter may sojourn
  up to `queue_interval` (default 100 ms); under sustained congestion,
  anything parked longer than the target is shed at dequeue
  (`code="bulkhead_queue_shed"`).
- **`queue_adaptive_lifo=True`** — while congested, serve the *newest* waiter
  first: its client is the one most likely still listening. FIFO otherwise.
  LIFO starves the old tail under overload by design, so pair it with
  `queue_target` to shed that tail instead of parking it forever.

A parked waiter whose [invocation deadline](deadlines.md) has already expired
is failed at wake instead of being granted a slot it can no longer use — no
knob needed.

## Hedging the tail

Even a healthy downstream has a slow tail. A **hedge** races it: if the
primary attempt hasn't completed after `delay`, fire a concurrent copy and
take whichever finishes first (losers are cancelled). Only safe on idempotent
reads — and `budget` caps the extra load it may add.

The textbook delay is "about the p95 latency" — but a fixed number is always
either too eager (wasted duplicate load) or too late (no tail rescue) as the
downstream's distribution moves. Let it track the observed tail instead:

```python
HedgeStrategy(
    delay=timedelta(milliseconds=200),   # fallback until the estimator warms
    max_attempts=2,
    adaptive_delay_quantile=0.95,        # hedge after the *observed* p95
    delay_min=timedelta(milliseconds=10),
)
```

The executor keeps a streaming quantile estimate (P² — five floats, no sample
storage) of primary-attempt latencies per `(policy, route)`, windowed so a
shifted distribution is picked up quickly, and hedges after *that* instead of
the fixed delay. `delay_min` / `delay_max` clamp it: the floor guards against
over-eager hedging when every call is fast, the cap against a degraded
downstream pushing the trigger past usefulness. The effective delay is
visible as the `forze.resilience.hedge.delay` gauge once
[`instrument_resilience`](observability.md#resilience-metrics) is attached.

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
from forze_redis import redis_circuit_breaker_store, redis_rate_limit_store

ResilienceDepsModule(
    spec=my_policies,
    breaker_store=redis_circuit_breaker_store(redis),
    rate_limit_store=redis_rate_limit_store(redis),
)
```

The shared rate limiter keeps its token bucket in Redis, mutated atomically on
the server's clock — the declared rate becomes the *fleet's* rate. The shared
breaker store does the same for circuit state, so one replica tripping opens
the circuit for all. Both **fail open**: on a Redis error they fall back to
the process-local implementation (emitting a `*_store_degraded` trace event),
so a coordination-store hiccup degrades to per-replica behavior instead of
failing calls.

Bulkheads deliberately stay process-local — fleet capacity is
`max_concurrency × replicas` by design, and the [adaptive
bulkhead](#bulkheads) converges across uncoordinated replicas without shared
state. The rest of the fleet story — drain, readiness, singleton startup
steps — is in [Shutdown & fleets](shutdown-and-fleets.md).

---
title: Shutdown & fleets
icon: lucide/server
summary: Graceful drain, readiness probes, and what changes when you run N replicas
---

Processes get told to stop — deploys, autoscaling, a node going away. And in
production there is rarely one process: the same app runs as N replicas behind
a load balancer. The runtime treats both as first-class: shutdown **drains**
instead of dropping work, and a declared **fleet posture** catches the
mistakes that only N replicas can make.

## Graceful drain

`ExecutionRuntime.shutdown()` — and therefore `scope()` exit and
`runtime_lifespan` — does not tear infrastructure down under in-flight work.
It drains first:

1. The scope stops admitting new top-level invocations. They fail with a
   **retryable** `throttled` (`code="draining"`) — a **429** at the FastAPI
   edge, a requeue-worthy nack for a queue consumer.
2. In-flight operations get a bounded window — `drain_timeout`, default 10
   seconds — to finish before lifecycle teardown closes the clients they
   depend on.
3. Lifecycle shutdown runs as usual, in reverse wave order.

```python
runtime = build_runtime(..., drain_timeout=timedelta(seconds=20))
```

An operation that is already running keeps all of its machinery: nested
dispatch deliberately rides the outer invocation's slot, so draining never
starves an admitted operation of its own dispatch chains. Zero in-flight work
exits immediately; a window that expires logs the leftover count and proceeds —
shutdown is never blocked indefinitely.

## Readiness

The load balancer should stop routing *before* the drain window starts. The
runtime exposes its state — `runtime.ready` and `runtime.draining` — and the
FastAPI integration turns it into a probe:

```python
from forze_fastapi.routes import attach_readiness_route

attach_readiness_route(router, runtime)   # GET /readyz
```

`200` while a scope is active and not draining; `503 draining` once shutdown
flips the gate, `503 unavailable` before the scope exists. Point your
orchestrator's readiness check here and the rollout sequence takes care of
itself: routing stops, in-flight work drains, teardown runs.

## Declare the fleet posture

Some startup work is safe in one process and a stampede in twenty — N replicas
all running `CREATE INDEX` or a data migration at the same moment. Declaring
the posture makes that a composition-time error instead of a 3 a.m. incident:

```python
from forze.application.execution import DeploymentProfile

runtime = build_runtime(..., deployment=DeploymentProfile.FLEET)
```

Under `FLEET`, building the runtime fails for any lifecycle step marked
`mutates_shared_state=True` that is not also `singleton_guarded`. The markers
are declared by the step author — mutation can't be detected structurally, so
the validation is honest-by-declaration: mark the steps that touch shared
backends, and the profile enforces that each one is guarded.

## Singleton lifecycle steps

The guard itself ships in `forze_kits`: wrap a step in a distributed lock so
one replica runs it and the rest skip —

```python
from forze.application.contracts.dlock import DistributedLockSpec
from forze_kits.lifecycle import singleton_lifecycle_step

step = singleton_lifecycle_step(
    ensure_indexes_step,
    spec=DistributedLockSpec(name="ensure-indexes"),  # resolved from the scope
    owner=instance_id,
)
```

You pass the lock *spec*, not a live port: the guard resolves the command port
from the execution context (`ctx.dlock.command(spec)`) at startup, so it slots
into a lifecycle plan that's assembled before any scope exists.

The first replica to acquire the lock runs the startup and releases it;
replicas that find the lock held **skip** — the holder is doing the work.
Shutdown later runs only on the replica whose startup actually executed. The
step must be *idempotent* ("ensure"-style): a replica that starts after the
holder released will acquire and run it again. Size the lock's TTL to
comfortably exceed the step's duration — no heartbeat extends it here.

!!! warning "Migrations are deploy steps"

    `singleton_lifecycle_step` is for ensure-style work: indexes, queue
    declarations, seed data. One-shot work like a schema migration wants
    run-exactly-once semantics, which a skip-if-held lock does not give —
    run it as a deploy step in your pipeline, not as a runtime step.

## What's shared, what's per-process

Most coordination state already lives in your backends (idempotency records,
distributed locks, the outbox). The resilience layer's state is process-local
by default, and each piece has a deliberate fleet answer:

| State | Default | In a fleet |
|-------|---------|------------|
| Circuit breaker | per-process | share via `redis_circuit_breaker_store` — one replica's open circuit protects them all |
| Rate limits | per-process (fleet rate = `permits × replicas`) | share via `redis_rate_limit_store` — the declared rate becomes the fleet's rate |
| Bulkheads | per-process | stays local **by design** — fleet capacity is `max_concurrency × replicas`, and [adaptive bulkheads](resilience.md#bulkheads) converge across uncoordinated replicas |

Wiring for the shared stores is on the [resilience
page](resilience.md#fleet-wide-state). The framework's periodic loops — the
outbox relay tick, consumer crash-restart backoff — are jittered out of the
box, so N replicas don't synchronize into a thundering herd against the same
claim query.

Knowing a fleet drains cleanly — or spots one thundering against a claim query —
is something you confirm by watching it; that's [Observability](observability.md).

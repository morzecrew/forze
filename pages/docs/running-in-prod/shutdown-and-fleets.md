---
title: Shutdown & fleets
icon: lucide/server
summary: Graceful drain and deploys, readiness probes, and what changes when you run N replicas
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

`200` once lifecycle startup has finished, until the scope starts draining.
`503 unavailable` before that — before the scope exists, and while later startup
steps are still running, so a pod is never routed to with half its clients open.
`503 draining` from the moment the scope starts draining: when a drain is
announced (see [Graceful deploys](#graceful-deploys)), or when shutdown closes the
gate. Point your orchestrator's readiness check here and the rollout sequence
takes care of itself: routing stops, in-flight work drains, teardown runs.

Point *liveness* somewhere else — at `attach_liveness_route(router)`, which answers
`200` unconditionally. A draining pod is alive but not ready; one endpoint answering
both questions gets the pod killed mid-drain instead of letting it finish.

Processes that are not HTTP servers — the outbox relay, inbox and commit-stream
consumers, the socket.io gateway, durable runners — get both probes from one
lifecycle step:

```python
from forze.application.execution.background import probe_listener_step

lifecycle_steps.append(probe_listener_step(port=8079))
```

Stdlib `asyncio`, no HTTP framework. The step reads the scope's state from the
`ctx` its startup hook receives, so it takes no runtime and sits in the plan of the
runtime it reports on. It answers through the same evaluator as the FastAPI route,
reports `draining` for the whole window, and closes only after. See
[The Grafana stack](grafana-stack.md#probes) for the Kubernetes mapping.

### Are the dependencies reachable?

The drain gate answers whether the process is *willing* to take traffic. It says
nothing about whether it can serve one: an un-drained process whose database is gone
answers `ready`, the load balancer keeps routing, and every request 500s.

Name the clients you cannot serve without and each is resolved from the deps
container and asked `health()` — the signature every forze client port already
declares:

```python
from datetime import timedelta

from forze_postgres import PostgresClientDepKey
from forze_redis import RedisClientDepKey

attach_readiness_route(
    router,
    runtime,
    probes={"postgres": PostgresClientDepKey, "redis": RedisClientDepKey},
    probe_timeout=timedelta(seconds=2),
)
```

The response then carries a per-dependency breakdown, and any failed check makes the
whole probe a `503`:

```json
{"status": "degraded",
 "checks": {"postgres": {"ok": true,  "detail": "ok"},
            "redis":    {"ok": false, "detail": "timed out after 2.0s"}}}
```

`probe_timeout` is the budget for **each** probe, not for the sweep — deliberately.
The two look equivalent, since the probes run concurrently and the wall clock is the
same either way. They are not: when a dependency is unreachable its driver retries
past the deadline, and a sweep-wide timeout then cancels *every* probe and answers
`503` with `checks: {}` — no breakdown, in exactly the situation the breakdown exists
for. Per probe, a hanging dependency is one failed check with a name on it.

A probe reports failures and never becomes one. A client whose `health()` raises is
reported as `{"ok": false, "detail": "<ExceptionType>"}` — the type and not the
message, because a driver's connection error routinely carries the DSN and this body
is served to anything that can reach the probe. Nothing is auto-discovered: the
integrations satisfy `health()` structurally, with no base port to enumerate, and a
sweep of every registered dependency would probe things readiness does not depend on.
A `probe_timeout` that is not positive is refused at attach time.

Keep the endpoint internal, as you would `/metrics`. `detail` is the client's own
summary, and a failing driver's is its exception text — which routinely names the
host, the port and the user it failed to authenticate as. That is exactly what the
breakdown is for and exactly what should not reach the public internet. Every request
also runs every probe, so the probe period is a real query rate against each
dependency.

No `probes` (the default) answers from the scope's state alone.

The probe listener takes the same map as `checks`, with `check_timeout` per check,
and gives the same answer and the same body as the route from the same probes.
Liveness, on either, runs no check:

```python
probe_listener_step(
    port=8079,
    checks={"postgres": PostgresClientDepKey},
    check_timeout=timedelta(seconds=2),
)
```

## Graceful deploys

A rolling deploy replaces every replica, and a replica on its way out should cost
nobody a request. That takes three stages, in this order:

1. **Announce.** Readiness fails, so the load balancer stops routing new work
   here. Operations are still admitted: whatever the balancer already sent, or
   sends before it notices, is served normally.
2. **Drain phases.** Work that has to happen while the replica still serves runs
   now — a courier handing its routes to another replica, sockets closing gently
   over a window.
3. **Teardown.** The gate closes, in-flight operations get `drain_timeout`, and
   lifecycle shutdown runs, as in [Graceful drain](#graceful-drain).

`runtime.drain()` runs the first two stages; leaving the scope runs the third:

```python
from datetime import timedelta

await runtime.drain(timedelta(seconds=15), announce_delay=timedelta(seconds=5))
```

`announce_delay` is how long readiness fails before the first phase starts. Make it
at least as long as your balancer takes to act on a failed probe — for Kubernetes,
the readiness probe's `periodSeconds × failureThreshold`, plus however long the
balancer behind the Service takes to drop the endpoint.

Nobody has to call `drain()`. When the scope exits without one, `shutdown()` runs
it first, with the runtime's `drain_budget` and no announce delay, so the phases
run on every orderly exit. A drain runs at most once per scope, and only after
startup finished: after a partial startup failure the rollback has already shut
the steps down, so no phase runs against closed clients.

`runtime.begin_draining()` is the first stage alone: it withdraws readiness and
returns. Call it from whatever tells the process it is leaving — a `preStop` hook,
an internal admin endpoint — to take the replica out of rotation well before it
receives `SIGTERM`. Withdrawing readiness never refuses an operation:
`runtime.draining` turns true at once, while `ctx.drain_gate.draining`, which
means operations are refused, stays false until shutdown closes the gate.

An HTTP server needs its signal handling to call `drain()` before it stops
accepting. For uvicorn that is `serve_uvicorn` — see
[Graceful shutdown under uvicorn](../integrations/fastapi.md#graceful-shutdown-under-uvicorn).

### Drain phases

A lifecycle step registers its phase at startup, on the `ctx` its hook receives:

```python
from datetime import timedelta

from forze.application.execution import ExecutionContext


async def startup(ctx: ExecutionContext) -> None:
    async def hand_over(allowed: timedelta) -> None:
        ...  # hand the routes back, within `allowed`

    ctx.drain_phases.register(
        "hand-over",
        hand_over,
        order=10,
        max_duration=timedelta(seconds=5),
    )
```

Phases run in ascending `order` and share one budget:

- Each phase may take `min(max_duration, budget left)`, and is called with that
  figure so it can shrink its own window to fit.
- A phase without `max_duration` may take everything that is left. Give the early
  phases a cap, or one slow phase leaves the later ones nothing.
- A phase that overruns is cancelled. One that ignores the cancellation is
  abandoned a second later. A phase left less than 100 ms is skipped, and a phase
  that fails does not stop the next. Each of these is logged.

So `drain()` returns within `announce_delay + budget`, plus a second when a phase
ignores its cancellation.

Phases are not the background loops registered in `ctx.drainables` — relays,
consumers, pollers. Those stop after the gate has closed, between units of work.
A phase runs before that, while the replica still serves, which is the point of it.

### What a step can read

A step cannot hold the runtime that runs it: the runtime is built around its
lifecycle plan, so it does not exist yet when its steps are built. A step reads
the scope's state from the `ctx` its hooks receive instead:

| Attribute | True once |
|---|---|
| `ctx.lifecycle.started` | every lifecycle startup hook has returned |
| `ctx.lifecycle.announced` | a drain was announced — readiness is withdrawn |
| `ctx.drain_gate.draining` | the gate closed — new operations are refused |

### Fit the budget to the grace period

The orchestrator kills the process when its grace period runs out — on
Kubernetes, `terminationGracePeriodSeconds`, 30 seconds by default. Everything an
orderly exit does has to fit inside it:

```text
announce_delay
+ drain budget                (drain_budget when shutdown() drains)
+ the HTTP graceful timeout   (uvicorn's timeout_graceful_shutdown)
+ drain_timeout               (in-flight operations)
+ lifecycle shutdown          (up to shutdown_step_timeout per step)
```

A budget that does not fit gets the process killed in the middle of a phase or of
teardown — exactly the exit this ordering exists to avoid. Raise the grace period
or shrink the budgets until it fits, with headroom.

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

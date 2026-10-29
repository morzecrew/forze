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

1. If nothing drained the scope yet, its drain phases run, while operations are
   still admitted — see [Graceful deploys](#graceful-deploys).
2. The scope stops admitting new top-level invocations. They fail with a
   **retryable** `throttled` (`code="draining"`) — a **429** at the FastAPI
   edge, a requeue-worthy nack for a queue consumer.
3. In-flight operations get a bounded window — `drain_timeout`, default 10
   seconds — to finish before lifecycle teardown closes the clients they
   depend on. Whatever is still running after it is cancelled.
4. Background loops stop between units of work, and detached background work is
   cancelled.
5. Lifecycle shutdown runs as usual, in reverse wave order.

Step 3 waits up to `drain_timeout`; cancelling what outlives it, step 4's two
parts, and each lifecycle step's shutdown in step 5 are each bounded by
`shutdown_step_timeout` (10 seconds by default).

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

An app that builds its runtime inside the FastAPI lifespan has no runtime yet when
its factory attaches routes. Pass a zero-argument callable instead: it is asked on
every request, and returning `None` answers `503 unavailable`, as a runtime with no
open scope does. `runtime_lifespan(build=...)` provides one:

```python
from fastapi import APIRouter, FastAPI
from forze.application.execution import build_runtime
from forze_fastapi import runtime_lifespan
from forze_fastapi.routes import attach_readiness_route

lifespan = runtime_lifespan(build=lambda: build_runtime(...))
app = FastAPI(lifespan=lifespan)
router = APIRouter()
attach_readiness_route(router, lifespan.runtime)
app.include_router(router)
```

A callable that raises answers `unavailable` too. Its exception is logged and never
served to the probe.

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
[The Grafana stack](grafana-stack.md#probes) for the Kubernetes mapping. To drain
such a process on `SIGTERM`, hold its scope with
[`serve_runtime`](#graceful-deploys).

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
A `probe_timeout` that is not positive is refused at attach time. A probe resolves
its key plain: a key registered only per route names no single client and is
reported as `registered per route only` — to probe such a client, register it under
a plain key of its own too. A probe has no tenant bound, so a client routed per tenant
is reported as `routed per tenant`, and a tenant-aware one as `tenant-aware`; probe one
that reaches a fixed backend instead.

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
the steps down, so no phase runs against closed clients. A drain cancelled before
its first phase started — an admin request whose client hung up — leaves the
phases to the next drain or to shutdown. If `shutdown()` is interrupted while it
waits for a drain another task runs, it stops that drain, and gives its phase a
second to unwind, before teardown closes anything.

`runtime.begin_draining()` is the first stage alone: it withdraws readiness and
returns. Call it from whatever tells the process it is leaving — a `preStop` hook,
an internal admin endpoint — to take the replica out of rotation well before it
receives `SIGTERM`. Withdrawing readiness never refuses an operation:
`runtime.draining` turns true at once, while `ctx.drain_gate.draining`, which
means operations are refused, stays false until shutdown closes the gate.

An HTTP server needs its signal handling to call `drain()` before it stops
accepting. For uvicorn that is `serve_uvicorn` — see
[Graceful shutdown under uvicorn](../integrations/fastapi.md#graceful-shutdown-under-uvicorn).
Under several uvicorn workers, or with a runtime the lifespan builds, it is
`runtime_lifespan(..., drain_budget=...)` — see
[Several workers](../integrations/fastapi.md#several-workers-or-a-runtime-built-at-startup).

A process that serves no HTTP — the outbox relay, a consumer, a durable runner —
needs the same, and has no server to provide it. Hold its scope with
`serve_runtime`, beside the [probe listener](#readiness):

```python
import asyncio
from datetime import timedelta

from forze.application.execution import build_runtime, serve_runtime
from forze.application.execution.background import probe_listener_step

runtime = build_runtime(..., lifecycle_steps=[relay_step, probe_listener_step(port=8079)])

asyncio.run(
    serve_runtime(runtime, drain_budget=timedelta(seconds=5), announce_delay=timedelta(seconds=3))
)
```

On the first `SIGTERM` or `SIGINT` it runs `runtime.drain()` — the probe listener
reports `draining` from the announcement on — then leaves the scope, which tears
down, and returns. A bare `async with runtime.scope()` that waits forever has no
handler: as PID 1 in a container it ignores `SIGTERM` and is killed when the grace
period ends. The signals behave as under `serve_uvicorn`: a drain that fails still
tears down, a `SIGINT` during the drain cancels it and gives the phases up, and a
repeated `SIGTERM` is logged and ignored. A signal during startup waits for it to
finish, and the drain phases then run in the shutdown instead. Off the main thread,
pass `stop=asyncio.Event()` and set it to drain, as a first signal would. From another
thread, set it on the runtime's loop: `loop.call_soon_threadsafe(stop.set)`.

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

Two phases ship with forze, both closing realtime connections spread over a window so
their clients reconnect elsewhere a few at a time: `connection_drain_lifecycle_step` for
[Socket.IO](../integrations/socketio.md#drain-a-replica), and
`realtime_sse_drain_lifecycle_step` for the
[FastAPI SSE and WebSocket tails](../integrations/fastapi.md#drain-the-realtime-tails).
`spread_drain_lifecycle_step` in `forze.application.execution.background` is the same
phase for connections of your own.

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
Kubernetes, `terminationGracePeriodSeconds`, 30 seconds by default. The worst
case of an orderly exit has to fit inside it:

```text
announce_delay
+ drain budget                  (drain_budget when shutdown() drains)
+ 1 s                           (a phase that ignores its cancellation)
+ the HTTP graceful timeout     (uvicorn's timeout_graceful_shutdown)
+ drain_timeout                 (in-flight operations)
+ shutdown_step_timeout × 3     (cancelling what drain_timeout left running,
                                 stopping background loops, closing background work)
+ shutdown_step_timeout × steps (lifecycle shutdown, one per step)
```

With every default and no HTTP server that is already 51 seconds before the first
lifecycle step: 10 for the drain, 1, 10 for `drain_timeout` and 3 × 10. Most exits
take a fraction of it, but the grace period has to cover the one that does not. A budget that does not fit gets the process killed
in the middle of a phase or of teardown — exactly the exit this ordering exists
to avoid. Shrink the timeouts, raise the grace period, or both, with headroom.
`serve_uvicorn` logs this sum, figure by figure, when the server starts, and so
does `runtime_lifespan` with a `drain_budget`, in each worker.

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

Startup runs with no tenant bound, so the lock's route must be tenant-global: not
`tenant_aware`, and not on a client routed per tenant that has no tenant to route by.
Either one fails startup
with a `configuration` error coded `tenant_global_required`, here and in
`leadership_lifecycle_step` below. Under a declared tenant-isolation floor, mark the route
`RedisDistributedLockConfig(tenant_global=True)` so the floor leaves it out.

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

## Run one replica's worth of work

The singleton guard holds its lock only while startup runs, so it cannot keep one
replica doing something for the life of the process. A replica that starts later finds
the lock free and starts the work again, and when the replica that started it dies,
nobody takes over. For work that must run on one replica at a time — a publisher, a
single sequencer, a coordinator — use `leadership_lifecycle_step`. Every replica
campaigns for a renewing lease, and the one that holds it runs your body until the
lease is gone.

Work that tolerates running everywhere needs neither. The framework's outbox relay,
durable recovery and scheduler pollers and realtime sweeps are safe on every replica, so
run them unwrapped.

```python
from forze.application.contracts.dlock import DistributedLockSpec
from forze_kits.lifecycle import LeadershipSpec, leadership_lifecycle_step

publisher = LeadershipSpec(
    lock=DistributedLockSpec(name="lobby-publisher", requires_fencing_token=True),
    key="lobby-publisher",
)


async def claim(ctx, term):
    # ... SET fence = :token WHERE fence < :token — no row: another holder has this token
    return await claim_lobby_fence(ctx, term.token)


async def publish(ctx, term):
    while term.live():  # one unit at a time, so the body stops between units
        # ... WHERE fence <= :token — no row: a newer holder has written, so stop
        if not await publish_lobby_snapshot(ctx, fence=term.token):
            return


step = leadership_lifecycle_step(publisher, owner=instance_id, lead=publish, claim=claim)
```

The step's startup only starts the campaign, on every replica, so it is marked
`requires_long_running` and nothing else: `FLEET` accepts it without a singleton guard,
and `SERVERLESS` refuses it.

### How a term ends

The leader renews the lease every `renew_every` (a third of the lock's TTL by default)
and tells the body that its term is ending through `term.ended`, early enough that the
body's `yield_grace` — and, if it overruns, the `release_timeout` it gets to unwind once
cancelled — finishes before the lease could lapse. Both deadlines are measured from when
the latest successful renewal was **sent**, so a slow answer shortens the term rather than
lengthening it. The spec refuses timings that do not fit:
`renew_every + yield_grace + release_timeout + margin` must be shorter than the TTL, and
`release_timeout` no longer than `yield_grace`.

| `term.reason` | When | What happens to the body |
|---|---|---|
| `lost` | a renewal answers that the lease is gone | cancelled at once |
| `expiring` | renewals failed until `yield_grace` before the lease could lapse; the body's own `term.live()` check can see it first, as on resuming after a pause | cancelled `release_timeout` before the lease could lapse |
| `stopped` | the process is shutting down | cancelled after `yield_grace`, or earlier if renewals are failing |
| `yielded` | another replica asked for the unit (see below) | as for `stopped` |
| `returned` / `raised` | the body ended on its own | — |
| `unclaimed` | the `claim` hook found its token already claimed | `lead` never runs |

The lease is released only once the body is done, and the next replica then leads within
one `campaign_every`. A release that fails or overruns `release_timeout` leaves the lease
to lapse at its TTL instead, and the next replica leads within one `campaign_every` after
that. A body that does not finish within `release_timeout` of being cancelled
keeps the lease until its TTL runs out, so no successor starts before then; one that
starts after it may run beside that body, which is what fencing is for. Write the body to
stop at unit boundaries: cancellation at an arbitrary await is what the grace exists to
avoid.

### What fencing is for

A responsive leader whose body finishes within `release_timeout` of being cancelled runs
only while it holds the lease. A body that checks `term.live()` between units also stops
at its first check after a paused process or a blocked event loop resumes past the term's
soft deadline: the check compares that deadline with the clock, so it refuses before the
step's own timer has run. Nothing in the lease can stop the rest: the unit a pause interrupted, a body that
ignores its term or its cancellation, or a lock backend that lost its data while the leader
cannot yet know. Fencing covers those. Set
`requires_fencing_token` on the lock, pass `term.token` with every write the body makes,
and have the store refuse a token lower than the highest it has seen.

A token can repeat after the lock backend loses its data, so the first write of a term
**claims** the token and later writes accept an equal one:

```sql
-- claim, before the body's first write: the stored fence must be strictly lower
UPDATE digests SET fence = :token WHERE name = :name AND fence < :token;
-- every later write of the term
UPDATE digests SET body = :body, fence = :token WHERE name = :name AND fence <= :token;
```

Pass the claim as `claim=`; the step runs it before `lead`, and a claim that matches no
row ends the term as `unclaimed` and campaigns again. A later write that matches no row
means a newer holder has written: end the term by returning from `lead`.

After the lock backend loses its data, its fencing counter starts again from one while the
store keeps the highest fence it saw. Every term's claim is then refused, and the fleet
climbs one token per campaign interval until the counter passes the store's fence — leadership
stalls for as many campaigns as the store's fence is high. `fence_floor` is the fix: give the
step a read of the store's highest fence, and set `requires_fence_advance` on the lock spec.

```python
publisher = LeadershipSpec(
    lock=DistributedLockSpec(name="lobby-publisher", requires_fence_advance=True),
    key="lobby-publisher",
)


async def lobby_fence(ctx):
    return await read_lobby_fence(ctx)  # SELECT fence FROM lobby WHERE id = 1


step = leadership_lifecycle_step(
    publisher, owner=instance_id, lead=publish, claim=claim, fence_floor=lobby_fence
)
```

A replica that finds the lease free reads the floor and moves the lock's counter past it
before acquiring, so the first term after a loss draws a token above the store's fence and
its claim goes through at once. With the counter intact the advance changes nothing. A lock
backend that cannot advance its counter fails the step at startup. A floor that cannot be
read in time is skipped and the replica acquires anyway: a token below the store's fence is
refused by the claim, and the next campaign reads the floor again. A floor the lock can
never advance past, such as `None`, stops the campaign for good. The
[Redis page](../integrations/redis.md#fencing-after-data-loss) covers the capability
itself. The
[one-replica publisher recipe](../recipes/run-on-one-replica.md) runs the whole pattern.

### The clock behind `margin`

Redis keeps a lease's expiry on its own clock. If that clock steps forward — an NTP
correction, a failover to a replica whose clock runs ahead — the lease ends early, while
the leader still counts on its own monotonic clock. `margin` (one second by default) is
the allowance for that: the leader treats its lease as gone `margin` before the TTL.
Size it for your deployment's clock discipline, and keep the TTL at least ten times the
backend's renewal latency, or terms flap.

### Handoff on request

With `handoff="on_request"`, a replica whose `prefer(ctx)` returns true asks for the unit
by holding a second lock, `{key}:want` (so no lease key may end in `:want`), and renewing
it every campaign interval. The
holder yields at its next renewal, unless it prefers the unit itself or the asker has not
renewed its request within two campaign intervals — a dead asker's want-lock outlives it
by a TTL, and yielding to it would only leave the unit unled. Other replicas stand aside while someone asks, until they have
seen the lease free for three campaign intervals, so a fleet whose asker cannot take the
lease is unled for about four at most. An asker gives the want-lock up after three failed
acquires in a row, or at once when one is not answered in time, and does not ask again for
one TTL, so holders yield to an asker that cannot take the unit at most once a TTL. A
failing `prefer` or want-lock read does not count: that replica just campaigns without
handoff. A replica that `may_campaign` refuses never asks.

### Operating it

- **Stopping.** The step stops within `yield_grace + 3 × release_timeout` plus a second:
  the grace, then the body's unwind, the release, and a want-lock's or a late acquire's
  release. Keep that under the runtime's `shutdown_step_timeout` (ten seconds by default),
  or the loop is cancelled before it releases and the next leader waits out the TTL.
- **Campaign calls.** `may_campaign`, `prefer`, `fence_floor` and the campaign's reads of
  the lock are each cut off after `campaign_every`. Handoff and the fence floor are
  best-effort: when `prefer`, the want-lock or the floor fails or overruns, the replica
  campaigns as if they were off — acquiring without the advance at worst draws a token the
  claim refuses. `may_campaign` is the app's own gate, so a failure there sits that
  campaign out. A `configuration` error from any of them stops campaigning for good, as
  below. An acquire is never cancelled: one that lands after the step stopped waiting, or
  whose answer is lost after it landed, is released.
- **A body that raises** is released and restarted after the supervisor's backoff. A
  `configuration` error, from the body or from any campaign call, stops campaigning for
  good, with a critical log, while the process stays up — wiring does not fix itself by
  retrying. So do the two fencing refusals no retry can clear: a counter at its ceiling
  (`dlock.fence_exhausted`) and a `fence_floor` the lock cannot advance past
  (`dlock.fence_out_of_range`, such as `None` for a store with no row yet).
- **Metrics.** `instrument_leadership()` exports `forze.leadership.held` and
  `forze.leadership.terms_ended`; see the [metric catalog](../reference/metrics.md#leadership).
  A sustained rate of `lost` or `expiring` means the lock backend is slow or unreachable
  from the leader.

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

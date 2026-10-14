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

Point *liveness* somewhere else — at `attach_liveness_route(router)`, which answers
`200` unconditionally. A draining pod is alive but not ready; one endpoint answering
both questions gets the pod killed mid-drain instead of letting it finish.

Processes that are not HTTP servers — the outbox relay, inbox and commit-stream
consumers, the socket.io gateway, durable runners — get both probes from one
lifecycle step:

```python
from forze.application.execution.background import probe_listener_step

lifecycle_steps.append(probe_listener_step(runtime, port=8079))
```

Stdlib `asyncio`, no HTTP framework, same runtime state, same drain semantics: it
reports `draining` for the whole window and closes only after. See
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

No `probes` (the default) leaves the drain-gate-only behaviour untouched.

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


async def publish(ctx, term):
    while term.live():  # one unit at a time, so the body stops between units
        await publish_lobby_snapshot(ctx, fence=term.token)


step = leadership_lifecycle_step(publisher, owner=instance_id, lead=publish)
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
| `expiring` | renewals failed until `yield_grace` before the lease could lapse | cancelled `release_timeout` before the lease could lapse |
| `stopped` | the process is shutting down | cancelled after `yield_grace`, or earlier if renewals are failing |
| `yielded` | another replica asked for the unit (see below) | as for `stopped` |
| `returned` / `raised` | the body ended on its own | — |
| `unclaimed` | the `claim` hook found its token already claimed | `lead` never runs |

The lease is released only once the body is done, and the next replica leads within one
`campaign_every`. A body that does not finish within `release_timeout` of being cancelled
keeps the lease until its TTL runs out, so no successor starts before then; one that
starts after it may run beside that body, which is what fencing is for. Write the body to
stop at unit boundaries: cancellation at an arbitrary await is what the grace exists to
avoid.

### What fencing is for

A responsive leader whose body finishes within `release_timeout` of being cancelled runs
only while it holds the lease. Nothing in the lease can stop a leader that is **not**
responsive: a paused process, a blocked event loop, a body that ignores cancellation, or
a lock backend that lost its data while the leader cannot yet know. Fencing covers those. Set
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
row ends the term as `unclaimed` and campaigns again. The
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
by holding a second lock, `{key}:want`, and renewing it every campaign interval. The
holder yields at its next renewal, unless it prefers the unit itself or the asker has not
renewed its request within two campaign intervals — a dead asker's want-lock outlives it
by a TTL, and yielding to it would only leave the unit unled. Other replicas stand aside while someone asks, until they have
seen the lease free for three campaign intervals, so a fleet whose asker cannot take the
lease is unled for about four at most. An asker whose acquires keep failing
gives the want-lock up after three tries and does not ask again for one TTL, so it cannot
take the unit from every holder in turn. A replica that `may_campaign` refuses never
asks.

### Operating it

- **Stopping.** The step stops within `yield_grace + 3 × release_timeout` plus a second:
  the grace, then the body's unwind, the release, and a want-lock's or a late acquire's
  release. Keep that under the runtime's `shutdown_step_timeout` (ten seconds by default),
  or the loop is cancelled before it releases and the next leader waits out the TTL.
- **Campaign calls.** `may_campaign`, `prefer` and the campaign's reads of the lock are
  each cut off after `release_timeout`; one that overruns or raises skips that campaign. An
  acquire is never cancelled: one that lands after the step stopped waiting, or that
  raised after landing, is released at once.
- **A body that raises** is released and restarted after the supervisor's backoff. A
  `configuration` error stops campaigning for good, with a critical log, while the
  process stays up — wiring does not fix itself by retrying.
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

# Shutdown and fleet posture

Bringing a service to rest: closing the admission gate, draining the planes, what `settled` and `attested` actually promise, and running the same app as N replicas. Per-call resilience is [resilience](resilience.md).

## Graceful shutdown & readiness

`runtime.shutdown()` (and `runtime_lifespan` / `scope()` exit) drains before teardown: new top-level invocations fail with retryable `throttled` (`code="draining"`, 429), in-flight operations get `drain_timeout` (default 10s, a `build_runtime` kwarg) to finish. Expose readiness so the load balancer stops routing first:

```python
from forze_fastapi.routes import attach_readiness_route

attach_readiness_route(router, runtime)   # GET /readyz → 200 / 503 draining
```

Runtime built inside the FastAPI lifespan, so the app factory has none yet? Pass a zero-argument callable instead, asked per request: with `lifespan = runtime_lifespan(build=build_my_runtime)`, `attach_readiness_route(router, lifespan.runtime)`. `None`, or a callable that raises, answers `503 unavailable`.

The drain gate says nothing about whether dependencies are reachable — an un-drained process whose database is gone still answers `ready`. Name the clients you cannot serve without and each is asked its `health()`:

```python
from datetime import timedelta

from forze_postgres import PostgresClientDepKey
from forze_redis import RedisClientDepKey

attach_readiness_route(
    router,
    runtime,
    probes={"postgres": PostgresClientDepKey, "redis": RedisClientDepKey},
    probe_timeout=timedelta(seconds=2),   # per probe, not for the sweep
)
```

Any failed check makes the response `503 {"status": "degraded", "checks": {...}}`, with `{"ok": bool, "detail": str}` per dependency. `probe_timeout` is per probe deliberately: a sweep-wide deadline cancels every probe when one dependency's driver retries past it, and answers with an empty breakdown.

Readiness answers `503 unavailable` until every lifecycle startup step has returned. A worker that is not an HTTP server gets the same answer, from the same evaluator, by putting `probe_listener_step(port=8079, checks={"postgres": PostgresClientDepKey})` (from `forze.application.execution.background`) in its runtime's own lifecycle steps: the step reads its scope from the `ctx` its hook receives.

Background loops the runtime owns stop **between units of work** rather than being cancelled mid-flight, and register as drainable. A custom loop or consumer must accept the stop signal to take part.

## Graceful deploys

The exit order is: withdraw readiness, run the drain phases while still serving, then close the gate and tear down.

- `runtime.begin_draining()` withdraws readiness with operations still admitted; `runtime.draining` turns true, `ctx.drain_gate.draining` (operations refused) stays false until shutdown.
- `await runtime.drain(budget, announce_delay=...)` announces, waits `announce_delay` for the balancer, then runs every phase steps registered with `ctx.drain_phases.register(name, phase, order=..., max_duration=...)`. A phase is awaited with `min(max_duration, budget left)` as a `timedelta`, cancelled past it, and skipped under 100 ms. It runs once per scope; `shutdown()` runs it first if nobody did, with `build_runtime(drain_budget=...)`.
- A step reads `ctx.lifecycle.started` and `ctx.lifecycle.announced` from its hook's `ctx` — it cannot hold the runtime whose plan contains it.
- Under FastAPI, serve with `await serve_uvicorn(uvicorn.Config(app), runtime, drain_budget=...)` from `forze_fastapi`, so `SIGTERM` drains before uvicorn stops accepting. Under `uvicorn --workers N`, gunicorn's uvicorn worker, or a runtime built in the lifespan, pass `runtime_lifespan(build=..., drain_budget=...)` instead: each worker drains its own runtime on `SIGTERM` before its server exits, and logs its own worst case (set gunicorn's `--graceful-timeout` above it). Without either, `uvicorn app:app` closes WebSockets at once (1012) and, without `--timeout-graceful-shutdown`, waits for open streams without limit.
- A process with no HTTP server (relay, consumer, worker) holds its scope with `await serve_runtime(runtime, drain_budget=...)` from `forze.application.execution`: the first `SIGTERM` drains, then the scope tears down. A bare `async with runtime.scope(): await asyncio.Event().wait()` has no handler, so as PID 1 it ignores `SIGTERM` and is killed at the grace period.
- Realtime connections close as a drain phase, spread over a window so clients reconnect elsewhere a few at a time: `connection_drain_lifecycle_step(sio, window=..., tail=...)` (`forze_socketio`; pass `attach_realtime_connection(..., draining=lambda: runtime.draining)` with the serving runtime, so connects meanwhile are turned away by a transport close the client reconnects from) and `realtime_sse_drain_lifecycle_step(hub, window=..., tail=...)` (`forze_fastapi.realtime`, ending SSE streams and WebSocket tails). The window and tail shrink to the phase's allowance. Route WebSockets least-connections.

Fit the worst case — `announce_delay + drain budget + 1 s + uvicorn graceful timeout + drain_timeout + shutdown_step_timeout × (3 + lifecycle steps)` — inside the orchestrator's grace period, or the process is killed mid-phase; `serve_uvicorn`, or `runtime_lifespan` given a `drain_budget`, logs the sum at startup. Off the main thread, pass `stop=asyncio.Event()` to `serve_uvicorn` or `serve_runtime` and set it to drain and stop.

## Quiesce: bring the planes to rest

When you need more than "in-flight requests finished" — before a shutdown, an export, or a migration — `quiesce` also waits for the operational planes:

```python
from forze_kits.integrations.quiesce import quiesce

async with runtime.scope():
    report = await quiesce(runtime, timeout=timedelta(seconds=60))
```

It closes the admission gate, stops the runtime's loops and flushes the outbox relay, then polls each outbox route (every route in the [spec inventory](runtime-lifecycle.md) by default), the durable-run plane and each named stream group until empty or the budget expires. Consumer groups stay explicit — a group name is the identity of whoever is reading, so pass `streams=` / `ack_streams=`.

Read the report correctly: `settled` means nothing was moving; `attested` means nothing was moving **and nothing could arrive**. Only `attested` is safe to build on. `close_gate=False` turns it into a read-only health check that can settle but never attest — and closing the gate is **one-way** for the life of the scope. It holds one process still; stop the fleet first.

## Fleet posture (N replicas)

```python
from forze.application.execution import DeploymentProfile, build_runtime

runtime = build_runtime(..., deployment=DeploymentProfile.FLEET)
```

`FLEET` fails assembly for any lifecycle step marked `mutates_shared_state=True` that is not `singleton_guarded`. Guard ensure-style startup work (indexes, queue declarations, seeds) with `singleton_lifecycle_step(step, spec=DistributedLockSpec(name=...), owner=instance_id)` from `forze_kits.lifecycle` (pass the lock spec, not a live port — the guard resolves the command port from the scope at startup) — one replica runs it, the rest skip. Run one-shot migrations as deploy steps, never as runtime steps.

Fleet-wide resilience state (`forze[redis]`): `ResilienceDepsModule(breaker_store=redis_circuit_breaker_store(redis), rate_limit_store=redis_rate_limit_store(redis))` — otherwise breakers protect one replica and the effective rate is `permits × replicas`. Both fail open to process-local state. Bulkheads stay process-local by design.

## Anti-patterns

- Running schema migrations as a `singleton_lifecycle_step` — skip-if-held gives at-most-one-runner *per startup wave*, not run-exactly-once; use a deploy step.
- Passing `runtime` to `probe_listener_step` (deprecated) or to any step meant for that runtime's own plan — the runtime does not exist yet when its steps are built; read state from the hook's `ctx`.
- Giving every drain phase no `max_duration` — the first slow phase spends the whole budget and the later ones are skipped.

## Reference

- [Shutdown & fleets](https://morzecrew.github.io/forze/latest/running-in-prod/shutdown-and-fleets/)
- [Portability](https://morzecrew.github.io/forze/latest/running-in-prod/portability/)

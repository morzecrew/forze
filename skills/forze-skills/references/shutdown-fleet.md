# Shutdown and fleet posture

## Graceful shutdown & readiness

`runtime.shutdown()` (and `runtime_lifespan` / `scope()` exit) drains before teardown: new top-level invocations fail with retryable `throttled` (`code="draining"`, 429), in-flight operations get `drain_timeout` (default 10s, a `build_runtime` kwarg) to finish. Expose readiness so the load balancer stops routing first:

```python
from forze_fastapi.routes import attach_readiness_route

attach_readiness_route(router, runtime)   # GET /readyz → 200 / 503 draining
```

Background loops the runtime owns stop **between units of work** rather than being cancelled mid-flight, and register as drainable. A custom loop or consumer must accept the stop signal to take part.

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
runtime = build_runtime(..., deployment=DeploymentProfile.FLEET)
```

`FLEET` fails assembly for any lifecycle step marked `mutates_shared_state=True` that is not `singleton_guarded`. Guard ensure-style startup work (indexes, queue declarations, seeds) with `singleton_lifecycle_step(step, spec=DistributedLockSpec(name=...), owner=instance_id)` from `forze_kits.lifecycle` (pass the lock spec, not a live port — the guard resolves the command port from the scope at startup) — one replica runs it, the rest skip. Run one-shot migrations as deploy steps, never as runtime steps.

Fleet-wide resilience state (`forze[redis]`): `ResilienceDepsModule(breaker_store=redis_circuit_breaker_store(redis), rate_limit_store=redis_rate_limit_store(redis))` — otherwise breakers protect one replica and the effective rate is `permits × replicas`. Both fail open to process-local state. Bulkheads stay process-local by design.

## Anti-patterns

- Running schema migrations as a `singleton_lifecycle_step` — skip-if-held gives at-most-one-runner *per startup wave*, not run-exactly-once; use a deploy step.

## Reference

- [Shutdown & fleets](https://morzecrew.github.io/forze/latest/running-in-prod/shutdown-and-fleets/)
- [Portability](https://morzecrew.github.io/forze/latest/running-in-prod/portability/)

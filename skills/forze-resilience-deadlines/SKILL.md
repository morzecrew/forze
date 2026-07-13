---
name: forze-resilience-deadlines
description: >-
  Adds fault tolerance and production hardening to a Forze app: resilience
  policies (retries, rate limits, circuit breakers, bulkheads), invocation
  deadlines and time budgets, graceful shutdown/drain, readiness probes, and
  fleet (multi-replica) deployment posture. Use when configuring timeouts,
  retries, throttling, adaptive concurrency, or deploying replicas behind a
  load balancer.
---

# Forze Resilience & Deadlines

Use when hardening a Forze application: composing resilience policies, declaring operation time budgets, wiring graceful shutdown and readiness, or preparing an app to run as N replicas. For runtime assembly see [`forze-wiring`](../forze-wiring/SKILL.md); for error kinds and metrics see [`forze-observability-errors`](../forze-observability-errors/SKILL.md).

## Resilience policies

A `ResiliencePolicy` is an ordered stack of strategies (outer → inner: rate limit → bulkhead → circuit breaker → retry → per-attempt timeout, plus optional fallback/hedge). Built-ins `occ` (retry on `concurrency`) and `transient` (retry on `infrastructure`, 30s per-attempt timeout) ship ready to use.

Apply declaratively on an operation registry or imperatively around a call:

```python
ResilienceWrap(policy="transient").to_step()      # declarative, on the registry

result = await ctx.resilience().run(              # imperative, around one call
    lambda: charge_card(payment),
    policy="transient",
    route="payments",   # keys breaker/bulkhead/rate-limit state per dependency
)
```

Register app policies via `ResilienceDepsModule(spec=...)` (merged over the built-ins). Retry only fires on **retryable** kinds: `concurrency`, `infrastructure`, `throttled`.

### Port-level policies

Wrap every public coroutine method of a resolved port without touching call sites:

```python
from forze.application.contracts.resilience import PortPolicy

ResilienceDepsModule(
    spec=my_policies,
    port_policies=(PortPolicy(key=HttpServiceDepKey, policy="vendor_rl"),),
)
```

### Bulkheads: fixed or adaptive

`BulkheadStrategy(max_concurrency=, max_queue=)` is a fixed cap. `AdaptiveBulkheadStrategy(latency_threshold=, max_concurrency=)` sets the cap by observed latency (AIMD): starts at `max_concurrency`, backs off multiplicatively when a completion exceeds the threshold, recovers additively. Errors never shrink the limit (that is the breaker's job); the two strategies are mutually exclusive in one policy. Add `latency_quantile=0.95` to breach on the *observed p95* (windowed P² estimate) instead of any single slow completion — outlier-immune; the contract becomes "the p95 stays under the threshold".

Queued bulkheads (`max_queue >= 1`) take opt-in queue management on both kinds: `queue_target=` (CoDel — shed waiters parked too long under sustained congestion) and `queue_adaptive_lifo=True` (serve newest first while congested; pair with `queue_target`).

### Adaptive client throttling

`AdaptiveThrottleStrategy(k=2.0, window=timedelta(minutes=2), min_throughput=10)` is the breaker's sibling for **degraded-but-alive** downstreams: it sheds locally with probability `max(0, (requests − k·accepts)/(requests + 1))`, so at 50% downstream failure it sends roughly the traffic the downstream absorbs (the breaker is all-or-trickle). Healthy traffic is never shed; shed calls raise retryable `throttled` (`code="adaptive_throttle"`); domain rejections count as accepts. **Mutually exclusive with `CircuitBreakerStrategy` in one policy** — pick the throttle for downstreams that degrade, the breaker for ones that die outright.

### Tail-based hedging

`HedgeStrategy(delay=, max_attempts=)` races a concurrent copy against a slow primary (idempotent reads only; `budget=` caps amplification), run via `ctx.resilience().run_hedged(...)`. Set `adaptive_delay_quantile=0.95` to hedge after the *observed* p95 per `(policy, route)` (streaming P² estimate, windowed) instead of the fixed delay — `delay` becomes the pre-warmup fallback, `delay_min`/`delay_max` clamp the estimate.

### Control plane: `ResilienceAdminPort`

`ctx.resilience.admin()` (or `ResilienceAdminDepKey`) inspects and retunes live policy state without a redeploy: `inspect(policy=...)` returns per-`(policy, route)` snapshots (forced-open flag, adaptive concurrency limit, in-use/waiting, effective hedge delay); `force_open(policy, route=None)` / `clear_forced_open(...)` are a manual breaker kill-switch; `retune(policy)` hot-swaps a `ResiliencePolicy` by name. See [Resilience tuning](https://morzecrew.github.io/forze/latest/reference/resilience-tuning/).

## Invocation deadlines

Declare a time budget on the **operation plan**, not per route or caller:

```python
registry.bind("orders.create").with_deadline(timedelta(seconds=5)).finish().freeze()
# or a default across many ops: registry.patch(selector).with_deadline(...).finish()
# scope it: registry.patch(selector, namespace=ns)  → matches only ops under ns
# settle it: registry.materialize_patches()  → fold patches into plans so a later
#            OperationRegistry.merge can't leak them onto a sibling's operations
```

- Boundaries may add a caller budget: `with bind_deadline(timeout_s): ...` (from `forze.application.execution`); `None` is a no-op passthrough. Binding is **tighten-only** — the tightest budget always wins.
- Expiry raises `exc.timeout` (`code="deadline_exceeded"`, **504** via FastAPI), which is **non-retryable** — the budget is spent. The per-attempt `TimeoutStrategy` stays retryable `infrastructure`; they compose.
- Ports can read `remaining_time()` to derive per-call budgets.
- Cross-service: the outbound HTTP adapter forwards the remaining budget as `X-Forze-Deadline-Budget` (opt out: `HttpServiceConfig(propagate_deadline=False)`); the receiving FastAPI side honors it only with `InvocationMetadataMiddleware(..., bind_deadline_from_header=True)`.

## Graceful shutdown & readiness

`runtime.shutdown()` (and `runtime_lifespan` / `scope()` exit) drains before teardown: new top-level invocations fail with retryable `throttled` (`code="draining"`, 429), in-flight operations get `drain_timeout` (default 10s, a `build_runtime` kwarg) to finish. Expose readiness so the load balancer stops routing first:

```python
from forze_fastapi.routes import attach_readiness_route

attach_readiness_route(router, runtime)   # GET /readyz → 200 / 503 draining
```

## Fleet posture (N replicas)

```python
runtime = build_runtime(..., deployment=DeploymentProfile.FLEET)
```

`FLEET` fails assembly for any lifecycle step marked `mutates_shared_state=True` that is not `singleton_guarded`. Guard ensure-style startup work (indexes, queue declarations, seeds) with `singleton_lifecycle_step(step, spec=DistributedLockSpec(name=...), owner=instance_id)` from `forze_kits.lifecycle` (pass the lock spec, not a live port — the guard resolves the command port from the scope at startup) — one replica runs it, the rest skip. Run one-shot migrations as deploy steps, never as runtime steps.

Fleet-wide resilience state (`forze[redis]`): `ResilienceDepsModule(breaker_store=redis_circuit_breaker_store(redis), rate_limit_store=redis_rate_limit_store(redis))` — otherwise breakers protect one replica and the effective rate is `permits × replicas`. Both fail open to process-local state. Bulkheads stay process-local by design.

## Gotchas

- A retry re-runs the **whole operation** in a fresh transaction — retried work must be safe to repeat.
- The rate limiter never queues: an empty bucket raises `throttled` immediately. To wait instead, wrap the call in a retry policy with `retry_on={ExceptionKind.THROTTLED}`.
- Mark `mutates_shared_state=True` on lifecycle steps that touch shared backends — the `FLEET` validation is honest-by-declaration, it cannot detect mutation structurally.
- The adaptive bulkhead's latency sample is the whole guarded call (retries included when composed with Retry) — set `latency_threshold` for the logical call, not a single attempt.

## Anti-patterns

1. Hand-rolled `asyncio.wait_for` timeouts in handlers — declare a plan deadline with `with_deadline(...)` so the catalog, FastAPI (`x-deadline-seconds`), and MCP projections stay truthful.
2. Retrying `timeout` failures in a policy — the kind is non-retryable by design; a fresh invocation carries a fresh deadline.
3. Running schema migrations as a `singleton_lifecycle_step` — skip-if-held gives at-most-one-runner *per startup wave*, not run-exactly-once; use a deploy step.
4. Declaring `permits/per` for the fleet while using the default in-process rate-limit store — each replica enforces it independently.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Resilience](https://morzecrew.github.io/forze/latest/running-in-prod/resilience/)
- [Resilience tuning reference](https://morzecrew.github.io/forze/latest/reference/resilience-tuning/)
- [Deadlines](https://morzecrew.github.io/forze/latest/running-in-prod/deadlines/)
- [Shutdown & fleets](https://morzecrew.github.io/forze/latest/running-in-prod/shutdown-and-fleets/)
- [Observability](https://morzecrew.github.io/forze/latest/running-in-prod/observability/)

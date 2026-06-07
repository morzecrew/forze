# Resilience contracts

A composable resilience policy pipeline (Polly / resilience4j style): declare a policy
once and apply it uniformly at adapter call sites or around whole operations. The
built-in executor implements retry, timeout, circuit-breaking, and bulkheads in-process —
no external dependency.

## Policy vocabulary

A `ResiliencePolicy` is an ordered tuple of strategy value objects (outer → inner):

| Strategy | Purpose |
|----------|---------|
| `BulkheadStrategy` | Limit concurrency with a bounded wait queue. |
| `CircuitBreakerStrategy` | Trip open on a rolling-window failure ratio; probe half-open. |
| `RetryStrategy` | Bounded retry with `BackoffStrategy` (jitter modes incl. decorrelated) and an optional `RetryBudget` token-bucket cap. |
| `TimeoutStrategy` | Per-attempt timeout. |
| `FallbackStrategy` | Marker enabling a call-site `fallback`. |

Retry classification reuses the `ExceptionKind` taxonomy — `RetryStrategy.retry_on`
narrows which retryable kinds (`CONCURRENCY`, `INFRASTRUCTURE`) a policy handles.

## `ResilienceSpec` and named policies

`ResilienceSpec(name, policies)` is the named-policy catalog. Register it via
`ResilienceDepsModule(spec=...)`; the module merges `builtin_default_policies()`
(`occ`, `transient`) as a floor, so an app can retune a policy but cannot remove one the
framework's own adapters depend on.

## `ResilienceExecutorPort`

`run(fn, *, policy, route=None, fallback=None)` runs a zero-arg async callable under a
named policy. `route` keys process-local breaker/bulkhead state so distinct backends under
one policy fail independently. Resolve the executor with `ctx.resilience()` (a registered
module is required), or `resolve_resilience_executor(ctx)` for the shared-default fallback.

    :::python
    result = await ctx.resilience().run(
        lambda: client.query(sql),
        policy="transient",
        route="reports",
    )

## Adapter boundary — `occ_retry`

`forze.application.execution.resilience.occ_retry` decorates a read-modify-write gateway
method with the `occ` policy (retry on `ExceptionKind.CONCURRENCY`). The Postgres / Mongo
/ Firestore write gateways use it; each retry re-reads current state before recomputing.

## Operation boundary — `ResilienceWrap`

`forze.application.hooks.resilience.ResilienceWrap` applies a named policy around a whole
operation, attached per operation:

    :::python
    from forze.application.hooks.resilience import ResilienceWrap

    registry.bind("reports.run").bind_outer().wrap(
        ResilienceWrap(policy="transient", route="reports").to_step()
    )

**Retry safety:** a retry re-executes the operation with a fresh transaction per attempt,
so transactional side effects roll back between attempts. Only attach a retry-bearing
policy to operations that tolerate re-execution: read-only, fully transactional, or
guarded by [`IdempotencyWrap`](idempotency.md). Use a timeout/breaker-only policy for
operations with non-transactional external side effects.

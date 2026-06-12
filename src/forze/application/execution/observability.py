"""OpenTelemetry instrumentation for Forze operations.

``instrument_operations`` wraps every operation in a registry with a span + metrics, using
the existing per-operation ``wrap`` middleware seam — no engine changes. OpenTelemetry is a
core dependency (the logging layer already uses it), so this is built in, not an optional
extra. Emits via the global OpenTelemetry providers — configure the SDK + exporter in your
app.
"""

from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Iterable

from opentelemetry import metrics, trace
from opentelemetry.metrics import Observation
from opentelemetry.trace import Status, StatusCode

from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)
from forze.application.contracts.tenancy import TenantPoolStats

from .operations.registry import OperationRegistry
from .resilience import InProcessResilienceExecutor

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Counter, Histogram, Meter
    from opentelemetry.trace import Tracer

    from .context import ExecutionContext

# ----------------------- #

_TELEMETRY_STEP_ID = "otel.telemetry"

_TELEMETRY_PRIORITY = -1_000_000
"""Lowest priority → outermost wrap, so the span measures the whole operation."""

OPERATIONS_COUNTER = "forze.operations"
DURATION_HISTOGRAM = "forze.operation.duration"

RESILIENCE_EVENTS_COUNTER = "forze.resilience.events"
BREAKER_STATE_GAUGE = "forze.resilience.breaker.state"
BULKHEAD_QUEUE_GAUGE = "forze.resilience.bulkhead.queue_depth"
BULKHEAD_LIMIT_GAUGE = "forze.resilience.bulkhead.limit"
HEDGE_DELAY_GAUGE = "forze.resilience.hedge.delay"

TENANT_POOL_SIZE_GAUGE = "forze.tenancy.pool.size"
TENANT_POOL_CAPACITY_GAUGE = "forze.tenancy.pool.capacity"
TENANT_POOL_CREATED_COUNTER = "forze.tenancy.pool.created"
TENANT_POOL_DISPOSED_COUNTER = "forze.tenancy.pool.disposed"
TENANT_POOL_EVICTED_COUNTER = "forze.tenancy.pool.evicted_explicit"

_BREAKER_PHASE_VALUES: dict[str, int] = {
    "breaker_close": 0,
    "breaker_half_open": 1,
    "breaker_open": 2,
}
"""Breaker gauge encoding: 0 = closed, 1 = half-open, 2 = open."""


def instrument_operations(
    registry: OperationRegistry,
    *,
    tracer: Tracer | None = None,
    meter: Meter | None = None,
) -> OperationRegistry:
    """Instrument every operation in *registry* with an OpenTelemetry span + metrics.

    Returns a new registry (call before ``.freeze()``). Each operation runs inside a span
    named by its key (attributes: kind, execution/correlation/causation ids, tenant,
    principal), and records a request counter (``forze.operations``) and a duration
    histogram (``forze.operation.duration``, ms), labelled by operation / kind / outcome.

    Emits via the global OTel providers unless *tracer* / *meter* are supplied — configure
    the OTel SDK + exporter in your app. Pair with ``configure_logging(otel_config=...)`` to
    correlate logs to the active span.
    """

    tracer = tracer or trace.get_tracer("forze")
    meter = meter or metrics.get_meter("forze")

    counter = meter.create_counter(
        OPERATIONS_COUNTER,
        unit="1",
        description="Count of Forze operations executed.",
    )
    duration = meter.create_histogram(
        DURATION_HISTOGRAM,
        unit="ms",
        description="Duration of Forze operations in milliseconds.",
    )

    for op in registry.operation_keys():
        step = MiddlewareStep(
            id=_TELEMETRY_STEP_ID,
            priority=_TELEMETRY_PRIORITY,
            factory=_telemetry_factory(str(op), tracer, counter, duration),
        )
        registry = registry.bind(op).bind_outer().wrap(step).finish(deep=True)

    return registry


# ....................... #


def instrument_resilience(
    executor: InProcessResilienceExecutor,
    *,
    meter: Meter | None = None,
) -> InProcessResilienceExecutor:
    """Export the executor's resilience events as always-on OpenTelemetry metrics.

    Attaches a metrics sink to *executor* (returned for chaining) — unlike the
    runtime-trace events, these are **independent of the tracing gate**, so a
    production process with tracing off still reports:

    - ``forze.resilience.events`` (counter): every event, labelled by
      ``forze.event`` / ``forze.policy`` / ``forze.route`` — retry attempts,
      timeouts, rate-limit/bulkhead rejections, budget exhaustion, breaker
      transitions. Note ``breaker_open`` counts the open *transition* and every
      admission rejected while open, so its rate tracks shed load.
    - ``forze.resilience.breaker.state`` (gauge): per policy/route breaker
      phase (0 = closed, 1 = half-open, 2 = open). Reported on transitions and
      on rejected admissions; a breaker that never tripped reports nothing
      (closed by absence).
    - ``forze.resilience.bulkhead.queue_depth`` (observable gauge): calls
      queued behind each bulkhead's semaphore, sampled at collection time.
      State appears lazily once a bulkhead-bearing policy is first used.
    - ``forze.resilience.bulkhead.limit`` (observable gauge): the current AIMD
      concurrency limit per adaptive bulkhead (``bulkhead_backoff`` events in
      the counter mark each multiplicative decrease).
    - ``forze.resilience.hedge.delay`` (observable gauge): the effective
      adaptive hedge delay in seconds — the windowed P² quantile estimate,
      clamped by the strategy's floor/cap. Appears once an adaptive-delay
      hedge policy is first used.

    Emits via the global OTel meter unless *meter* is supplied. Call once at
    assembly time, alongside :func:`instrument_operations`.
    """

    meter = meter or metrics.get_meter("forze")

    events = meter.create_counter(
        RESILIENCE_EVENTS_COUNTER,
        unit="1",
        description="Count of resilience events (retries, rejections, breaker transitions).",
    )
    breaker_state = meter.create_gauge(
        BREAKER_STATE_GAUGE,
        unit="1",
        description="Circuit breaker phase per policy/route (0=closed, 1=half-open, 2=open).",
    )

    def _sink(event: str, policy: str, route: str | None) -> None:
        labels: dict[str, str] = {"forze.policy": policy}

        if route is not None:
            labels["forze.route"] = route

        events.add(1, {**labels, "forze.event": event})

        phase = _BREAKER_PHASE_VALUES.get(event)

        if phase is not None:
            breaker_state.set(phase, labels)

    def _observe_queue_depth(_options: CallbackOptions) -> Iterable[Observation]:
        for policy, route, waiting in executor.bulkhead_queue_depths():
            labels = {"forze.policy": policy}

            if route is not None:
                labels["forze.route"] = route

            yield Observation(waiting, labels)

    meter.create_observable_gauge(
        BULKHEAD_QUEUE_GAUGE,
        callbacks=[_observe_queue_depth],
        unit="1",
        description="Calls queued behind each bulkhead's semaphore.",
    )

    def _observe_limits(_options: CallbackOptions) -> Iterable[Observation]:
        for policy, route, limit in executor.adaptive_bulkhead_limits():
            labels = {"forze.policy": policy}

            if route is not None:
                labels["forze.route"] = route

            yield Observation(limit, labels)

    meter.create_observable_gauge(
        BULKHEAD_LIMIT_GAUGE,
        callbacks=[_observe_limits],
        unit="1",
        description="Current AIMD concurrency limit per adaptive bulkhead.",
    )

    def _observe_hedge_delays(_options: CallbackOptions) -> Iterable[Observation]:
        for policy, route, delay in executor.hedge_delays():
            labels = {"forze.policy": policy}

            if route is not None:
                labels["forze.route"] = route

            yield Observation(delay, labels)

    meter.create_observable_gauge(
        HEDGE_DELAY_GAUGE,
        callbacks=[_observe_hedge_delays],
        unit="s",
        description="Effective adaptive hedge delay (P² quantile estimate) per policy/route.",
    )

    executor.set_metrics_sink(_sink)

    return executor


# ....................... #


def instrument_tenant_pools(
    pools: dict[str, Any],
    *,
    meter: Meter | None = None,
) -> None:
    """Export tenant pool churn counters as OpenTelemetry observable metrics.

    *pools* maps a label (e.g. ``"postgres"``) to anything exposing
    ``pool_stats() -> TenantPoolStats`` — every routed client does. Emits, per
    pool (labelled ``forze.client``):

    - ``forze.tenancy.pool.size`` / ``forze.tenancy.pool.capacity`` (gauges)
    - ``forze.tenancy.pool.created`` / ``….disposed`` /
      ``….evicted_explicit`` (cumulative observable counters)

    The alert that matters: a sustained ``created`` rate while ``size`` sits
    at ``capacity`` is LRU thrash — hot tenants' pools evicted by cold
    one-off traffic, each rebuild paying full connection establishment. That
    signal is what gates any future admission-policy work on the registries.

    Emits via the global OTel meter unless *meter* is supplied. Call once at
    assembly time, alongside the other ``instrument_*`` calls.
    """

    meter = meter or metrics.get_meter("forze")

    def _observe(
        pick: Callable[[TenantPoolStats], int],
    ) -> Callable[[CallbackOptions], Iterable[Observation]]:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            for label, client in pools.items():
                stats: TenantPoolStats = client.pool_stats()

                yield Observation(pick(stats), {"forze.client": label})

        return callback

    meter.create_observable_gauge(
        TENANT_POOL_SIZE_GAUGE,
        callbacks=[_observe(lambda s: s.size)],
        unit="1",
        description="Live tenant pools per routed client.",
    )
    meter.create_observable_gauge(
        TENANT_POOL_CAPACITY_GAUGE,
        callbacks=[_observe(lambda s: s.capacity)],
        unit="1",
        description="Tenant pool capacity (max_cached_tenants) per routed client.",
    )
    meter.create_observable_counter(
        TENANT_POOL_CREATED_COUNTER,
        callbacks=[_observe(lambda s: s.created)],
        unit="1",
        description="Cumulative tenant pool creations.",
    )
    meter.create_observable_counter(
        TENANT_POOL_DISPOSED_COUNTER,
        callbacks=[_observe(lambda s: s.disposed)],
        unit="1",
        description="Cumulative tenant pool disposals.",
    )
    meter.create_observable_counter(
        TENANT_POOL_EVICTED_COUNTER,
        callbacks=[_observe(lambda s: s.evicted_explicit)],
        unit="1",
        description="Cumulative explicit tenant evictions (rotation signals).",
    )


# ....................... #


def _telemetry_factory(
    op_name: str,
    tracer: Tracer,
    counter: Counter,
    duration: Histogram,
) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> Middleware[Any, Any]:
        async def middleware(
            next: Any,  # noqa: A002 — matches the Middleware protocol parameter name
            args: Any,
        ) -> Any:
            kind = "query" if ctx.inv_ctx.is_read_only() else "command"
            labels = {"forze.operation": op_name, "forze.operation.kind": kind}

            start = perf_counter()
            outcome = "success"

            with tracer.start_as_current_span(
                op_name, attributes=_span_attributes(ctx, op_name, kind)
            ) as span:
                try:
                    return await next(args)

                except BaseException as exc:
                    outcome = "error"
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR))
                    raise

                finally:
                    elapsed_ms = (perf_counter() - start) * 1000.0
                    out = {**labels, "forze.outcome": outcome}
                    counter.add(1, out)
                    duration.record(elapsed_ms, out)

        return middleware

    return factory


# ....................... #


def _span_attributes(
    ctx: ExecutionContext, op_name: str, kind: str
) -> dict[str, str]:
    attributes: dict[str, str] = {
        "forze.operation": op_name,
        "forze.operation.kind": kind,
    }

    metadata = ctx.inv_ctx.get_metadata()
    if metadata is not None:
        attributes["forze.execution_id"] = str(metadata.execution_id)
        attributes["forze.correlation_id"] = str(metadata.correlation_id)
        if metadata.causation_id is not None:
            attributes["forze.causation_id"] = str(metadata.causation_id)

    tenant = ctx.inv_ctx.get_tenant()
    if tenant is not None:
        attributes["forze.tenant_id"] = str(tenant.tenant_id)

    authn = ctx.inv_ctx.get_authn()
    if authn is not None:
        attributes["forze.principal_id"] = str(authn.principal_id)

    return attributes

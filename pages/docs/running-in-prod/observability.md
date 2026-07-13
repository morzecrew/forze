---
title: Observability
icon: lucide/activity
summary: Spans, metrics, and a distributed trace for every operation and port, via OpenTelemetry
---

Forze is already observability-rich inside — a runtime tracer, a transaction
tracer, structured logs. One call pushes that out to **OpenTelemetry**: a span
and metrics for every operation, tagged with identity and correlation, so your
operations show up in any tracing or metrics backend.

OpenTelemetry is a **core dependency** — the logging layer already uses it — so
this is built in. You only bring an exporter.

## Instrument every operation

Wrap the registry once, before you freeze it:

```python
from forze.application.execution import instrument_operations

registry = instrument_operations(build_orders_registry())
frozen = registry.freeze()
```

That's the whole integration. It emits through the **global** OTel providers by
default; pass `tracer=` / `meter=` to target your own.

## What you get

Every operation produces a **span**, named by its operation key and nested under
whatever span is already active (an incoming HTTP request, say). A failure
records the exception and sets the span status to `ERROR` — then re-raises it
unchanged. The span carries:

| Attribute | Value |
|-----------|-------|
| `forze.operation` / `forze.operation.kind` | the operation key and `query` / `command` |
| `forze.execution_id` / `forze.correlation_id` / `forze.causation_id` | the invocation metadata |
| `forze.tenant_id` / `forze.principal_id` | the bound tenant and principal |

Alongside each span, two **metrics** — `forze.operations` (a counter) and
`forze.operation.duration` (a histogram, in ms) — labelled by operation, kind,
and outcome.

## Trace every outbound call

The operation span tells you *that* an operation ran and how long it took; to see
*where* the time went — which database read, which cache call, which outbound
request — span the ports too. In a hexagonal app every external call is a port, so
opting the resolved ports into a span turns that seam into a complete outbound-I/O
trace under each operation. It rides on the dependency registry, alongside
`instrument_operations` on the operation registry:

```python
from forze.application.execution import DepsRegistry

deps = DepsRegistry.from_modules(postgres_module, redis_module).with_otel_port_spans()
```

Freeze `deps` into the runtime as usual. Every document / cache / queue / search /
HTTP call now opens a `CLIENT` span named `surface.op` (`document_command.create`,
`cache_query.get`), nested under the operation span:

| Attribute | Value |
|-----------|-------|
| `forze.port.domain` / `forze.port.surface` | the port family and its dependency surface |
| `forze.port.op` | the method called — `create`, `get`, `publish` |
| `forze.port.route` / `forze.port.phase` | the specification name and phase, when present |

The name stays low-cardinality (`surface.op`); the specification name rides as the
`route` attribute, never in the name. The span sits **inside** the resilience
policy, so a retried call is one span per attempt and a call shed by an open breaker
or a full bulkhead emits none — the trace shows the work that actually reached the
backend. Failure status follows the exception *kind*: an infrastructure, internal,
or configuration fault reds the span, while a domain failure the caller can handle —
not-found, conflict, precondition — leaves it clean, exactly as a 404 does not red an
HTTP client span. Streaming methods (a cursor, a consume loop, a subscription) pass
through un-spanned — a lone span around a long-lived stream is the wrong shape;
per-message spans belong in the consumer.

For a **log line** per outbound call instead of (or alongside) a span, add
`deps.with_port_logging()`. It logs every port call uniformly — `surface`, `route`,
`op`, and `duration_ms` under `forze.integrations.<domain>` — at `trace` on success
(so it costs nothing in production unless you turn trace on), `debug` on an expected
domain failure, and `warning` with a traceback on an unexpected one.

## Follow the trace across boundaries

Spans and port spans cover one operation inside one process. When the work crosses a
boundary the trace would normally break — so Forze carries the W3C `traceparent`
across the two crossings OpenTelemetry's transport instrumentation can't bridge on
its own.

An **event published now and consumed later** keeps its trace when you set
`propagate_trace=True` on the [outbox integration](../data-events/events-sagas.md):
the publishing operation's trace context is captured as the event is staged, travels
with it through the broker, and the consumer rebuilds it so the handler's spans link
back to the publish span. On a relational backend, add a nullable `traceparent`
column first — exactly as you do for `hlc_ordering`.

An [**outbound HTTP request**](../integrations/http.md) carries it automatically: the
HTTP adapter injects the active trace context into every request (a no-op when no
span is active), honouring your application's configured propagator. Nothing to
enable.

!!! warning "The `traceparent` is untrusted metadata"

    It arrives over the broker or the wire, so it influences trace **parenting
    only** — never identity, tenancy, or deduplication. Treat it as a hint for the
    tracing backend, nothing more.

Inbound HTTP runs the other way: a request *arriving* at FastAPI is picked up by the
standard `opentelemetry-instrumentation-fastapi`, which creates the server span your
operation span nests under — so Forze does not re-extract it.

## Resilience metrics

The [resilience layer](resilience.md) makes decisions worth watching —
retries, rejections, breaker trips, bulkhead backoff. `instrument_resilience`
exports them as **always-on metrics**, independent of any tracing gate, so a
production process with tracing off still reports them:

```python
from forze.application.execution import instrument_resilience

instrument_resilience(ctx.resilience())  # once, when the scope is up
```

| Metric | What it carries |
|--------|-----------------|
| `forze.resilience.events` (counter) | every event — retry attempts, timeouts, rate-limit and bulkhead rejections, breaker transitions — labelled by event, policy, and route |
| `forze.resilience.breaker.state` (gauge) | breaker phase per policy/route: 0 closed, 1 half-open, 2 open |
| `forze.resilience.bulkhead.queue_depth` (gauge) | calls queued behind each bulkhead, sampled at collection |
| `forze.resilience.bulkhead.limit` (gauge) | the current adaptive-bulkhead concurrency limit |
| `forze.resilience.hedge.delay` (gauge) | the effective adaptive hedge delay (P² quantile estimate), in seconds |

Two reading notes: `breaker_open` counts the open transition *and* every
admission shed while open, so its rate tracks shed load; and a breaker that
never tripped reports no state at all — closed by absence.

## Tenant pool metrics

[Routed clients](../identity-tenancy-enc/multi-tenancy.md) keep one connection pool per tenant in a
bounded LRU, and evicting a pool is expensive — the next request rebuilds the
connection from scratch. `instrument_tenant_pools` exports the churn
counters:

```python
from forze.application.execution import instrument_tenant_pools

instrument_tenant_pools({"postgres": pg, "redis": redis})
```

Per client (labelled `forze.client`): `forze.tenancy.pool.size` and
`….capacity` gauges, plus cumulative `….created`, `….disposed`, and
`….evicted_explicit` counters. The alert worth setting: a **sustained
creation rate while `size` sits at `capacity`** means the LRU is thrashing —
hot tenants' pools evicted by cold one-off traffic, each rebuild paying full
connection establishment. The fix is usually a larger `max_cached_tenants`;
the metric tells you when.

## Document L1 metrics

The [in-process L1](../data-events/caching.md#an-in-process-l1-for-hot-documents)
exports its counters the same way:

```python
from forze.application.integrations.document import instrument_document_l1

instrument_document_l1()
```

Per document (labelled `forze.document`): `forze.cache.l1.size` /
`….capacity` gauges and cumulative `….hits` / `….misses` / `….evictions`
counters. The hit rate validates that the L1 is earning its staleness budget,
and **sustained evictions at full capacity with a sagging hit rate** is the
scan-pollution signature — the signal to switch the eviction policy to the
in-box W-TinyLFU store or raise `capacity`.

## Configure logging in one call

`bootstrap_logging` wires the whole logging surface: the framework's own loggers
(`forze.*`, `forze_kits.*`, and the `forze.integrations.*` adapters), any
integration loggers you name, third-party stdlib loggers routed through the same
formatter, and the uncaught-exception hook.

```python
from forze import bootstrap_logging
from forze_postgres import FORZE_POSTGRES_LOGGER_NAMES
from forze_redis import FORZE_REDIS_LOGGER_NAMES

bootstrap_logging(
    level="info",
    render_mode="json",  # "console" for local dev
    logger_names=[FORZE_POSTGRES_LOGGER_NAMES, FORZE_REDIS_LOGGER_NAMES],
    third_party=["uvicorn", "sqlalchemy.engine"],
)
```

`configure_logging` remains the lower-level entry point when you want to wire the
pieces yourself. Either way, pass `otel_config=...` and the active span's
`trace_id` and `span_id` are injected into every log line — because
`instrument_operations` starts that span, your structured logs line up with the
operation trace automatically, no extra wiring.

## Log levels and the verbosity budget

The framework holds one rule: **at `level="info"` it emits almost nothing per
request.** A quiet default is a feature — you turn detail *up* when investigating,
rather than filtering noise *out* in steady state. Each level has a fixed meaning:

| Level | What the framework logs here | Steady-state volume |
|-------|------------------------------|---------------------|
| `trace` | per-row / per-message / per-port-call detail | **none in production** — the trace gate is one integer compare unless you configure `level="trace"` |
| `debug` | per-operation internals, cache hits, dedup skips | opt-in |
| `info` | lifecycle events only: startup, shutdown, saga pivot, relay batch summaries | rare |
| `warning` | degraded-but-continuing: retries exhausted, breaker open, a callback failed | rare, deduped |
| `error` | an unhandled server-side fault (a bug) | should be ~zero |
| `critical` | data loss or an unrecoverable condition | ~zero |

A **domain failure is never an error.** A validation, not-found, conflict, or
precondition outcome is the application working as designed — it logs at `debug`
or not at all, exactly as it leaves an [HTTP span clean](#trace-every-outbound-call).
Only an unhandled fault reaches `error`.

## Tame high-volume logs

Two per-event controls collapse the events that would otherwise flood a log,
without dropping the first occurrence you actually need to see. They are a no-op
for events that don't opt in:

```python
# keep 1 in 100 of a uniformly high-volume event
logger.debug("cache miss", _sample=100)

# emit a flapping condition at most once per window (default 60s)
logger.warning("upstream degraded", _dedup_key="upstream-degraded")
```

`_sample=N` keeps one in every `N` events sharing the same logger and message;
`_dedup_key` emits at most one event per key per window (`_dedup_window=` overrides
the seconds). The control keys are stripped before rendering. This is on by default
(`configure_logging(enable_sampling=True)`).

For per-request access logs — the largest steady-state source — the FastAPI and MCP
middlewares are quiet by default: successful requests are sampled 1-in-N and error
responses are always logged. The FastAPI middleware additionally drops health and
readiness probe *paths* (`DEFAULT_HEALTH_PATHS`); MCP messages have no such path, so
its default sampler applies no path exclusion. Configure either with
`access_log=AccessLogSampler(...)`: `mode="full"` logs every request, `mode="off"`
disables them, and `sample_rate` / `exclude` tune the rate and the excluded subjects
(request paths for FastAPI, method names for MCP).

## Sensitive data is scrubbed

Log output runs through a redaction pass that masks sensitive **keys** (password,
token, secret, api-key, cookie, authorization, …) and secret-shaped **values**
(bearer tokens, JWTs, connection DSNs) in both extras and the message text. Extend
it for deployment-specific patterns once at startup:

```python
from forze.base.scrubbing.policy import register_sensitive_patterns

register_sensitive_patterns(keys=["x_internal_token"])
```

Disable with `configure_logging(sanitize_logs=False)` only when you fully control
the sink and its retention.

## Naming: where a log line comes from

Every logger is namespaced so you can raise or lower detail per area without
touching the rest. Core framework logs sit under `forze.*` (`forze.application`,
`forze.domain`); pre-built wiring under `forze_kits.*`; each integration under its
own `forze_<name>.*` (`forze_postgres.adapters`, `forze_redis.kernel`). Generic
adapter machinery shared across integrations logs under `forze.integrations.<domain>`
(`forze.integrations.cache`, `forze.integrations.document`) — filter the whole group
with `forze.integrations.*`, or a single domain on its own.

## Bring your own exporter

Forze emits to the global tracer and meter providers; your application owns the
SDK and exporter choice — OTLP, Prometheus, console, whatever your backend
speaks. The OTel API and SDK ship with Forze, so you add only the exporter
package and the few lines of standard OTel setup that point the providers at it.

The signals you watch in production are also what you assert against before
shipping — see [Testing](../testing/overview.md).

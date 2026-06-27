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

## Logs correlate for free

`configure_logging(otel_config=...)` injects the active span's `trace_id` and
`span_id` into every log line. Because `instrument_operations` is what starts the
span, your structured logs line up with the operation trace automatically — no
extra wiring.

## Bring your own exporter

Forze emits to the global tracer and meter providers; your application owns the
SDK and exporter choice — OTLP, Prometheus, console, whatever your backend
speaks. The OTel API and SDK ship with Forze, so you add only the exporter
package and the few lines of standard OTel setup that point the providers at it.

The signals you watch in production are also what you assert against before
shipping — see [Testing](../testing/overview.md).

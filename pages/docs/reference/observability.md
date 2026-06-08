# Observability (OpenTelemetry)

Forze is observability-rich internally (runtime tracer, tx tracer, structured logs).
`instrument_operations` pushes that out to **OpenTelemetry**: a **span + metrics for every
operation**, so Forze operations appear in your tracing/metrics backend with latency,
outcome, and identity attributes.

OpenTelemetry is a **core dependency** (the logging layer already uses it), so this is
built in — no extra to install. Your application owns the SDK + exporter choice (OTLP,
Prometheus, console, …).

## Instrument the registry

Call `instrument_operations` once, before freezing — it wraps every registered operation:

```python
from forze.application.execution import instrument_operations

registry = build_my_registry()
registry = instrument_operations(registry)   # spans + metrics on all ops
frozen = registry.freeze()
```

It emits via the **global** OTel providers by default (pass `tracer=`/`meter=` to override).

Each operation produces:

- a **span** named by the operation key, with attributes `forze.operation`,
  `forze.operation.kind` (`query`/`command`), `forze.execution_id`, `forze.correlation_id`,
  `forze.causation_id`, `forze.tenant_id`, `forze.principal_id`. A failure records the
  exception and sets span status `ERROR` (the exception is re-raised unchanged). The span
  nests under any active OTel context (e.g. an incoming HTTP request span).
- two **metrics**, labelled by operation / kind / outcome:
  `forze.operations` (counter) and `forze.operation.duration` (histogram, ms).

## Configure the SDK (your app)

Forze emits to the providers; configure them + an exporter yourself, e.g. OTLP:

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

tp = TracerProvider()
tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tp)

metrics.set_meter_provider(
    MeterProvider(metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())])
)
```

The OTel **API + SDK ship with Forze**; you add only your chosen **exporter** package.

## Log ↔ trace correlation (free)

`configure_logging(otel_config=...)` already injects `trace_id`/`span_id` of the active span
into every log line. With `instrument_operations` starting the span, your structured logs
correlate to the operation trace automatically — no extra wiring.

## Not covered (yet)

- **Transaction-scope child spans** — the live `TxTracer` seam exists but injecting an OTel
  tx tracer needs a deps-seam change; deferred.
- **Per-port-op spans** — the runtime tracer is per-port-call (high cardinality), so it is
  intentionally not exported as spans.

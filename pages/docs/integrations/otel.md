# OpenTelemetry (observability)

`forze_otel` exports a **span + metrics for every operation** to OpenTelemetry, so Forze
operations appear in your tracing/metrics backend with latency, outcome, and identity
attributes. Forze is observability-rich internally (runtime tracer, tx tracer, structured
logs); this is the seam that pushes it out to an APM.

Install the extra:

```bash
pip install "forze[otel]"
```

`forze_otel` depends only on `opentelemetry-api` — your application owns the SDK and exporter
choice (OTLP, Prometheus, console, …).

## Instrument the registry

Call `instrument_operations` once, before freezing — it wraps every registered operation:

```python
from forze_otel import instrument_operations

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

`forze_otel` only emits; configure the providers + exporter yourself, e.g. OTLP:

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

## Log ↔ trace correlation (free)

`configure_logging(otel_config=...)` already injects `trace_id`/`span_id` of the active span
into every log line. With `instrument_operations` starting the span, your structured logs
correlate to the operation trace automatically — no extra wiring.

## Not covered (yet)

- **Transaction-scope child spans** — the live `TxTracer` seam exists but injecting an OTel
  tx tracer needs a deps-seam change; deferred.
- **Per-port-op spans** — the runtime tracer is per-port-call (high cardinality), so it is
  intentionally not exported as spans.

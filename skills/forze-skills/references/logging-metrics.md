# Logging and metrics

Structured logging bound to the call context, the `instrument_*` calls that export metrics, and one scrape for every worker of a pre-fork server. The exception model itself is [errors](errors.md).

## Logging

Configure logging once at application startup — `bootstrap_logging` wires the framework loggers, named integration loggers, third-party stdlib loggers, and the uncaught-exception hook in one call:

```python
from forze import bootstrap_logging

bootstrap_logging(
    level="info",
    render_mode="json",  # "console" for local dev
    third_party=["uvicorn", "fastapi"],
)
```

Pass extra integration logger roots via `logger_names=[...]` when an integration's loggers are not already covered. Naming loggers does not narrow redaction: unlisted loggers that propagate still print at `warning` and above (or a higher configured `level`) through a scrubbing root handler, on the configured stream. Uvicorn's `uvicorn` and `uvicorn.access` loggers do not propagate: name both in `third_party`. `redact_unnamed=False` turns that off: every logger outside `logger_names` then prints unredacted, and a warning says so. It is only for an app that manages the root logger and its redaction itself.

`configure_logging` / `attach_foreign_loggers` (from `forze.base.logging`) remain the lower-level entry points when you want to wire the pieces yourself. Pass `otel_config=...` to inject the active span's `trace_id` / `span_id` into every log line.

For console development output, tune traceback depth with `ForzeConsoleRenderer(max_traceback_frames=0)` (show all frames) or `traceback_supress=["uvicorn", "starlette"]`.

Log event fields are scrubbed by default (`sanitize_logs=True`; Logfire-aligned log string rules when `text_scrub=True`, uniform `**********` placeholder). API/error payloads use `forze.base.scrubbing.sanitize(..., context="egress")`, not the log context.

```python
from forze.base.scrubbing import dump_for_error_context, sanitize_pydantic_errors
```

Use `get_logger` in modules and bind stable context:

```python
from forze import get_logger

logger = get_logger("app.projects").bind(component="projects")
logger.info("project_created", project_id=str(project_id))
```

`ExecutionContext.inv_ctx.bind(...)` binds `execution_id`, `correlation_id`, optional `causation_id`, `invocation_surface`, `principal_id`, and `tenant_id` into logging context. `invocation_surface` names the boundary that minted the metadata (`http`, `websocket`, `socketio`, `mcp`, `cli`, `inbox`, `temporal`, `inngest`, `durable`, or `unknown` when none did); operation spans carry it as `forze.invocation_surface`. A boundary of your own passes a literal token: `mint_invocation_metadata(surface="grpc")` from `forze.application.execution.ingress`, matching `^[a-z][a-z0-9_]{0,31}$`.

## Instrument at assembly

`bootstrap_telemetry` installs the OpenTelemetry SDK and emits nothing. Metrics come from
opt-in `instrument_*` calls, each made once per process at assembly time. They all emit
through the global OTel meter unless you pass `meter=`:

```python
from forze.application.execution import (
    instrument_connection_pools,
    instrument_operations,
    instrument_process,
    instrument_resilience,
)
from forze_http import HttpClient, instrument_http_clients
from forze_postgres import PostgresClient
from forze_redis import RedisClient

postgres_client = PostgresClient()
redis_client = RedisClient()
payments = HttpClient()

registry = instrument_operations(build_registry())  # returns a new registry: before .freeze()
instrument_connection_pools({"postgres": postgres_client, "redis": redis_client})
instrument_http_clients({"payments": payments})
instrument_process()  # once per process: in every worker of a pre-fork server

# once the runtime scope is up
instrument_resilience(ctx.resilience())
```

| Call | Exports | Alert on |
|---|---|---|
| `instrument_operations` | a span per operation, `forze.operations`, `forze.operation.duration` (ms) by operation, kind and outcome | error rate and latency per operation |
| `instrument_resilience` | `forze.resilience.events`, breaker state, bulkhead `queue_depth` / `in_use` / `limit`, adaptive hedge delay | breaker open; `in_use / limit` near 1; `in_use` that stays up when traffic stops (a slot never released) |
| `instrument_connection_pools` | `forze.connection_pool.in_use` / `idle` / `max` / `waiting`, labelled `forze.client` | `in_use` pinned at `max`: the pool is exhausted |
| `instrument_http_clients` | `forze.http.client.in_flight` per `HttpClient` | a count that climbs under steady load and never falls: a `stream` or `send` block that never exits |
| `instrument_process` | `process.cpu.time` per `cpu.mode`, `process.memory.usage` | memory or CPU creep under a steady load |

- **Pools:** `PostgresClient`, `RedisClient` and `MongoClient` report their pool. `waiting`
  is Postgres only, and `max` is absent for a pool with no cap. A client before
  `initialize` or after `close` reports nothing.
- **Routed clients are refused** (`configuration`) by both `instrument_connection_pools`
  and `instrument_http_clients`: a routed client keeps one client per tenant, and a sum
  would hide the saturated one. Its pool cache (size and churn) is `instrument_tenant_pools`.
- **Process:** `process.memory.usage` is Linux only (resident memory from
  `/proc/self/statm`); elsewhere it is not registered. Prometheus renders the pair as
  `process_cpu_time_seconds_total` and `process_memory_usage_bytes`.

Other planes ship their own call beside the wiring that owns them, for example
`instrument_tenant_pools` and `instrument_crypto` (`forze.application.execution`),
`instrument_document_l1` (`forze.application.integrations.document`) and the realtime hub,
gateway and mailbox calls in [realtime transports](realtime-transports.md). The hub,
gateway, backplane and connections calls take `channel=`: call each once per channel, all
with the same `meter=` or none, and each channel exports its own series.
[The metric catalog](https://morzecrew.github.io/forze/latest/reference/metrics/) lists every
series. See [resilience and deadlines](resilience.md) for the policies behind the resilience
events.

## One scrape for every worker

Under `uvicorn --workers N` or gunicorn, a plain Prometheus route answers from whichever
worker took the connection. Give each worker a snapshot instead, and the route serves the
union, each series labelled `service_instance_id`:

```python
from pathlib import Path

from fastapi import APIRouter, FastAPI

from forze.application.execution import ExecutionRuntime, build_runtime, instrument_process
from forze.base.telemetry import bootstrap_telemetry
from forze_fastapi import runtime_lifespan
from forze_fastapi.routes import MetricsSnapshots, attach_metrics_route, worker_metrics_reader


def create_app() -> FastAPI:  # uvicorn orders.app:create_app --factory --workers 4
    reader = worker_metrics_reader()
    snapshots = MetricsSnapshots(directory=Path("/dev/shm/forze-metrics"))

    def build() -> ExecutionRuntime:  # in each worker, after the fork
        bootstrap_telemetry(
            service_name="orders-api", exporter="none", extra_metric_readers=[reader]
        )
        instrument_process()
        return build_runtime(lifecycle_steps=snapshots.writer_step(reader))

    router = APIRouter()
    attach_metrics_route(router, reader, snapshots=snapshots)

    app = FastAPI(lifespan=runtime_lifespan(build=build))
    app.include_router(router)

    return app
```

- `MetricsSnapshots` is keyword-only: `directory` (required, one directory per worker
  group, a tmpfs path), `interval` (10 s), `ttl` (three intervals; below two is refused).
- `worker_metrics_reader()` needs `opentelemetry-exporter-prometheus` 0.66b0 or newer,
  installed by you: the `observability` extra does not bring it.
- Sum on the query side (`sum without (service_instance_id) (...)`) and alert on
  `count(forze_metrics_snapshot_age_seconds)` falling below the worker count: Prometheus's
  `up` stays 1 while one worker is missing. The route answers `503` when it has nothing to
  serve.
- Snapshots join the workers of one host. Across hosts, or for processes that are not web
  servers, push OTLP to one collector instead.

## Anti-patterns

- **Logging secrets or raw credentials** — log logical refs and ids only.
- **Binding log context manually in handlers** — bind request identity at the boundary.
- **Branching on `invocation_surface`** — it is attribution, not authority: any code can mint metadata with any valid token. Restrict per-surface access with the surface's exposure allowlist.
- **Reading the surface from the request** — pass a literal; never a header, frame or message value, and never a token naming a person, tenant or customer.
- **A plain `/metrics` route under `--workers N`** — consecutive scrapes reach different workers, so counters interleave, `rate()` sees phantom resets, and one scrape never shows every worker's gauges. Serve `MetricsSnapshots`, or push OTLP.
- **`bootstrap_telemetry` at import** — under gunicorn `--preload` every worker inherits one `service.instance.id`, and the snapshot route withholds them all. Call it in the per-worker build.
- **A routed client in `instrument_connection_pools` or `instrument_http_clients`** — refused at assembly; export its pool cache with `instrument_tenant_pools`.
- **A tenant or principal id as a metric label** — one series per tenant, per metric, per process, forever. Ids belong in span attributes and log fields, which already carry them.
- **Mixing meters across realtime channels** — a call passing the SDK's `forze` meter after a default call made before the SDK was configured registers under a second entry, and the SDK drops the later call's channels. Pass the same `meter=` to every call, or none.

## Reference

- [Base layer (errors and logging)](https://morzecrew.github.io/forze/latest/reference/errors/)
- [Observability](https://morzecrew.github.io/forze/latest/running-in-prod/observability/)
- [Metric catalog](https://morzecrew.github.io/forze/latest/reference/metrics/)
- [Several workers, one scrape](https://morzecrew.github.io/forze/latest/running-in-prod/grafana-stack/#several-workers-one-scrape)
- [Runtime settings](connection-settings.md) for `bootstrap_telemetry` arguments from the environment

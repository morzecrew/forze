# Logging and metrics

Structured logging bound to the call context, and the operation and resilience metrics a frozen registry emits. The exception model itself is [errors](errors.md).

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

## Operation and resilience metrics

`instrument_operations(registry)` (before freeze) emits an OpenTelemetry span plus `forze.operations` / `forze.operation.duration` metrics per operation. `instrument_resilience(ctx.resilience())` (once the runtime scope is up) exports resilience events — retries, rejections, breaker state, bulkhead queue depth/limit — as always-on metrics, independent of tracing. Both emit via the global OTel providers; the app brings the exporter. See [resilience and deadlines](resilience.md) for the policies behind those events.

## Anti-patterns

- **Logging secrets or raw credentials** — log logical refs and ids only.
- **Binding log context manually in handlers** — bind request identity at the boundary.
- **Branching on `invocation_surface`** — it is attribution, not authority: any code can mint metadata with any valid token. Restrict per-surface access with the surface's exposure allowlist.
- **Reading the surface from the request** — pass a literal; never a header, frame or message value, and never a token naming a person, tenant or customer.

## Reference

- [Base layer (errors and logging)](https://morzecrew.github.io/forze/latest/reference/errors/)
- [Observability](https://morzecrew.github.io/forze/latest/running-in-prod/observability/)

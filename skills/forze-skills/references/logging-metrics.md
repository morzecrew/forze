# Logging and metrics

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

Pass extra integration logger roots via `logger_names=[...]` when an integration's loggers are not already covered.

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

`ExecutionContext.inv_ctx.bind(...)` binds `execution_id`, `correlation_id`, optional `causation_id`, `principal_id`, and `tenant_id` into logging context.

## Operation and resilience metrics

`instrument_operations(registry)` (before freeze) emits an OpenTelemetry span plus `forze.operations` / `forze.operation.duration` metrics per operation. `instrument_resilience(ctx.resilience())` (once the runtime scope is up) exports resilience events — retries, rejections, breaker state, bulkhead queue depth/limit — as always-on metrics, independent of tracing. Both emit via the global OTel providers; the app brings the exporter. See [resilience and deadlines](resilience.md) for the policies behind those events.

## Anti-patterns

- **Logging secrets or raw credentials** — log logical refs and ids only.
- **Binding log context manually in handlers** — bind request identity at the boundary.

## Reference

- [Base layer (errors and logging)](https://morzecrew.github.io/forze/latest/reference/errors/)
- [Observability](https://morzecrew.github.io/forze/latest/running-in-prod/observability/)

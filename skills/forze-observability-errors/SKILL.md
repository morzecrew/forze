---
name: forze-observability-errors
description: >-
  Applies Forze structured errors, handled decorators, logging configuration,
  call-context binding, OpenTelemetry context injection, and FastAPI error
  mapping. Use when adding error handling, diagnostics, or logging.
---

# Forze observability and errors

Use when surfacing domain/application failures, mapping infrastructure exceptions, or adding structured diagnostics.

## Error model

All expected domain/application failures should derive from `CoreError`.

| Error | HTTP mapping in FastAPI | Use when |
|-------|-------------------------|----------|
| `NotFoundError` | 404 | resource is missing |
| `ConflictError` | 409 | duplicate key, revision conflict |
| `ValidationError` | 422 | invalid user or external input |
| `DomainError` | 400 | domain invariant violation |
| `InvalidOperationError` | 400 | application invariant violation |
| `AuthenticationError` | 401 | authentication failed |
| `AuthorizationError` | 403 | permission denied |
| `InfrastructureError` | 500 | backend/service failure |

Set stable `code` values for machine handling and use `details` for structured context.

```python
from forze.base.errors import ConflictError

raise ConflictError(
    "Project slug already exists",
    code="project_slug_conflict",
    details={"slug": slug},
)
```

## Adapter exception mapping

Use `@handled(...)` on adapter methods to convert provider exceptions into `CoreError` subclasses. Let existing `CoreError` values pass through.

```python
from forze.base.errors import ConflictError, CoreError, InfrastructureError, error_handler, handled


@error_handler
def pg_errors(exc: Exception, op: str, **kwargs) -> CoreError:
    if isinstance(exc, UniqueViolation):
        return ConflictError(f"Duplicate during {op}", code="duplicate")
    return InfrastructureError(f"Postgres failed during {op}")


class ProjectAdapter:
    @handled(pg_errors)
    async def create(self, dto: CreateProjectCmd) -> ProjectRead:
        ...
```

## Logging

Configure structlog once at application startup.

```python
from forze.base.logging import attach_foreign_loggers, configure_logging, ForzeConsoleRenderer

configure_logging(level="info", render_mode="json", logger_names=["forze"])
attach_foreign_loggers(["uvicorn", "fastapi"], render_mode="json")
```

For console development output, tune traceback depth with `ForzeConsoleRenderer(max_traceback_frames=0)` (show all frames) or `traceback_supress=["uvicorn", "starlette"]`.

Log event fields are scrubbed by default (`sanitize_logs=True`; Logfire-aligned log string rules when `text_scrub=True`, uniform `**********` placeholder). API/error payloads use `forze.base.scrubbing.sanitize(..., context="egress")`, not the log context.

```python
from forze.base.scrubbing import dump_for_error_context, sanitize_pydantic_errors
```

Use `Logger` instances in modules and bind stable context:

```python
from forze.base.logging import Logger

logger = Logger("app.projects").bind(component="projects")
logger.info("project_created", project_id=str(project_id))
```

`ExecutionContext.inv_ctx.bind(...)` binds `execution_id`, `correlation_id`, optional `causation_id`, `principal_id`, and `tenant_id` into logging context.

## FastAPI mapping

Call `register_exception_handlers(app)` once. It converts `CoreError` to JSON and emits the error code in `X-Error-Code`.

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
```

## Anti-patterns

1. **Raising raw provider exceptions from adapters** — map them to `CoreError`.
2. **Using plain strings as error categories** — use `code` and `details`.
3. **Logging secrets or raw credentials** — log logical refs and ids only.
4. **Binding log context manually in handlers** — bind request identity at the boundary.
5. **Catching `CoreError` only to re-raise it unchanged** — let middleware/presentation layers handle it.

## Reference

- [`pages/docs/core-package/base-layer.md`](../../pages/docs/core-package/base-layer.md)
- [`src/forze/base/errors.py`](../../src/forze/base/errors.py)
- [`src/forze/base/logging`](../../src/forze/base/logging)
- [`src/forze_fastapi/exceptions.py`](../../src/forze_fastapi/exceptions.py)

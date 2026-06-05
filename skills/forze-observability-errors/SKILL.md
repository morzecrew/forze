---
name: forze-observability-errors
description: >-
  Applies Forze structured errors (CoreException / exc factories), logging
  configuration, call-context binding, OpenTelemetry context injection, and
  FastAPI error mapping. Use when adding error handling, diagnostics, or logging.
---

# Forze observability and errors

Use when surfacing domain/application failures, mapping infrastructure exceptions, or adding structured diagnostics.

## Error model

Raise expected domain/application failures as `CoreException`, built through the `exc` factory. Each kind maps to an HTTP status in the FastAPI integration.

| Factory | Kind | HTTP status | Use when |
|---------|------|-------------|----------|
| `exc.not_found(...)` | `not_found` | 404 | resource is missing |
| `exc.conflict(...)` | `conflict` | 409 | duplicate key, revision conflict |
| `exc.validation(...)` | `validation` | 422 | invalid user or external input |
| `exc.domain(...)` | `domain` | 400 | domain invariant violation |
| `exc.precondition(...)` | `precondition` | 400 | precondition not met |
| `exc.authentication(...)` | `authentication` | 401 | authentication failed |
| `exc.authorization(...)` | `authorization` | 403 | permission denied |
| `exc.infrastructure(...)` | `infrastructure` | 500 | backend/service failure |
| `exc.internal(...)` / `exc.concurrency(...)` / `exc.configuration(...)` | — | 500 | unexpected/internal failures |

Each factory takes `(summary, *, code=None, details=None)`. Set a stable `code` for machine handling and use `details` for structured context.

```python
from forze.base.exceptions import exc

raise exc.conflict(
    "Project slug already exists",
    code="project_slug_conflict",
    details={"slug": slug},
)
```

## Adapter exception mapping

Shipped `forze_*` adapters already translate common provider errors into `CoreException`. When you implement a **custom adapter** in your application, catch provider exceptions and raise the matching `exc.*` kind; let any existing `CoreException` propagate unchanged.

```python
from forze.base.exceptions import CoreException, exc


class ProjectAdapter:
    async def create(self, dto: CreateProjectCmd) -> ProjectRead:
        try:
            ...
        except CoreException:
            raise
        except UniqueViolation as e:
            raise exc.conflict("Duplicate project", code="duplicate") from e
        except Exception as e:
            raise exc.infrastructure("Postgres create failed") from e
```

For declarative mapping (what shipped adapters use internally), Forze also exposes `ExceptionInterceptor` and `ChainExceptionMapper` from `forze.base.exceptions`.

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

Call `register_exception_handlers(app)` once. It converts `CoreException` to a JSON response (and maps unhandled exceptions to 500).

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
```

## Anti-patterns

1. **Raising raw provider exceptions from adapters** — map them to `CoreException`.
2. **Using plain strings as error categories** — use `code` and `details`.
3. **Logging secrets or raw credentials** — log logical refs and ids only.
4. **Binding log context manually in handlers** — bind request identity at the boundary.
5. **Catching `CoreException` only to re-raise it unchanged** — let middleware/presentation layers handle it.

## Reference

- [Base layer (errors and logging)](https://morzecrew.github.io/forze/docs/core-package/base-layer/)
- [FastAPI integration](https://morzecrew.github.io/forze/docs/integrations/fastapi/)

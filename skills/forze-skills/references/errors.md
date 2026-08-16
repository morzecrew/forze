# Errors

The exception model: `CoreException` and its kinds, mapping an adapter's provider exceptions onto it, and what a FastAPI client finally sees. Logging and metrics are [logging and metrics](logging-metrics.md).

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
| `exc.throttled(...)` | `throttled` | 429 | rate limit / draining rejection (retryable) |
| `exc.timeout(...)` | `timeout` | 504 | invocation time budget spent (non-retryable) |
| `exc.concurrency(...)` | `concurrency` | 409 | optimistic-lock / serialization race (retryable) |
| `exc.internal(...)` / `exc.configuration(...)` | — | 500 | unexpected/internal failures |

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

## FastAPI mapping

Call `register_exception_handlers(app)` once. It converts `CoreException` to a JSON response (and maps unhandled exceptions to 500).

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
```

## Anti-patterns

- **Raising raw provider exceptions from adapters** — map them to `CoreException`.
- **Using plain strings as error categories** — use `code` and `details`.
- **Catching `CoreException` only to re-raise it unchanged** — let middleware/presentation layers handle it.

## Reference

- [Base layer (errors and logging)](https://morzecrew.github.io/forze/latest/reference/errors/)

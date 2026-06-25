---
title: Errors
icon: lucide/triangle-alert
summary: The exception kinds, their egress policy, and how to raise them
---

The narrative is in [Errors & failures](../writing-operation/errors.md); this is the
exhaustive surface — every `ExceptionKind`, how to raise it, and the egress
policy each carries.

## Raising

Every failure is a `CoreException`, raised through the `exc` factory — the kind is
the method:

```python
from forze.base.exceptions import exc

raise exc.domain("A shipped order is final.")
raise exc.conflict("Email already registered.", code="email_taken")
```

`exc.<kind>(summary, *, code=None, details=None)` — `code` defaults to
`core.<kind>`. A `CoreException` carries `kind`, `summary`, `code`, and optional
`details`.

## The kinds

Each kind has an **egress policy** with two flags: `expose_details` (are details
safe to return to a caller?) and `retryable` (is the failure transient?). Only
the retryable kinds may appear in a [resilience](../running-in-prod/resilience.md) retry
policy.

| Kind | Meaning | Exposes details | Retryable | HTTP status | Default code |
|------|---------|:---:|:---:|:---:|--------------|
| `validation` | malformed input | ✅ | — | 422 | `core.validation` |
| `domain` | a business rule was violated | ✅ | — | 400 | `core.domain` |
| `precondition` | a required state wasn't met (e.g. stale revision, a bad query field) | ✅ | — | 400 | `core.precondition` |
| `conflict` | the change collides with current state | ✅ | — | 409 | `core.conflict` |
| `concurrency` | transient contention | ✅ | ✅ | 409 | `core.concurrency` |
| `not_found` | the target doesn't exist | ✅ | — | 404 | `core.not_found` |
| `authentication` | who is calling | — | — | 401 | `core.authentication` |
| `authorization` | what they may do | — | — | 403 | `core.authorization` |
| `configuration` | the app is wired wrong | — | — | 500 | `core.configuration` |
| `infrastructure` | a backing system failed | — | ✅ | 500 | `core.infrastructure` |
| `throttled` | a rate limit rejected the call | — | ✅ | 429 | `core.throttled` |
| `timeout` | the invocation's [time budget](../running-in-prod/deadlines.md) ran out | — | — | 504 | `core.timeout` |
| `internal` | an unexpected bug | — | — | 500 | `core.internal` |

## Outcomes

Handlers return a result or raise. Where code needs the *outcome* rather than a
raise — a `finally_` stage hook — it receives an `Outcome`: `Success(value)` or
`Failure(exc)`, from `forze.application.contracts.execution`.

## At the edge

Core owns the canonical kind→status mapping (the column above, via
`http_status_for_kind`), but applies it at no transport itself. The
[FastAPI](../integrations/fastapi.md) exception handlers turn a `CoreException`
into a response — the status from the kind, the `code` on an error-code header,
and details exposed only when `expose_details` is set; kinds with no status of
their own (`configuration`, `infrastructure`, `internal`) map to `500`.

---
title: Errors
icon: lucide/triangle-alert
summary: The exception kinds, their egress policy, and how to raise them
---

The narrative is in [Errors & failures](../in-depth/errors.md); this is the
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
the retryable kinds may appear in a [resilience](../in-depth/resilience.md) retry
policy.

| Kind | Meaning | Exposes details | Retryable | Default code |
|------|---------|:---:|:---:|--------------|
| `validation` | malformed input | ✅ | — | `core.validation` |
| `domain` | a business rule was violated | ✅ | — | `core.domain` |
| `precondition` | a required state wasn't met (e.g. stale revision) | ✅ | — | `core.precondition` |
| `conflict` | the change collides with current state | ✅ | — | `core.conflict` |
| `concurrency` | transient contention | ✅ | ✅ | `core.concurrency` |
| `not_found` | the target doesn't exist | ✅ | — | `core.not_found` |
| `authentication` | who is calling | — | — | `core.authentication` |
| `authorization` | what they may do | — | — | `core.authorization` |
| `configuration` | the app is wired wrong | — | — | `core.configuration` |
| `infrastructure` | a backing system failed | — | ✅ | `core.infrastructure` |
| `throttled` | a rate limit rejected the call | — | ✅ | `core.throttled` |
| `timeout` | the invocation's [time budget](../in-depth/deadlines.md) ran out | — | — | `core.timeout` |
| `internal` | an unexpected bug | — | — | `core.internal` |

## Outcomes

Handlers return a result or raise. Where code needs the *outcome* rather than a
raise — a `finally_` stage hook — it receives an `Outcome`: `Success(value)` or
`Failure(exc)`, from `forze.application.contracts.execution`.

## At the edge

Core never picks an HTTP status. The [FastAPI](../integrations/fastapi.md)
exception handlers map a `CoreException` to a response — the status from the kind,
the `code` on an error-code header, and details exposed only when
`expose_details` is set.

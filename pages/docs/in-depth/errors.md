---
title: Errors & failures
icon: lucide/triangle-alert
summary: How Forze represents failure, how it propagates, and how it reaches the edge
---

In Forze a failure is a typed value with a **kind**. A handler (or a stage)
raises a `CoreException`; its kind says *what* went wrong; the operation aborts
and its transaction rolls back; and the edge turns it into a response. One model,
end to end.

## The failure taxonomy

Raise a failure through the `exc` factory — the kind is the method:

```python
from forze.base.exceptions import exc

raise exc.domain("A shipped order is final.")
raise exc.conflict("Email already registered.", code="email_taken")
```

Every `CoreException` carries a `kind`, a human `summary`, a machine `code`
(defaults to `core.<kind>`), and optional `details`. The kinds:

| Kind | Use it for |
|------|------------|
| `validation` | malformed input |
| `domain` | a business rule was violated |
| `precondition` | a required state wasn't met (e.g. a stale revision) |
| `conflict` | the change collides with current state |
| `concurrency` | a transient, retryable contention failure |
| `not_found` | the target doesn't exist |
| `authentication` / `authorization` | who they are / what they may do |
| `configuration` | the app is wired wrong (a startup-time mistake) |
| `infrastructure` | a backing system failed (transient) |
| `throttled` | a rate limit rejected the call (transient — capacity refills) |
| `internal` | an unexpected bug |

## Raising aborts the operation

A raised `CoreException` propagates straight out: the operation stops, the
transaction **rolls back** (no partial writes), and deferred after-commit work
never runs. Stage hooks observe the failure — `finally_` and `on_failure` get to
clean up — but they don't swallow it; the kind reaches the caller intact.

## Outcomes, when you need them

Handlers return their result or raise. When code needs the *outcome* rather than
a raise — a `finally_` hook, say — it receives an `Outcome`: `Success(value)` or
`Failure(exc)`.

## Two flags every kind carries

Beyond its meaning, each kind has an **egress policy** with two booleans:

- **`expose_details`** — are the details safe to show a caller? (`internal`,
  `authentication`, `authorization`, `infrastructure`, and `throttled` say no.)
- **`retryable`** — is this transient? Only **`concurrency`**,
  **`infrastructure`**, and **`throttled`** are. This flag is what the
  [resilience](resilience.md) retry policies key on — you can only retry a kind
  that declares itself retryable.

## At the edge

Core stays transport-neutral — it never picks an HTTP status. The
[FastAPI integration](../integrations/fastapi.md) turns a `CoreException` into a response,
exposing details only when the kind's policy allows and logging the rest
server-side.

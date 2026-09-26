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

`exc.<kind>(summary, *, code=None, details=None, resource_type=None)` — `code`
defaults to `core.<kind>`. A `CoreException` carries `kind`, `summary`, `code`, and
optional `details` and `resource_type` (see [Non-disclosing denials](#non-disclosing-denials)).

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

## Non-disclosing denials

A 403 for a row you may not see and a 404 for a row that does not exist tell a caller which
ids exist. A `DenialPosture` closes that: for the resource types it names, a denial and a
not-found render as one response — `404`, code `core.not_found`, detail `"Not found"`, no
context — on every transport, because the collapse happens in the shared envelope.

```python
from forze.base.exceptions import DenialPosture

runtime = build_runtime(
    ...,
    denial_posture=DenialPosture(mode="non_disclosing", resource_types={"notes", "invoices"}),
)
```

The runtime sets the posture for the whole process while its scope is entered. What is covered:

- a denial from the authz before-hook when the hook has a `resource_factory` — it carries the
  resource's `resource_type`;
- a not-found from a document port — it carries the spec name, so name your authz resource
  types after your document specs;
- anything you raise with `resource_type=` yourself. A handler's own
  `exc.not_found("Note 7 not found")` without it renders as written, and that difference is
  itself the leak.

A denial about no particular resource — the hook without a `resource_factory`, a policy scope
that denies a whole list — stays a 403: it says nothing about any row. The server-side
exception keeps its real kind and code, so logs and metrics still tell a denial from a miss.

For the row itself, read it with `owned_by` (see the [document port](contracts/document.md#fetch-one)):
a foreign row is then not found by the read, not by a check after it.

!!! warning "What this does and does not close"
    It closes the **response-shape** oracle. It does not close timing — refusing a row that
    exists can still take a different time than missing one that does not — and it does not
    stop enumeration by volume, which is throttling's job. It is **off** by default, and turning
    it on changes what clients see: any client that tells 403 from 404 will break. Adopt it per
    resource type, and list the types rather than the endpoints, so every route over a type is
    covered at once.

To check it holds across a workload, add `denial_bodies_identical("notes.read", ...)` from
`forze_dst.invariants` to a [simulation](../dst/invariants.md): it fails when one operation refuses
with more than one rendered response.

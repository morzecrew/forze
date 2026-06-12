---
title: Deadlines
icon: lucide/timer
summary: Time budgets for operations — declared once, enforced everywhere, propagated downstream
---

An operation without a time budget can outlive the caller that wanted it: the
client gave up seconds ago, but the handler is still holding a connection,
a bulkhead slot, and a transaction. Forze uses **gRPC-style deadlines**: a
budget is bound once at operation entry, and everything downstream — hooks,
the transaction, dispatched operations, resilience strategies, outbound calls —
inherits it for free.

## Declare the budget on the plan

A deadline is a property of the **operation**, not of the route or the caller.
Declare it where the operation is composed:

```python
registry = (
    registry
    .bind("orders.create")
    .with_deadline(timedelta(seconds=5))
    .finish()
    .freeze()
)
```

Patch mode sets a default across many operations at once —
`registry.patch(selector).with_deadline(timedelta(seconds=10)).finish()` gives
every matched operation a budget in one line.

Merging is **restrictive**: across patches, explicit plans, and any
caller-bound deadline, the *tightest* budget wins. A layer can shorten an
operation's budget, never extend it — to give one operation more room than a
broad patch default, narrow the patch selector instead.

## Bind a caller budget at the boundary

A boundary (HTTP middleware, a worker loop) may additionally bind the caller's
own budget:

```python
from forze.application.execution import bind_deadline

with bind_deadline(timeout_s):   # None is a no-op passthrough
    result = await resolved_op(args)
```

Binding is task-scoped and **tighten-only** — a nested bind can shorten the
budget but never extend past the enclosing deadline. `bind_deadline(None)`
passes through, so an *optional* per-request timeout forwards without
branching. Ports that want to derive per-call budgets can read
`remaining_time()` — seconds left, or `None` when no deadline is bound.

## What enforcement looks like

- **Entry fail-fast** — an operation invoked with its budget already spent
  fails immediately, before any hook runs.
- **The whole plan is bounded** — the budget covers hooks, the transaction,
  and dispatch chains, and propagates into dispatched operations.
- **Resilience cooperates** — the [resilience executor](resilience.md) gates
  its entire strategy chain from the outside, and a retry abandons a backoff
  sleep that would outlive the deadline, surfacing the real error instead.

Expiry raises `exc.timeout` (`code="deadline_exceeded"`) — **504** at the
FastAPI edge, details suppressed. The kind is deliberately **non-retryable**:
within the same invocation the budget is spent, and a fresh invocation carries
a fresh deadline.

!!! note "Deadline ≠ per-attempt timeout"

    The resilience `TimeoutStrategy` bounds a *single attempt* and keeps
    raising retryable `infrastructure`, so a retry can take another shot. The
    invocation deadline bounds the *whole call* and is final. They compose:
    per-attempt timeouts inside, the deadline outside.

When no deadline is declared or bound, nothing changes — the unbound path is
one ContextVar read, with no timeout machinery.

## The budget is visible

A declared deadline projects through the operation catalog like every other
plan-derived fact: `OperationCatalogEntry.deadline`, an `x-deadline-seconds`
vendor extension plus a "Time budget" line on
[generated FastAPI routes](../integrations/fastapi.md#generated-routes), and a
sentence in each [MCP tool description](../integrations/mcp.md) — so clients
and agents can set their own timeouts instead of retrying a call that died of
budget exhaustion.

## Budgets cross service boundaries

When one Forze service calls another, the [outbound HTTP
adapter](../integrations/http.md) attaches the caller's remaining budget as an
`X-Forze-Deadline-Budget` header automatically (opt out per service with
`HttpServiceConfig(propagate_deadline=False)`). The receiving side honors it
only when asked:

```python
app.add_middleware(
    InvocationMetadataMiddleware,
    ctx_dep=runtime.get_context,
    bind_deadline_from_header=True,
)
```

The header carries a **duration**, not an instant, so clock skew between hosts
doesn't matter; and because binding is tighten-only, a forged or stale value
can only shorten the sender's own request — never extend it.

The budget is deliberately **not** a message-envelope header: deadlines belong
to the synchronous call chain. A queued event consumed after a backlog must
not inherit its producer's leftover budget.

## Cancellation never skips committed work

A deadline firing — or a client disconnecting — cancels the operation's task.
One window must survive that: after the root transaction **commits**, the
deferred after-commit work (idempotency records, event dispatch) is a
cancellation-protected critical section. It runs to completion even while
cancellation is pending, and the cancellation re-raises afterwards — so a
committed transaction is never left half-announced.

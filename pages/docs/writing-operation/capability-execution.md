---
title: Capability execution
icon: lucide/list-ordered
summary: How an operation runs — the stages around a handler, and how they're ordered
---

[Core Concepts](../core-concepts/application-layer.md) introduced an operation as
"a handler plus its plan." This chapter *is* that plan: the stages that wrap a
handler, what each one may touch, and how Forze orders them — the mechanism every
transaction, event, and authorization check rides on.

## An operation is a pipeline

The handler computes the result; everything around it is a **stage**. Stages run
in a fixed order — some outside any transaction, some inside it, some only after
it commits.

![Stages wrap the handler: before and wrap on the outside, the handler and on-success inside the transaction, after-commit at the end](../_diagrams/light/operation-composition.svg#only-light){ data-src="../_diagrams/light/operation-composition.svg#only-light" }
![Stages wrap the handler: before and wrap on the outside, the handler and on-success inside the transaction, after-commit at the end](../_diagrams/dark/operation-composition.svg#only-dark){ data-src="../_diagrams/dark/operation-composition.svg#only-dark" }

| Stage | When it runs | Typical use |
|-------|--------------|-------------|
| `before` | before the handler | auth, input checks, rate limiting |
| `wrap` | around the whole chain | metrics, retries, logging |
| `prepare` *(two-phase)* | outside the transaction, before it opens | external calls / compute that feed the write |
| `tx_before` | inside the transaction, before the handler | locks, preconditions |
| `on_success` *(tx)* | inside the transaction, after the handler | writes that must commit together |
| `after_commit` | after the root transaction commits | best-effort follow-up |
| `on_success` *(outer)* | after everything succeeded | out-of-transaction follow-up |
| `on_failure` / `finally_` | on the outcome | cleanup, error hooks |

You author these on the registry's scope binders — `bind_outer()` for the outer
scope, `bind_tx().set_route(...)` for the transactional one — then `freeze()`.

## The handler owns the result

One principle keeps the pipeline honest: **the handler defines the result;
stages never replace it.** They observe, guard, and react — they don't substitute
their own return value.

- `before` sees only the `args`.
- `on_success` and `after_commit` see the `args` and the `result`.
- `on_failure` sees the exception; `finally_` sees the outcome.

A stage may *raise* to abort the operation, but it can't quietly swap the answer.
That's exactly why you can bolt on an audit log or a permission check without
risk of corrupting what the handler returns.

## Ordering within a stage

When several hooks share a stage and one depends on another — authentication
before authorization, a lock before a write — you don't hand-sort them. Each step
declares **capabilities**: what it `provides` and what it `requires`.

```python
from forze.application.contracts.execution import BeforeStep

registry = (
    registry
    .bind("orders.create")
    .bind_outer()
    .before(
        BeforeStep(id="authn", factory=authn_factory, provides=("authn.principal",)),
        BeforeStep(id="authz", factory=authz_factory, requires=("authn.principal",)),
    )
    .finish(deep=True)
    .freeze()
)
```

Forze orders the stage from those edges (topologically, then by `priority`, then
declaration order). Use `depends_on=(...)` to order by explicit step id when
there's no capability to name. For a single linear guard, a higher `priority` is
enough — reach for capabilities only when hooks genuinely share prerequisites.

## Cross-cutting patches

`bind(...)` plans named operations. To apply a default across *many* — a tx
route, a deadline, an audit hook — use `patch(selector)`, which carries the same
binder chain but targets every operation a selector matches:

```python
from datetime import timedelta

from forze.base.primitives import str_key_selector

registry = (
    registry
    .patch(str_key_selector.all_keys())
    .with_deadline(timedelta(seconds=10))
    .finish(deep=True)
)
```

Patches are **late-bound** — selectors resolve at `freeze()` against the full set of
operations — so a top-level `patch(all_keys())` applied *after* assembling the registry
covers everything in it. That reach is **fail-closed** across
`OperationRegistry.merge(...)`: a broad patch authored inside one sub-registry that
would reach a sibling's operations makes `merge` raise, naming the offending selectors
(a patch matching only its own part, or one added after the merge, is never flagged).
Resolve a flagged reach by scoping the patch to a namespace
(`patch(selector, namespace=ns)` — "everything *I* contribute"), settling it into
concrete per-operation plans (`materialize_patches()`), or allowing it explicitly
(`merge(..., cross_registry=True)`) when the patch is *meant* to govern
downstream-contributed operations.

## Validated at freeze, not at request

`freeze()` is where the plan is checked and locked. Capability graphs must
resolve — no missing or duplicate providers, no cycles — dispatch targets must
exist, and any transactional stage must declare a route. A misconfigured pipeline
fails when you **build** the registry, at startup, rather than on a user's first
request.

That freeze-time check covers the transactional stages too — when those stages
open a transaction, and what happens when one rolls back, is the subject of
[Transactions](transactions.md).

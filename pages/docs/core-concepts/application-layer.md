---
title: Application
icon: lucide/workflow
summary: Specifications, operations, and the registry — how a business action is defined and composed
---

The application layer defines **what happens** — without knowing **how**
persistence or transport work. It turns aggregates into runnable operations and
keeps infrastructure behind contracts.

## Specifications bind it together

A **specification** is the logical name that ties an aggregate to its operations
and, later, to the adapters that store it. It's the single string — `"orders"` —
shared by the spec, the operation registry, and the dependency wiring. Get it
consistent and everything resolves; mismatch it and a port won't be found.

```python
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes

order_spec = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)
```

`DocumentSpec` is one of several `…Spec` types (`SearchSpec`, `CacheSpec`, …),
each naming a capability for one aggregate. In code the suffix is `Spec`; in
these docs we call it a *specification*.

When a read model carries credential or secret material (password hashes, token
digests), mark the spec with `sensitive=True` (available on `DocumentSpec` and
`SearchSpec`). Generated external surfaces — the FastAPI route generators and
the MCP tool/resource registrations — refuse to project a sensitive spec and
fail with a configuration error at attach/registration time, so a
credential-bearing read model can never leak through a generated endpoint. The
shipped authn specs (password accounts, API-key accounts, sessions, invites)
are marked this way.

## From handler to operation

Three roles turn business logic into something the runtime can run. Keeping them
distinct is what lets hooks, transactions, and dependency resolution stay *out*
of your business code.

![A handler is registered as an operation, then resolved against the context at run time](../_diagrams/light/operation-anatomy.svg#only-light){ data-src="../_diagrams/light/operation-anatomy.svg" }
![A handler is registered as an operation, then resolved against the context at run time](../_diagrams/dark/operation-anatomy.svg#only-dark){ data-src="../_diagrams/dark/operation-anatomy.svg" }

| Role | What it is | When it exists |
|------|------------|----------------|
| **Handler** | The business action — `async (args) -> result`, implementing `Handler[Args, R]` | You write it, or use a built-in |
| **Operation** | A handler plus its plan: stage hooks, transaction scope, query/command kind | Registered in the registry |
| **ResolvedOperation** | An operation with its dependencies resolved from a context | Built per call, at run time |

You rarely write all three. Forze ships built-in handlers for standard document
work; you register them and the registry assembles the operation.

## The operation registry

The **operation registry** maps each operation name to its handler and hooks. You
build it, then **freeze** it — a frozen registry is immutable and safe to share
across every request.

```python
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry

registry = build_document_registry(
    order_spec,
    DocumentDTOs(read=OrderRead, create=OrderCreate),
).freeze()
```

The built-in document operations cover the usual surface:

| Operation | Does | Returns |
|-----------|------|---------|
| `get` | Fetch one by id | read model |
| `create` | Create one | read model |
| `update` | Partial update | updated read model |
| `kill` | Hard delete | — |
| `list` | Paginated query | page of read models |

Cursor pagination, projections, and aggregates add more variants; soft
delete and restore come from `build_soft_deletion_registry`.

## Composing behaviour with stage hooks

Cross-cutting behaviour — a precondition, an audit entry, an after-commit
dispatch — attaches as a **stage hook** on the registry, never inside the
handler. Hooks run around the handler without replacing its result.

| Hook | Runs |
|------|------|
| `BeforeStep` | before the handler — preconditions, validation |
| `OnSuccessStep` | after a successful handler, inside the scope |
| after-commit | once the transaction commits |

This is how an `orders.update` operation can require an unshipped order, wrap
itself in a transaction, and emit an event — all without the handler knowing any
of it happened.

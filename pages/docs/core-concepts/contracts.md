---
title: Contracts & adapters
icon: lucide/plug
summary: Ports describe capabilities, adapters implement them, and the context resolves one to the other
---

Forze follows **hexagonal architecture** — ports and adapters. The application
declares *what* it needs as **ports** (contracts); integration packages provide
*how* as **adapters**. Neither side knows the other's concrete type.

![The application depends on a port; an integration package provides the adapter behind it](../_diagrams/light/integration-adapter-boundary.svg#only-light){ loading=lazy }
![The application depends on a port; an integration package provides the adapter behind it](../_diagrams/dark/integration-adapter-boundary.svg#only-dark){ loading=lazy }

## How a capability gets filled

1. The application defines a **port** — a contract describing a capability
   (document storage, a cache, search).
2. An integration package provides an **adapter** implementing that port for one
   backend — `forze_postgres`, `forze_redis`, and so on.
3. A **dependency registry** wires adapters to ports at startup, keyed by
   specification name.
4. A handler resolves the port from the **execution context** — and never imports
   the adapter.

```python
doc = ctx.document.command(order_spec)   # resolves a DocumentCommandPort
await doc.create(CreateOrderCmd(customer="Ada", total=99))
```

Swapping Postgres for Mongo changes step 3 — the registry — and nothing else.

## The capability catalog

These are the everyday contracts the application asks for. Each is a port (or a
read/write pair, CQRS-style), resolved from the context and backed by an adapter
in one or more integration packages.

| Capability | What it provides |
|------------|------------------|
| **Document** | Versioned aggregate storage, split into query and command ports |
| **Cache** | Read-through document caching, keyed by a `CacheSpec` |
| **Search** | Full-text, vector, and hub/federated search |
| **Counter** | Atomic, namespace-scoped counters |
| **Storage** | S3-style object upload and download |
| **Queue** | Point-to-point message produce and consume |
| **Pub/Sub & Stream** | Fan-out topics and append-only logs |
| **Outbox & Inbox** | Stage events in the write transaction, relay after commit, dedupe on the way in |
| **Idempotency** | Deduplicate retried requests |
| **Durable** | Long-running workflows, schedules, and functions |
| **Analytics** | Named, parameterized warehouse queries |

??? note "The full contract surface"

    The same resolve-a-port pattern covers more than the everyday set:

    - **Data** — graph nodes and edges, embeddings providers
    - **Coordination** — distributed locks, sagas, resilience policies, a deterministic clock and id source
    - **Identity & access** — authentication, authorization, principals, API keys, passwords, delegation, tenancy
    - **Integration** — outbound HTTP services

    Every one is resolved from the execution context exactly like the rest, and
    none require importing an adapter.

Method-level detail for each port lives with its integration recipe. The
*pattern* never changes: ask the context, get a port, never touch an adapter.

## Testing is just another adapter

Because handlers only ever see ports, tests swap real adapters for in-memory
ones. `forze_mock` ships fakes for every contract, backed by shared state — so
business logic is unit-testable with no database in sight.

```python
from forze_mock import MockDepsModule

# wire MockDepsModule in place of the real integration modules
```

By default mock transactions are no-ops; pass `strict_tx=True` to make
rollbacks revert the DB-backed mock stores — see
[Strict transactions under mock](../in-depth/transactions.md#strict-transactions-under-mock).

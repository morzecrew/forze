---
title: MongoDB
icon: lucide/leaf
summary: Document storage, search, transactions, and outbox on MongoDB
---

`forze[mongo]` implements document storage, search, transaction coordination,
and the transactional outbox on MongoDB — the same contracts as Postgres, behind
collections instead of tables.

## Install

```bash
uv add 'forze[mongo]'
```

Needs MongoDB. Multi-document transactions and outbox flush require a **replica
set** (or sharded cluster), not a standalone server.

## The client

```python
from forze_mongo import MongoClient

mongo = MongoClient()
```

`RoutedMongoClient` resolves a per-tenant connection — see
[Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

## Wire it

Relations are `(database, collection)` tuples, keyed by spec name:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_mongo import MongoClient, MongoConfig, MongoDepsModule, MongoDocumentConfig, mongo_lifecycle_step

orders_mongo = MongoDocumentConfig(read=("app", "orders"), write=("app", "orders"))

deps = DepsRegistry.from_modules(
    MongoDepsModule(client=mongo, rw_documents={"orders": orders_mongo}, tx={"orders"}),
)
lifecycle = LifecyclePlan.from_steps(
    mongo_lifecycle_step(uri="mongodb://localhost:27017", db_name="app", config=MongoConfig()),
)
```

## What it provides

| Contract | Keyed by | Module arg |
|----------|----------|------------|
| Document query / command | `DocumentSpec.name` | `rw_documents` / `ro_documents` |
| Transactions | route in `tx` | `tx` |
| Search | `SearchSpec.name` | `searches` |
| Outbox | `OutboxSpec.name` | `outboxes` |
| Counter | `CounterSpec.name` | `counters` |

## Notes

- **You own the collections and indexes.** Forze reads existing collections; it
  doesn't create or index them.
- **Transactions and outbox need a replica set** — a standalone `mongod` can't
  open multi-document transactions.
- `MongoSearchConfig` is imported from `forze_mongo.execution.deps` (not the
  top-level package).
- Relations accept a static `(database, collection)` tuple or a per-tenant
  resolver; routed clients handle database-per-tenant.

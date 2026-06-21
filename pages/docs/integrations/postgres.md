---
title: PostgreSQL
icon: lucide/database
summary: Document storage, search, and transactions on PostgreSQL
---

`forze[postgres]` implements document storage (read + write), full-text / vector
/ hub / federated search, and transaction coordination on PostgreSQL. Persistence
stays behind Forze contracts; PostgreSQL tables, indexes, and pools live at the
edge.

## Install

```bash
uv add 'forze[postgres]'
```

Needs a reachable PostgreSQL. Search engines may need extensions (PGroonga for
ranked text, pgvector for similarity).

## The client

```python
from forze_postgres import PostgresClient

pg = PostgresClient()
```

Use `RoutedPostgresClient` when the tenant or route decides the DSN — see
[Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

## Wire it

Map each logical spec name to physical relations, register them on the deps
module, and open the pool from the lifecycle plan:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_postgres import (
    PostgresConfig,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresLifecycleModule,
)

orders_pg = PostgresDocumentConfig(
    write=("public", "orders"),
    read=("public", "orders"),
    bookkeeping_strategy="application",  # or "database" with an UPDATE trigger
)

deps = DepsRegistry.from_modules(
    PostgresDepsModule(client=pg, rw_documents={"orders": orders_pg}, tx={"orders"}),
)
lifecycle = LifecyclePlan.from_modules(
    PostgresLifecycleModule(client=pg, dsn="postgresql://…", config=PostgresConfig()),
)
```

## What it provides

| Contract | Keyed by | Notes |
|----------|----------|-------|
| Document query / command | `DocumentSpec.name` (`rw_documents`, `ro_documents`) | read-write or read-only relations |
| Search | `SearchSpec.name` (`searches`) | `engine="pgroonga"` / `"fts"` / `"vector"`, plus hub & federated |
| Transactions | route in the module `tx` set | coordinates Postgres-backed ports on one connection |
| Analytics | `AnalyticsSpec.name` (`analytics`) | named, parameterized warehouse SQL — optional |

## Notes

- **You own the schema.** Forze *introspects* existing relations; it never
  creates application tables. Provision read / write / history / search relations
  (and extensions) with your migration tool first.
- **Bookkeeping.** `bookkeeping_strategy="application"` bumps `rev` /
  `last_update_at` in the write gateway; `"database"` defers to a `BEFORE UPDATE`
  trigger you supply.
- **Routed clients** require `introspector_cache_partition_key` on the deps
  module so the catalog cache partitions by tenant.
- **Relations** can be static `(schema, table)` tuples or per-tenant resolvers —
  see [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

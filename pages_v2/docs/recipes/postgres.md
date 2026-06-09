---
title: PostgreSQL
icon: lucide/database
summary: Swap the in-memory adapter for a real PostgreSQL backend without touching your handlers
---

This recipe takes the `users` service from the
[Quickstart](../get-started/quickstart.md) and points it at PostgreSQL. The
domain models, the spec, and the handlers **do not change** — only the
dependency wiring does. That's the layered architecture paying off.

!!! info "Prerequisites"

    - A working service from the [Quickstart](../get-started/quickstart.md)
      (a `User` aggregate and a `users` `DocumentSpec`).
    - A reachable PostgreSQL instance.
    - The Postgres extra installed:

        ```bash
        uv add 'forze[postgres]'
        ```

## Step 1 — Create the tables

Forze **introspects** existing schema; it never creates application tables for
you. Provision them with your migration tool before wiring the adapter.

A read-write document needs a **write** table; add a **history** table only if
you want revision history.

```sql
CREATE TABLE users (
    id         uuid PRIMARY KEY,
    rev        bigint      NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    name       text        NOT NULL,
    email      text
);
```

!!! tip "Let the database keep the bookkeeping"

    If you add a `BEFORE UPDATE` trigger that bumps `rev` and `updated_at`,
    set `bookkeeping_strategy="database"` (Step 2) and Forze defers to it.
    Without a trigger, use `"application"` and Forze bumps them in the write
    gateway.

## Step 2 — Configure the document mapping

A `PostgresDocumentConfig` maps the logical spec to physical relations as
`(schema, table)` tuples:

```python
from forze_postgres import PostgresClient, PostgresDocumentConfig

pg = PostgresClient()

users_pg = PostgresDocumentConfig(
    write=("public", "users"),
    # read and write can be the same table, or read can target a
    # cheaper projection / view
    read=("public", "users"),
    bookkeeping_strategy="application",
)
```

!!! tip "Multi-tenant?"

    Reach for `RoutedPostgresClient` instead of `PostgresClient` when the
    current tenant or route decides the DSN. Everything else in this recipe
    stays the same.

## Step 3 — Register the deps module

The module keys each config by the **same logical name** as the spec
(`"users"`). This name is the contract between your handlers and the backend —
get it wrong and the port won't resolve.

```python
from forze.application.execution import DepsRegistry
from forze_postgres import PostgresDepsModule

postgres_module = PostgresDepsModule(
    client=pg,
    # key matches DocumentSpec(name="users", ...); read-only
    # documents go under ro_documents instead
    rw_documents={"users": users_pg},
    # opt the route into transaction coordination so multi-step
    # writes commit atomically
    tx={"users"},
)

deps = DepsRegistry.from_modules(postgres_module)
```

!!! warning "The name must match everywhere"

    `DocumentSpec.name`, the key in `rw_documents`, and any `tx` route all
    share one string. A mismatch surfaces at request time as an unresolved
    dependency — see [Troubleshooting](#troubleshooting).

## Step 4 — Open and close the pool on lifecycle

`PostgresLifecycleModule` opens the connection pool on startup and closes it on
shutdown. Feed it into the runtime alongside the deps:

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import PostgresConfig, PostgresLifecycleModule

lifecycle = LifecyclePlan.from_modules(
    PostgresLifecycleModule(
        client=pg,
        dsn="postgresql://forze:forze@localhost:5432/forze",
        config=PostgresConfig(min_size=1, max_size=10),
    ),
)
```

## Step 5 — Build the runtime from Postgres instead of Mock

Here is the entire diff from the Quickstart — swap the dependency source, add
the lifecycle plan, leave the handlers alone:

```python
from forze.application.execution import DepsRegistry, ExecutionRuntime

def construct_runtime() -> ExecutionRuntime:
    crt = ExecutionRuntime(
        # was DepsRegistry.from_modules(MockDepsModule()).freeze()
        deps=DepsRegistry.from_modules(postgres_module).freeze(),
        # new: the pool now opens and closes with the runtime scope
        lifecycle=lifecycle.freeze(),
    )
    _rt.set_once(crt)
    return crt
```

The swap from `MockDepsModule` to `postgres_module` is the only line your
business code cares about. The FastAPI routes, the spec, the registry, and
every handler are byte-for-byte identical to the Quickstart.

## Verify

```bash
uv run uvicorn main:app --reload
```

```bash
curl -s -X POST http://127.0.0.1:8000/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Ada", "email": "ada@example.com"}'
```

Then confirm the row landed in Postgres:

```sql
SELECT id, rev, name FROM users;
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Port for `users` won't resolve at request time | The key in `rw_documents` differs from `DocumentSpec.name` | Use the same string in the spec, the module, and `tx` |
| Startup fails complaining about a missing `BEFORE UPDATE` trigger | `bookkeeping_strategy="database"` but no trigger exists | Add the trigger, or switch to `"application"` |
| `Write relation is required for non read-only documents` | A read-write document was registered without `write` | Add `write=(schema, table)`, or register it under `ro_documents` |
| Reads or writes hit the wrong table | A `(schema, table)` tuple points at the wrong relation | Check every `read` / `write` / `history` tuple against your migrations |

## Where to go next

<div class="grid cards" markdown>

-   :lucide-zap: **[Add caching](../get-started/quickstart.md)**

    ---

    Put a `CacheSpec` on the document and register a Redis cache route — reads
    serve from cache, writes invalidate it.

-   :lucide-search: **Full-text & vector search**

    ---

    `PostgresSearchConfig` with PGroonga, FTS, or vector engines, keyed by a
    `SearchSpec` name.

-   :lucide-database: **Multi-tenancy**

    ---

    `RoutedPostgresClient` for database-per-tenant, or relation resolvers for
    schema-per-tenant.

-   :lucide-cog: **[Runtime & lifecycle](../core-concepts/runtime.md)**

    ---

    How the execution context, pool lifecycle, and transaction scopes fit
    together.

</div>

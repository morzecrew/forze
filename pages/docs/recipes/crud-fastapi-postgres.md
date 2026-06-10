---
title: CRUD over Postgres
icon: lucide/database
summary: A full HTTP CRUD service for an aggregate, backed by Postgres — handlers never touch SQL
---

A `DocumentSpec` plus a `PostgresDepsModule` is the entire persistence story.
The FastAPI routes resolve the document ports from the context and return read
models — no SQL, no ORM, and optimistic concurrency for free.

The runnable version lives at `examples/recipes/crud_fastapi/` — `just run`
brings up ephemeral Postgres, serves the API, and tears it down.

## The aggregate

A `Product` with the three write shapes — domain model, create command, and a
partial update — plus a read model:

```python
--8<-- "recipes/crud_fastapi/app.py:domain"
```

## The specification

One spec names the aggregate and its write types. `"products"` is the logical
name shared by the adapter wiring:

```python
--8<-- "recipes/crud_fastapi/app.py:spec"
```

## Wire Postgres

`PostgresDocumentConfig` maps the spec to its tables; `PostgresDepsModule`
registers the document ports under `"products"`, and the lifecycle module owns
the connection pool:

```python
--8<-- "recipes/crud_fastapi/app.py:wiring"
```

??? note "The demo table"

    The example creates its `products` table on startup so it's self-contained.
    A real service owns its schema through migrations — Forze reads and writes
    rows, it doesn't manage DDL. The columns `id`, `rev`, `created_at`, and
    `last_update_at` are the document bookkeeping fields.

## The routes

The runtime opens inside the app's lifespan; each route resolves the document
**command** or **query** port from the context and calls it:

```python
--8<-- "recipes/crud_fastapi/app.py:routes"
```

- **Create / get / list / delete** map straight onto the document ports.
- **Update** carries the document's `rev` — a stale `rev` raises a `conflict`,
  which `register_exception_handlers` turns into a `409`. That's
  [optimistic concurrency](../in-depth/concurrency-conflicts.md) with no extra
  code.
- A missing id raises `not_found` → `404`.

Hand-writing the routes keeps this recipe transparent; the same endpoints can
also be [generated from an operation
registry](../integrations/fastapi.md#generated-routes) with
`attach_document_routes`.

## Run it

```bash
cd examples/recipes/crud_fastapi
just run
```

Then open [http://localhost:8000/docs](http://localhost:8000/docs).

## Where next

<div class="grid cards" markdown>

-   :lucide-zap: **[Cache reads with Redis](cache-reads-with-redis.md)**

    ---

    Serve repeat reads from Redis and invalidate on writes — same handlers, one
    extra module.

-   :lucide-copy-check: **[Add idempotency](add-idempotency.md)**

    ---

    Make a retried `POST` a no-op that returns the first result.

</div>

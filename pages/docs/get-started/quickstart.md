---
title: Quickstart
icon: lucide/zap
summary: A working in-memory CRUD service in about ten minutes — no Docker
---

## What you will build

A minimal REST service for a `User` aggregate:

| Method | Path | Action |
|--------|------|--------|
| `POST` | `/users` | Create a user |
| `GET` | `/users/{id}` | Get one user |
| `GET` | `/users` | List users |
| `DELETE` | `/users/{id}` | Delete a user |

Storage is **in-memory** — no Docker, no migrations. The complete, runnable file
is [`examples/quickstart/app.py`](https://github.com/morzecrew/forze/blob/main/examples/quickstart/app.py);
the steps below build it up.

## Step 1 — Create the project

```bash
uv init forze-quickstart
cd forze-quickstart
uv add 'forze[fastapi]'
```

Everything below goes into a single `main.py`.

## Step 2 — Define the domain models

An aggregate needs a **domain model**, a **create command**, and a **read
model**. `Document` gives you `id`, `rev`, and timestamps for free.

```python
--8<-- "quickstart/app.py:domain"
```

??? question "Why three types?"

    - **Domain model** — the business entity, with behaviour and invariants.
    - **Create command** — the frozen input for `POST`.
    - **Read model** — the frozen projection returned from `GET` (here it adds a
      computed `email_provided`).

    Update commands come later; this quickstart skips them on purpose.

## Step 3 — Declare a specification

The [specification](../core-concepts/application-layer.md) is the logical name —
`"users"` — that ties the models to their operations and, later, to adapters.

```python
--8<-- "quickstart/app.py:spec"
```

## Step 4 — Build the operation registry

`build_document_registry` assembles the standard CRUD operations; `freeze()`
makes the registry immutable and shareable.

```python
--8<-- "quickstart/app.py:registry"
```

## Step 5 — Wire the runtime

`MockDepsModule` provides in-memory adapters for every contract. The
[`ExecutionRuntime`](../core-concepts/runtime.md) builds the context on startup;
a `RuntimeVar` holds it for per-request access.

```python
--8<-- "quickstart/app.py:runtime"
```

## Step 6 — Attach the routes

The runtime runs inside the app's lifespan. Each route resolves a
[`DocumentFacade`](../core-concepts/application-layer.md) from the context and
calls an operation — the handlers never touch HTTP:

```python
--8<-- "quickstart/app.py:routes"
```

`register_exception_handlers` maps a `CoreException` to a response, so a missing
user comes back as a `404`. (Higher-level route builders are
[planned](../integrations/fastapi.md); for now routes are hand-wired.)

## Step 7 — Run it

```bash
uv run uvicorn main:app --reload
```

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the interactive
explorer, or try it from the shell:

```bash
# Create — note the id in the response
curl -s -X POST http://127.0.0.1:8000/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Ada", "email": "ada@example.com"}'

curl -s http://127.0.0.1:8000/users            # list
curl -s http://127.0.0.1:8000/users/<id>       # get one
curl -s -X DELETE http://127.0.0.1:8000/users/<id>   # delete
```

## What you just did

You built a complete service without a single line of HTTP or storage code in
your domain:

- A **`User` aggregate** with its command and read models — pure Python, no
  infrastructure.
- A **specification** and a frozen **operation registry** — the named operations
  the service exposes.
- An **`ExecutionRuntime`** wired to in-memory adapters, opened for the app's
  lifetime.
- **Routes** that resolve operations from the context and return read models.

The only thing tying this to "in-memory" is `MockDepsModule` in Step 5. Swap it
for `PostgresDepsModule` + `RedisDepsModule` and the domain, spec, registry, and
routes don't change — that's the whole point. The
[PostgreSQL integration](../integrations/postgres.md) shows the swap.

## Where to go next

<div class="grid cards" markdown>

-   :lucide-compass: **[Core concepts](../core-concepts/overview.md)**

    ---

    Understand the layers, contracts, and runtime behind what you just built.

-   :lucide-database: **[Back it with Postgres](../recipes/cache-reads-with-redis.md)**

    ---

    Swap the in-memory adapters for real infrastructure.

</div>

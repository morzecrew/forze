---
title: Runtime
icon: lucide/cog
summary: The execution context, lifecycle, and transactions that run every request
---

The **runtime** turns a pile of specifications, operations, and adapters into a
running service. It owns three things: the dependency wiring, the lifecycle of
infrastructure clients, and the per-request execution context.

![DepsRegistry and LifecyclePlan feed the ExecutionRuntime, which creates an ExecutionContext per scope](../_diagrams/light/execution-runtime.svg#only-light){ loading=lazy }
![DepsRegistry and LifecyclePlan feed the ExecutionRuntime, which creates an ExecutionContext per scope](../_diagrams/dark/execution-runtime.svg#only-dark){ loading=lazy }

## Two registries, frozen

You describe the runtime with two registries, then **freeze** them — the runtime
takes frozen, validated inputs and does not coerce them:

- **`DepsRegistry`** — *what to build*: which adapters back which specifications.
- **`LifecyclePlan`** — *startup and shutdown*: opening pools, warming caches, and
  closing connections, in dependency order.

```python
from forze.application.execution import (
    DepsRegistry,
    ExecutionRuntime,
    LifecyclePlan,
)

runtime = ExecutionRuntime(
    deps=DepsRegistry.from_modules(postgres_module, redis_module).freeze(),
    lifecycle=LifecyclePlan.from_modules(postgres_lifecycle).freeze(),
)
```

## The execution context

The **`ExecutionContext`** is the seam from the [overview](overview.md): the one
object handlers use to resolve every port. It also carries request identity — who
is calling (`AuthnIdentity`), which tenant (`TenantIdentity`), and correlation
metadata — bound at the application boundary, for example in HTTP middleware.

## The scope lifecycle

A runtime runs inside a **scope**. Entering it builds the context and starts
infrastructure; leaving it tears everything down in reverse.

```python
async with runtime.scope():
    ctx = runtime.get_context()
    # ... handle requests ...
```

1. **Create context** — resolve the frozen deps into live ports.
2. **Startup** — run lifecycle startup steps in dependency order.
3. **Serve** — the application handles requests.
4. **Shutdown** — run shutdown steps in reverse.
5. **Reset** — clear the context.

!!! warning "No scope, no context"

    Resolving a port outside an active scope fails. Wire `runtime.scope()` (or
    `startup()` / `shutdown()`) into your framework's lifespan — see the
    [Quickstart](../get-started/quickstart.md).

If a startup step fails, the steps that already ran are shut down in reverse
before the error propagates — no half-open pools left behind.

## Transactions

Multi-step writes that must commit together run in a **transaction scope**, keyed
by the same specification name as the deps wiring:

```python
async with ctx.tx_ctx.scope("orders"):
    await doc.create(...)
    await outbox.stage(...)
```

Nested scopes reuse the same transaction, with savepoints where the backend
supports them.

## You've got the model

That's the whole shape of Forze: **aggregates** at the center, **operations** over
them, **ports** out to infrastructure, and a **runtime** to tie it together. Time
to build something with it.

<div class="grid cards" markdown>

-   :lucide-zap: **[Quickstart](../get-started/quickstart.md)**

    ---

    A running service in about ten minutes, in-memory — no Docker.

-   :lucide-database: **[CRUD over Postgres](../recipes/crud-fastapi-postgres.md)**

    ---

    Wire real backends: Postgres, Redis, FastAPI, and more.

</div>

---
title: Project structure
icon: lucide/folder-tree
summary: How to lay out a real Forze service so the boundaries stay honest
---

The [Quickstart](quickstart.md) fit in one `main.py`. A real service grows into
the four [layers](../core-concepts/architecture.md) — and a directory layout that
mirrors them keeps the dependency rules easy to follow.

## A layout that maps to the layers

```text
myservice/
├── domain/            # aggregates, commands, read models, invariants — pure Python
│   └── orders.py
├── application/       # specs, custom handlers, operation registries
│   └── orders.py
├── infrastructure/    # deps modules, lifecycle plans, client construction
│   └── wiring.py
├── interface/         # routes / event handlers, the runtime + lifespan
│   └── http.py
└── main.py            # compose the runtime and the app
```

Nothing forces these names — Forze imposes no project layout. What matters is the
**dependency direction**, and folders make it visible.

## What goes where

| Layer | Holds | May import |
|-------|-------|------------|
| **domain** | `Document` subclasses, create/update commands, read models, `@invariant` / `@update_validator`, mixins | nothing else in your app |
| **application** | `DocumentSpec` (and friends), `build_*_registry`, custom handlers, stage hooks | domain |
| **infrastructure** | deps modules (`PostgresDepsModule`, …), `LifecyclePlan`, client setup | application, domain |
| **interface** | FastAPI / Socket.IO routes, `ExecutionRuntime` construction, the context dependency | application, infrastructure |

## The one rule

Dependencies point **inward**: `domain` imports nothing from your other layers,
`application` imports only `domain`, and infrastructure and interface sit on the
outside. This is what lets you swap Postgres for Mongo, or FastAPI for a CLI,
without touching business logic — and it's enforced by import-linter contracts,
not just convention. See [Architecture](../core-concepts/architecture.md).

## The name is the seam

One logical name — `"orders"` — appears in the **spec** (application), the
**deps module route** (infrastructure), and is resolved in **routes**
(interface). That shared string is the contract between the layers; keep it
consistent and everything wires up. Get it wrong and a port won't resolve.

## Next steps

<div class="grid cards" markdown>

-   :lucide-compass: **[Core concepts](../core-concepts/overview.md)**

    ---

    Understand the layers, contracts, and runtime this layout is built around.

-   :lucide-database: **[CRUD on Postgres](../recipes/crud-fastapi-postgres.md)**

    ---

    Put the layout to work with a real Postgres-backed API.

</div>

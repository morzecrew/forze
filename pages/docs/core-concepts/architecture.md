---
title: Architecture
icon: lucide/layers
summary: The four layers Forze enforces and the dependency rules that keep them honest
---

Forze organizes code into four layers. Each has one responsibility, and
dependencies flow **inward** — outer layers depend on inner layers, never the
reverse.

!!! abstract "The one rule"

    **Dependencies point toward the domain.** Business logic at the center
    knows nothing about the database, the web framework, or the cache. Swap
    any of those and the domain doesn't move.

![Layered architecture: domain at the center, wrapped by application, then infrastructure and interface](../_diagrams/light/layered-architecture.svg#only-light){ data-src="../_diagrams/light/layered-architecture.svg#only-light" }
![Layered architecture: domain at the center, wrapped by application, then infrastructure and interface](../_diagrams/dark/layered-architecture.svg#only-dark){ data-src="../_diagrams/dark/layered-architecture.svg#only-dark" }

| Layer | Responsibility | Depends on |
|-------|----------------|------------|
| **Domain** | Business logic, invariants, validation, model behaviour | — |
| **Application** | Orchestration, handlers, contracts, composition, runtime | Domain |
| **Infrastructure** | Databases, caches, external services, adapter implementations | Application, Domain |
| **Interface** | HTTP routes, WebSocket handlers, user-facing entry points | Application, Domain, Infrastructure |

## The four layers

=== ":lucide-box: Domain"

    Pure business logic: entities, value objects, commands, and validation
    rules. Domain code imports from **no other layer** — only Pydantic
    models, dataclasses, and plain Python.

    No database drivers. No HTTP frameworks. No adapters or containers.

    This is the most stable part of the system. Changing a database engine or
    web framework never touches domain code.

=== ":lucide-workflow: Application"

    Defines **what** happens without knowing **how**:

    - **Contracts (ports)** — the capabilities the app needs, each described as
      a contract: document storage, cache, transactions, search, queues.
    - **Handlers** — single-purpose operations that receive an execution
      context and resolve ports from it.
    - **Composition** — facades, the `OperationRegistry`, and the stage hooks
      that wrap handlers.
    - **Execution runtime** — the dependency registry, lifecycle hooks, and
      transaction management.

    Imports from the domain, never from infrastructure or interface.

=== ":lucide-server: Infrastructure"

    Concrete implementations of application contracts — one optional package
    per backend:

    - `forze_postgres`, `forze_mongo`, `forze_firestore` — documents, search, transactions
    - `forze_redis` — cache, counters, idempotency, pub/sub, streams
    - `forze_s3`, `forze_gcs` — object storage
    - `forze_temporal`, `forze_inngest` — durable workflows and functions
    - `forze_rabbitmq`, `forze_sqs` — queues

    Imports from application and domain to implement contracts and serialize
    domain models.

=== ":lucide-globe: Interface"

    The outermost, user-facing boundary — transport concerns only: receive a
    request, resolve an operation from the frozen registry, return a response.

    - `forze_fastapi` — HTTP routing, idempotency, OpenAPI
    - `forze_socketio` — real-time WebSocket events and typed dispatch

    Depends on application and infrastructure. **Never** contains business
    logic.

## Why the direction matters

Because dependencies only ever point inward, the things most expensive to
change are the things best insulated:

!!! success "Swap infrastructure, keep everything else"

    Trade Postgres for Mongo by changing the **dependency registry** — not a
    single handler. Replace FastAPI with a CLI without touching business
    logic. The import rules make this a configuration change, not a rewrite.

These rules aren't documentation — they're enforced by import-linter
contracts in `pyproject.toml`. A pull request that makes the domain import an
adapter fails CI.

## Practical impact

What actually changes when you make a common change, and what stays put:

| Scenario | What changes | What stays the same |
|----------|--------------|---------------------|
| Postgres :material-arrow-right: Mongo | Dependency module, lifecycle step | Domain models, handlers, specs |
| Add Redis caching | Dependency module, lifecycle step, cache flag on spec | Domain models, handlers |
| Replace FastAPI with gRPC | Interface / transport layer | Domain, handlers, specs, adapters |
| Add a business rule | Domain model validation | Adapters, routing |
| Audit-log every operation | Stage hooks (`BeforeStep` / `OnSuccessStep`) | Domain models, adapters |

## Where to go next

<div class="grid cards fz-cards" markdown>

-   :lucide-box: **[Domain](domain-layer.md)**

    ---

    Models, versioning, update semantics, mixins, and validation.

-   :lucide-workflow: **[Application](application-layer.md)**

    ---

    Handlers, operations, stage hooks, and the registry.

-   :lucide-plug: **[Contracts & adapters](contracts.md)**

    ---

    How ports describe capabilities and adapters implement them.

-   :lucide-cog: **[Runtime](runtime.md)**

    ---

    Execution context, lifecycle, and transaction scopes.

</div>

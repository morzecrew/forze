# Layered Architecture

Forze organizes code into four layers with strict dependency rules. Each layer has a clear responsibility, and dependencies flow **inward**: outer layers depend on inner layers, never the reverse.

## The four layers

At the center sits the **domain**: pure business logic with no external dependencies. The **application** layer wraps it with orchestration, contracts, and composition. **Infrastructure** provides concrete implementations of application contracts. The **interface** layer is the user-facing entry point that ties everything together.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/layered-architecture.svg" alt="Layered architecture">
  <img class="d2-dark" src="../../assets/diagrams/dark/layered-architecture.svg" alt="Layered architecture">
</div>

| Layer | Responsibility | Depends on |
|-------|----------------|------------|
| **Domain** | Business logic, invariants, validation rules, model behavior | Nothing |
| **Application** | Orchestration, usecases, contracts (ports), composition, execution runtime | Domain |
| **Infrastructure** | Databases, caches, external services, adapter implementations | Application, Domain |
| **Interface** | HTTP routes, WebSocket handlers, user-facing entry points | Application, Infrastructure |

## Domain layer

The domain layer holds pure business logic. It defines entities (`Document`), value objects (`BaseDTO`), commands (`CreateDocumentCmd`), and validation rules. Domain code never imports from any other layer. This means:

- No database drivers or HTTP frameworks
- No adapter classes or dependency containers
- Only Pydantic models, dataclasses, and plain Python

The domain layer is the most stable part of the system. Changing a database engine or web framework never requires changes to domain code.

## Application layer

The application layer defines **what** happens without knowing **how**. It contains:

- **Contracts (ports)**: protocol interfaces describing capabilities the application needs (document storage, cache, transactions, search, queues, etc.)
- **Usecases**: single-purpose operations that receive an `ExecutionContext` and resolve ports from it
- **Composition**: facade providers, registries, and plans that wire usecases with middleware
- **Execution runtime**: dependency injection container, lifecycle hooks, and transaction management

The application layer imports from the domain layer but never from infrastructure or interface.

## Infrastructure layer

The infrastructure layer provides concrete implementations of application contracts:

- `forze_postgres` implements document and search ports using PostgreSQL
- `forze_redis` implements cache, counter, idempotency, pub/sub, and stream ports
- `forze_s3` implements the storage port using S3-compatible services
- `forze_mongo` implements document and transaction ports using MongoDB

Infrastructure packages import from both application and domain layers to implement contracts and serialize domain models.

## Interface layer

The interface layer is the outermost, user-facing boundary of the backend. It handles transport concerns: receiving requests, invoking usecases, and returning responses. Typically it is the only layer that end users (or other services) interact with directly.

- `forze_fastapi` provides HTTP routing, idempotency handling, and OpenAPI integration
- `forze_socketio` provides real-time WebSocket event routing and typed command dispatch

Interface packages depend on the application layer (to invoke usecases and resolve contexts) and on infrastructure (for runtime wiring and lifecycle management). They never contain business logic.

## Dependency rule

The dependency rule is enforced by design and by import-linter contracts defined in `pyproject.toml`:

    :::text
    Domain  ←  Application  ←  Infrastructure
                    ↑
                Interface ──→ Infrastructure

- Domain imports nothing from other layers
- Application imports from domain only
- Infrastructure imports from application and domain
- Interface imports from application and infrastructure

This means you can swap Postgres for Mongo by changing the dependency plan, not the usecases. You can replace FastAPI with a CLI without touching business logic.

## Practical impact

| Scenario | What changes | What stays the same |
|----------|-------------|-------------------|
| Switch from Postgres to Mongo | Dependency module, lifecycle step | Domain models, usecases, specs |
| Add Redis caching | Dependency module, lifecycle step, cache flag on spec | Domain models, usecases |
| Replace FastAPI with gRPC | Interface/transport layer | Domain models, usecases, specs, adapters |
| Add a new business rule | Domain model validation | Infrastructure adapters, routing |
| Add audit logging to all operations | Usecase plan (add an effect) | Domain models, adapters |

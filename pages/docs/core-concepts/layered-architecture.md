# Layered Architecture

Forze organizes code into three layers with strict dependency rules. Dependencies flow **inward**: infrastructure depends on application, application depends on domain. The domain layer has no external dependencies.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/layered-architecture.svg" alt="Layered architecture">
  <img class="d2-dark" src="../../assets/diagrams/dark/layered-architecture.svg" alt="Layered architecture">
</div>

## Layer responsibilities

| Layer | Responsibility | Depends on |
|-------|----------------|------------|
| **Domain** | Business logic, invariants, validation rules, model behavior | Nothing |
| **Application** | Orchestration, usecases, contracts (ports), composition, execution runtime | Domain |
| **Infrastructure** | Databases, caches, HTTP routers, external services, adapter implementations | Application, Domain |

## Domain layer

The domain layer holds pure business logic. It defines entities (`Document`), value objects (`BaseDTO`), commands (`CreateDocumentCmd`), and validation rules. Domain code never imports from the application or infrastructure layers. This means:

- No database drivers or HTTP frameworks
- No adapter classes or dependency containers
- Only Pydantic models, dataclasses, and plain Python

The domain layer is the most stable part of the system. Changing a database engine or web framework should never require changes to domain code.

## Application layer

The application layer defines **what** happens without knowing **how**. It contains:

- **Contracts (ports)** -- protocol interfaces describing capabilities the application needs (document storage, cache, transactions, search, etc.)
- **Usecases** -- single-purpose operations that receive an `ExecutionContext` and resolve ports from it
- **Composition** -- facade providers, registries, and plans that wire usecases with middleware
- **Execution runtime** -- dependency injection container, lifecycle hooks, and transaction management

The application layer imports from the domain layer but never from infrastructure.

## Infrastructure layer

The infrastructure layer provides concrete implementations of application contracts:

- `forze_postgres` implements document and search ports using PostgreSQL
- `forze_redis` implements cache, counter, idempotency, pub/sub, and stream ports using Redis
- `forze_s3` implements the storage port using S3-compatible services
- `forze_mongo` implements document and transaction ports using MongoDB
- `forze_fastapi` provides HTTP routing and idempotency handling

Infrastructure packages import from both application and domain layers to implement contracts and serialize domain models.

## Dependency rule

The dependency rule is enforced by design and by import-linter contracts defined in `pyproject.toml`:

```
Domain  <--  Application  <--  Infrastructure
```

- Domain imports nothing from application or infrastructure
- Application imports from domain only
- Infrastructure imports from both application and domain

This means you can swap Postgres for Mongo by changing the dependency plan, not the usecases. You can replace FastAPI with a CLI without touching business logic.

## Practical impact

| Scenario | What changes | What stays the same |
|----------|-------------|-------------------|
| Switch from Postgres to Mongo | Dependency module, lifecycle step | Domain models, usecases, specs |
| Add Redis caching | Dependency module, lifecycle step, cache flag on spec | Domain models, usecases |
| Replace FastAPI with gRPC | Router/transport layer | Domain models, usecases, specs, adapters |
| Add a new business rule | Domain model validation | Infrastructure adapters, routing |
| Add audit logging to all operations | Usecase plan (add an effect) | Domain models, adapters |

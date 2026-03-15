---
name: forze-architecture
description: Understand the Forze framework's DDD/hexagonal architecture, layer boundaries, and import constraints. Use when creating new modules, refactoring code, or reviewing architecture compliance.
---

# Forze Architecture

Forze is a **Domain-Driven Design (DDD) and Hexagonal Architecture** framework for Python backend services. Import boundaries are enforced at build time by `import-linter` contracts defined in `pyproject.toml`.

## Layer Hierarchy

```text
forze.application          ← highest layer
    └── composition        ← orchestration (facades, registries)
    └── usecases           ← use case implementations
    └── execution          ← runtime, context, middleware, DI
    └── dto                ← data transfer objects
    └── mapping            ← DTO mapping steps
    └── contracts          ← ports (Protocol interfaces) and specs

forze.domain               ← middle layer
    └── mixins             ← reusable model mixins
    └── models             ← Document, CoreModel, BaseDTO
    └── validation         ← update validators

forze.base                 ← lowest layer (primitives, errors, logging)
```

**Rule:** Each layer may only import from layers below it. Never import upward.

## Integration Packages

Integration packages are **isolated adapters** that implement ports from `forze.application.contracts`:

| Package | Purpose |
|---------|---------|
| `forze_fastapi` | HTTP API (routers, routing, features) |
| `forze_postgres` | PostgreSQL (documents, search, transactions) |
| `forze_redis` | Redis (cache, counter, pub/sub, streams, idempotency) |
| `forze_s3` | S3-compatible object storage |
| `forze_mongo` | MongoDB (documents, history) |
| `forze_temporal` | Temporal workflows |
| `forze_rabbitmq` | RabbitMQ queues |
| `forze_sqs` | AWS SQS |
| `forze_socketio` | Socket.IO |
| `forze_mock` | In-memory mock adapters for local development |

**Rule:** Integration packages **never import from each other**. They are `protected` modules — no other package may import them. They only depend on the core `forze` package.

## `forze_fastapi` Internal Layering

```text
forze_fastapi.routers      ← endpoint definitions (document, search)
forze_fastapi.routing       ← router class, route features, params
forze_fastapi.constants     ← shared constants
```

## Import Rules Summary

1. `forze.application` → `forze.domain` → `forze.base` (downward only)
2. Application sublayers: `composition` → `usecases` → `execution` → `dto` → `mapping` → `contracts`
3. Domain sublayers: `mixins` → `models` → `validation`
4. Integration packages are isolated from each other
5. No external package may import from integration packages

## Creating New Modules

When adding code to Forze, determine the correct layer:

- **New port/protocol** → `forze.application.contracts.<namespace>/`
- **New spec type** → `forze.application.contracts.<namespace>/`
- **New usecase** → `forze.application.usecases/`
- **New facade/registry** → `forze.application.composition/`
- **New domain model** → `forze.domain.models/`
- **New mixin** → `forze.domain.mixins/`
- **New validator** → `forze.domain.validation/`
- **New primitive/utility** → `forze.base/`
- **New adapter** → `forze_<provider>/adapters/`

## Validation

Run `just quality` (or `just quality -s` for strict mode) to verify import contracts, linting, types, dead code, dependencies, and security.

The import contract check runs: `uv run lint-imports`

## Common Patterns

### Immutable Data Classes

Forze uses `attrs` with frozen, slotted, keyword-only classes:

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class MyService:
    ctx: ExecutionContext
    repo: DocumentReadPort[MyReadModel]
```

### Protocol-Based Ports

All contracts use `typing.Protocol` with `@runtime_checkable` where needed:

```python
@runtime_checkable
class MyPort(Protocol):
    def do_something(self, key: str) -> Awaitable[str]: ...
```

### Spec-Driven Resolution

Ports are resolved by spec objects (`DocumentSpec`, `SearchSpec`, `CacheSpec`) through `ExecutionContext`:

```python
result = ctx.doc_read(my_spec).get(pk)
```

## Key Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Import-linter contracts, dependencies, tool config |
| `justfile` | Build/test/quality commands |
| `CONTRIBUTING.md` | Development workflow and conventions |

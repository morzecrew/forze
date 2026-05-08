---
title: Getting Started
summary: Choose the right first path through Forze
---

Forze is easiest to learn in two passes: first understand the shape of a Forze project, then build one complete service.

## Start with the mental model

A Forze backend keeps business rules in the domain layer and puts infrastructure behind application contracts.

| Piece | What you write first | Where to read next |
|-------|----------------------|--------------------|
| Domain models | `Document`, create command, update DTO, read model | [Domain Layer](concepts/domain-layer.md) |
| Specs | `DocumentSpec`, `SearchSpec`, cache specs | [Specs and wiring](concepts/specs-and-wiring.md) |
| Usecases | business operations that receive `ExecutionContext` | [Application Layer](concepts/application-layer.md) |
| Adapters | Postgres, Redis, FastAPI, queues, storage | [Integrations](integrations/fastapi.md) |

## Build your first project

Follow the full walkthrough when you are ready to write code:

[First project walkthrough](first-project-walkthrough.md)

It creates a small CRUD service with:

- a `Project` aggregate
- a `DocumentSpec`
- Postgres-backed document storage
- Redis-backed caching
- FastAPI endpoints

## Pick a task-oriented recipe

If you already know what you need, jump straight to a recipe:

- [CRUD with FastAPI, Postgres, and Redis](recipes/crud-fastapi-postgres-redis.md)
- [Read-only document API](recipes/read-only-document-api.md)
- [Add caching](recipes/add-caching.md)
- [Add idempotency](recipes/add-idempotency.md)
- [Background workflow](recipes/background-workflow.md)

## Keep API inventories for later

Beginner pages avoid long tables and signatures. When you need exact contracts, dependency keys, or operation signatures, use the [Reference](reference/index.md).

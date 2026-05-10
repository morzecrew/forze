---
title: Overview
summary: The foundation for building backend services with clean architecture
---

The `forze` core package provides the foundation for building backend services with clean architecture. It contains everything you need to define domain models, declare contracts, compose usecases, and wire dependencies — without depending on any specific database, queue, or web framework.

## Package structure

| Layer | Module | What it provides |
|-------|--------|------------------|
| **Base** | `forze.base` | Error hierarchy, codecs, primitives (UUID, datetime, types),<br>serialization, file I/O, introspection |
| **Domain** | `forze.domain` | `Document`, `CoreModel`, `BaseDTO`, mixins,<br>update validators, field constants |
| **Application** | `forze.application` | Contracts (ports, specs), execution engine, middlewares,<br>usecase plans, composition, mapping, DTOs |

Dependencies flow inward: Application → Domain → Base. No layer imports from an outer one.

## Section guide

### Foundations

Start here to understand the building blocks:

- [Base Layer](base-layer.md) — error types, codecs, UUID generation, serialization helpers, and other shared utilities
- [Domain Models](domain-models.md) — `Document`, commands, read models, mixins, and update validation

### Application machinery

These pages cover the orchestration layer:

- [Contracts](contracts.md) — protocol ports, specs, and dependency keys for all infrastructure concerns
- [Authentication](authentication.md) — full authn contract surface (verifiers, resolvers, lifecycle, dep keys), `forze_authn` first-party stack, and `forze_oidc` integration surface
- [Execution](execution.md) — `ExecutionContext`, dependency injection, runtime lifecycle
- [Middleware & Plans](middleware-plans.md) — guards, effects, transaction wrapping, usecase plans, and the registry
- [Composition & Mapping](composition.md) — facades, providers, DTO mapping pipelines, and paginated responses

### Reference

- [Query Syntax](query-syntax.md) — filter and sort DSL used across all adapters

## Quick example

A minimal aggregate definition using the core package:

    :::python
    from forze.domain.models import (
        Document,
        CreateDocumentCmd,
        ReadDocument,
        BaseDTO,
    )
    from forze.domain.mixins import SoftDeletionMixin
    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec


    # Domain model
    class Task(SoftDeletionMixin, Document):
        title: str
        done: bool = False


    # Commands and read model
    class CreateTaskCmd(CreateDocumentCmd):
        title: str


    class UpdateTaskCmd(BaseDTO):
        title: str | None = None
        done: bool | None = None


    class TaskRead(ReadDocument):
        title: str
        done: bool
        is_deleted: bool = False


    # Specification (infra maps "tasks" to tables via PostgresDepsModule)
    task_spec = DocumentSpec(
        name="tasks",
        read=TaskRead,
        write={
            "domain": Task,
            "create_cmd": CreateTaskCmd,
            "update_cmd": UpdateTaskCmd,
        },
        cache=CacheSpec(name="tasks"),
    )

With this in place, adapters (Postgres, Mongo, etc.) know how to store and retrieve tasks, usecases can be composed with middleware, and the entire aggregate is testable without any infrastructure.

## Related sections

- [Core Concepts](../concepts/index.md) — architectural overview, layered architecture, and design rationale
- [First Project Walkthrough](../first-project-walkthrough.md) — end-to-end walkthrough of building a service with Forze
- [Integrations](../integrations/postgres.md) — adapter packages for Postgres, Redis, S3, MongoDB, and more

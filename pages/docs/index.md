---
title: Welcome to forze
summary: Domain-Driven Design and Hexagonal Architecture for backend services
---

**Forze** is a Python toolkit for building backend services with clear boundaries:
domain-first models, application-level orchestration, and replaceable infrastructure adapters.

If you are new to the package, start with:

1. [Installation](installation.md)
2. [Getting Started](getting-started.md)
3. [Core Concepts](core-concepts/index.md)

## Why use Forze?

Forze helps you keep business logic stable while storage and framework choices evolve.

- **Layered** — four clean layers (domain, application, infrastructure, interface) with strict dependency rules
- **Explicit** — contracts (ports) describe what the app needs; adapters deliver it
- **Composable** — adapters are wired declaratively via dependency plans
- **Testable** — usecases run with fake or in-memory dependencies
- **Framework-agnostic** — core modules are not tied to any web framework, database, or cloud service

/// note
Forze is not a full-stack framework. It provides architecture primitives and integration packages you compose.
///

## Architecture at a glance

Forze organizes code into four layers. Dependencies flow **inward**: the interface and infrastructure layers depend on the application layer, which depends on the domain layer.

| Layer | Responsibility | Examples |
|-------|----------------|----------|
| **Domain** | Business logic, invariants, validation | Models, commands, value objects |
| **Application** | Orchestration, contracts, composition | Usecases, ports, execution runtime |
| **Infrastructure** | Concrete adapter implementations | Postgres, Redis, S3, MongoDB |
| **Interface** | User-facing entry points | FastAPI routes, Socket.IO handlers |

Read more in [Layered Architecture](core-concepts/layered-architecture.md).

## Package layout

| Package | Purpose |
|---------|---------|
| `forze` | Core contracts, execution runtime, composition helpers, domain primitives |
| `forze_fastapi` | HTTP router helpers and idempotent route integration |
| `forze_postgres` | Postgres-backed document, search, and transaction adapters |
| `forze_redis` | Cache, counters, idempotency, pub/sub, and stream adapters |
| `forze_s3` | S3-compatible storage adapter |
| `forze_mongo` | Mongo-backed document and transaction adapters |
| `forze_socketio` | Socket.IO transport adapter for typed realtime events |
| `forze_temporal` | Temporal workflow integration (scaffolding) |
| `forze_sqs` | SQS message queue adapter |
| `forze_rabbitmq` | RabbitMQ message queue adapter |

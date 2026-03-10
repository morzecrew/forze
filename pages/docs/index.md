---
title: Welcome to forze
---

**Forze** is a Python toolkit for building backend services with clear boundaries:
domain-first models, application-level orchestration, and replaceable infrastructure adapters.

If you are new to the package, start with:

1. [Installation](installation.md)
2. [Getting Started](getting-started.md)
3. [Core Concepts](core-concepts/index.md)

## Why use Forze?

Forze helps you keep business logic stable while storage/framework choices evolve.

- **Layered** — clear separation between domain, application, and infrastructure
- **Explicit** — contracts (ports) describe what the app needs
- **Composable** — adapters are wired declaratively via dependency plans
- **Testable** — usecases can run with fake/in-memory dependencies
- **Framework-agnostic** — core modules are not tied to FastAPI/Postgres/etc.

!!! note ""
    Forze is not a full-stack framework. It provides architecture primitives and integration packages you compose.

## Package layout

| Package | Purpose |
|---------|---------|
| `forze` | Core contracts, execution runtime, composition helpers, domain primitives |
| `forze_fastapi` | HTTP router helpers and idempotent route integration |
| `forze_postgres` | Postgres-backed document/search/transaction adapters |
| `forze_redis` | Cache, counters, and idempotency adapters |
| `forze_s3` | S3-compatible storage adapter |
| `forze_mongo` | Mongo-backed document/transaction adapters |
| `forze_socketio` | Socket.IO transport adapter for typed realtime events |
| `forze_temporal` | Temporal integration package (currently minimal) |
| `forze_sqs` | Amazon SQS message queue adapter |
| `forze_rabbitmq` | RabbitMQ message queue adapter |

## Typical request flow

<div class="d2-diagram">
  <img class="d2-light" src="assets/diagrams/light/contracts-adapters.svg" alt="Request flow from usecase to adapters">
  <img class="d2-dark" src="assets/diagrams/dark/contracts-adapters.svg" alt="Request flow from usecase to adapters">
</div>

In practice: router/handler resolves an `ExecutionContext`, usecases request ports (`ctx.doc_read(...)`, `ctx.doc_write(...)`, `ctx.search(...)`, `ctx.storage(...)`), and adapters execute infrastructure work.
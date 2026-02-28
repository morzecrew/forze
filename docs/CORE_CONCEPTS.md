# Core Concepts

This document explains the foundational ideas behind Forze: how it structures backend services, what patterns it uses, and why they matter. It is intended for both **users** integrating Forze into their projects and **contributors** extending or maintaining the library.

The concepts described here are meant to be **stable** — they reflect the design philosophy and architectural choices that persist even as the implementation evolves. Specific class names, module paths, or API details may change; the underlying ideas should not.

## Overview

Forze is a **structural foundation** for backend services. It does not impose a specific framework or database; instead, it provides:

- **Layered architecture** — domain, application, and infrastructure with clear boundaries
- **Contracts and adapters** — interfaces that decouple business logic from technology choices
- **Composable operations** — business logic as first-class, testable units
- **Declarative configuration** — plans and specs instead of imperative wiring

The result: you can swap databases, add caching, or change web frameworks without rewriting business logic.

## Layered Architecture

Forze organizes code into three layers. Dependencies flow **inward**: infrastructure depends on application, application depends on domain. The domain layer has no external dependencies.

```
┌─────────────────────────────────────────────────────────────┐
│  Infrastructure (adapters)                                    │
│  Databases, caches, storage, web frameworks, workflows        │
└───────────────────────────┬─────────────────────────────────┘
                            │ implements
┌───────────────────────────▼─────────────────────────────────┐
│  Application (operations, orchestration, contracts)           │
│  Use cases, execution runtime, dependency resolution         │
└───────────────────────────┬─────────────────────────────────┘
                            │ uses
┌───────────────────────────▼─────────────────────────────────┐
│  Domain (models, invariants, validation)                      │
│  Business rules, value objects, no infrastructure             │
└─────────────────────────────────────────────────────────────┘
```

**Why it matters:** Each layer has a clear responsibility. Domain logic stays pure and testable. Application orchestrates without knowing storage details. Infrastructure can be swapped or extended without touching business rules.

## Domain Layer

The domain layer holds **business logic and invariants**. It knows nothing about databases, HTTP, or external services.

### Versioned Entities

The core domain model is built around **versioned entities** — aggregates that track revisions, timestamps, and identity. Updates produce new revisions rather than mutating in place. Immutable fields (identity, creation time, revision) stay fixed; only what is meant to change can change.

This supports optimistic concurrency, audit trails, and clear lifecycle semantics.

### Value Objects and Commands

Domain inputs and outputs use **value objects** — immutable data structures that carry meaning. Commands for creating or updating entities are value objects, not raw dictionaries. Read models (what consumers see) may differ from domain models (what invariants enforce). This separation allows query optimization and write consistency to evolve independently.

### Pluggable Validation

Domain logic can declare **update validators** — hooks that run when an entity is updated. They receive previous state, new state, and the patch, and can enforce invariants or reject invalid changes. Invariants stay close to the model instead of scattering across use cases.

### Composable Concerns

Reusable domain concerns (soft deletion, naming, numbering, etc.) are provided as **mixins**. They combine without deep inheritance hierarchies. You compose what you need.

**Why it matters:** Business rules live in one place. The domain is testable in isolation. Changes to infrastructure do not ripple into business logic.

## Application Layer

The application layer **orchestrates** domain logic and coordinates infrastructure. It defines *what* happens, not *how* persistence or transport work.

### Operations (Use Cases)

An **operation** is a single, well-defined business action. It takes arguments and returns a result. Operations are the primary unit of business logic in the application layer.

Operations support composition via:

- **Guards** — Run before execution; validate or enrich arguments
- **Effects** — Run after execution; side effects like logging, indexing, or events
- **Middlewares** — Wrap execution; cross-cutting concerns like retries or metrics

Composition is **immutable**: adding a guard or effect returns a new instance. This makes it easy to build variants (e.g. “create with audit effect”) without mutating shared objects.

**Transactional operations** extend this with explicit transaction boundaries. They can have **side guards** and **side effects** that run outside the transaction — for example, sending a notification only after the transaction commits.

### Execution Runtime

The **execution runtime** is the runnable scope where operations run. It combines three things:

1. **Dependency plan** — A declarative description of what dependencies exist and how to build them
2. **Lifecycle plan** — Startup and shutdown hooks, executed in order
3. **Execution context** — The runtime environment passed to operations, providing dependency resolution and transaction management

When you enter the runtime scope, the context is created, lifecycle startup runs (e.g. connect to databases, warm caches), and operations can execute. On exit, lifecycle shutdown runs (e.g. close connections) and the context is torn down.

**Why it matters:** Dependencies and lifecycle are configured once, declaratively. Operations receive a context and resolve what they need. No global state, no hidden coupling.

### Dependency Plan

The **dependency plan** describes how to build the dependency container. It is a list of modules — each module produces a set of dependencies. Plans can be composed (e.g. “base deps + database deps + cache deps”). The runtime builds the final container from the plan before any operation runs.

Dependencies are **not** limited to contracts (ports). The container can hold anything: raw database clients, custom services built on top of ports, contract implementations, or parameterized factories. For example, a Postgres module might register the connection client, a types provider, a transaction manager, and a document storage factory — each resolved by its own key. Operations resolve what they need from the context.

**Dependency routers** handle cases where resolution depends on a parameter (e.g. which aggregate) or when the project uses multiple adapters for the same contract. A router selects the right implementation based on a specification — for instance, routing document storage to different databases depending on the aggregate type. This keeps the dependency plan declarative while supporting complex, multi-adapter setups.

### Operation Registry and Plan

The **operation registry** maps logical operation names (e.g. “get”, “create”, “search”) to factories that produce operations. When you ask for an operation by name, the registry looks up the factory, builds the operation, and returns it.

The **operation plan** describes how each operation should be composed — which guards, effects, and middlewares wrap it, and in what order. Plans are keyed by operation name and can be merged or extended. A base plan might add logging to all operations; a specific plan might add authorization to “create” only.

When the registry resolves an operation, it applies the plan: it builds the base operation, then wraps it with the configured guards, effects, and middlewares. The result is a fully composed operation ready to run.

Plans support **overrides** — replacing the default implementation of an operation with a custom one — and **priorities** — controlling the order of guards and effects when multiple plans are merged.

**Why it matters:** Operations are registered once; composition is declared in plans. You can add auditing, idempotency, or custom behavior by extending the plan, without touching the core operation. The registry and plan are the composition backbone.

## Contracts and Adapters (Hexagonal Architecture)

**Contracts** (also called ports) are interfaces defined by the application. **Adapters** are implementations provided by infrastructure. The application depends on contracts, not adapters.

Typical contracts include:

- **Storage** — Read, write, search for domain aggregates
- **Transaction boundary** — Begin, commit, rollback
- **Cache** — Get, set, invalidate
- **Blob storage** — Store and retrieve files
- **Counters** — Distributed increment
- **Idempotency** — Track and deduplicate requests
- **Streams** — Publish and consume events
- **Workflows** — Orchestrate long-running processes

Each contract has one or more adapters (Postgres, Redis, S3, etc.). The dependency plan wires adapters to contracts. Switching from Postgres to Mongo means changing the plan, not the operations.

### Aggregate Specification

For document-like aggregates, a **specification** binds together: the namespace used for caching, the storage relations (tables, views), the model types (read, domain, commands), and optional features (search, soft delete, caching). Adapters use the spec to configure themselves.

You define the spec once. Switching adapters means swapping the implementation, not rewriting the spec.

**Why it matters:** Business logic talks to contracts. Infrastructure implements them. Technology choices stay at the edges. Tests stub contracts with in-memory or fake implementations.

Forze gives you a **structural foundation** — concepts and patterns that keep backend code clean, modular, and maintainable as it grows. The specifics may evolve; the ideas endure.

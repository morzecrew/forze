---
title: Glossary
icon: lucide/book-open
summary: Quick reference for Forze terminology
---

## Core concepts

**Aggregate**
:   A business entity with identity, versioning, and invariants. Subclass `Document` to create one; add the `AggregateRoot` mixin when you need domain events.

**Adapter**
:   A concrete implementation of a port for a specific backend — `PostgresDocumentAdapter` implements the document port for PostgreSQL.

**Execution context**
:   The runtime object that resolves ports to adapters on demand. Handlers receive it and request capabilities without knowing which adapter backs them.

**Operation**
:   A single business action — create a user, list orders, delete a document. Operations are registered in an operation registry and resolved at runtime.

**Operation registry**
:   The frozen catalog mapping operation names to their handlers and hooks. Built with `build_document_registry()` and locked with `freeze()`.

**Port**
:   A contract describing a capability the application needs — storage, cache, search, messaging. Ports live in the application layer; adapters implement them in infrastructure.

**Specification (Spec)**
:   The logical name (`"users"`) that binds a model to its operations and adapters — `DocumentSpec`, `SearchSpec`, `CacheSpec`, and so on.

---

## Spec types

**DocumentSpec**
:   Declares a document aggregate with CRUD operations. Binds domain model, create command, and read model to a logical name.

**SearchSpec**
:   Declares a search index for full-text or structured queries over documents.

**CacheSpec**
:   Declares a cache layer for read-through or write-through caching.

**QueueSpec**
:   Declares a message queue for async task dispatch.

**StreamSpec**
:   Declares a pub/sub stream for event distribution.

**GraphSpec**
:   Declares a graph data model for relationship queries.

---

## Model types

**Domain model**
:   The business entity with behavior and invariants. Subclass `Document` for identity, revision, and timestamps.

**Create command**
:   The frozen input for `POST` operations. Use `BaseDTO` (the `CreateDocumentCmd` alias is deprecated).

**Update command**
:   Partial update payload for `PATCH` operations. Typically a `BaseDTO` with optional fields.

**Read model**
:   The frozen projection returned from `GET` operations. May include computed fields not stored in the domain model.

**Domain event**
:   A record of something that happened in the domain. Subclass `DomainEvent` for `event_id`, `occurred_at`, and payload.

---

## Runtime and wiring

**DepsModule**
:   A module that registers adapters for specs. Examples: `PostgresDepsModule`, `RedisDepsModule`, `MockDepsModule`.

**DepsRegistry**
:   The container holding all registered deps modules and their wiring.

**ExecutionRuntime**
:   The top-level runtime that manages lifecycle, builds contexts, and coordinates shutdown.

**Lifecycle plan**
:   The dependency graph that determines startup and shutdown order for adapters.

**Route (wiring)**
:   The mapping from a spec name to its adapter configuration. Not to be confused with HTTP routes.

**Transaction route**
:   A transactional boundary that groups operations. Configured per-request or per-saga step.

**Isolation level**
:   The transaction isolation an operation requires — `READ_COMMITTED < SNAPSHOT < SERIALIZABLE`. Declared with `set_isolation(...)` and enforced fail-closed against the route's transaction manager.

---

## Identity and tenancy

**AuthnIdentity**
:   The verified identity of a request — who is making the call.

**AuthzScope**
:   The permissions associated with an identity — what they're allowed to do.

**TenantIdentity**
:   The tenant context bound to a request. Determines data isolation strategy.

**Tenant tier**
:   The isolation level for a tenant: `tagged` (marker column), `namespace` (schema/prefix), or `dedicated` (separate instance).

---

## Patterns

**CQRS**
:   Command Query Responsibility Segregation. Forze's query/command port split follows this pattern — writes go through command ports, reads through query ports.

**OCC**
:   Optimistic Concurrency Control. Forze uses revision numbers to detect concurrent modifications.

**Outbox**
:   A transactional pattern where events are written to a local table in the same transaction as state changes, then relayed to external systems.

**Inbox**
:   The receiving side of outbox — deduplicated message processing with exactly-once semantics.

**Saga**
:   A sequence of operations across aggregates or services, with compensation logic for rollback.

---

## See also

- [Overview](../core-concepts/overview.md) — the mental model and vocabulary
- [Architecture](../core-concepts/architecture.md) — the four-layer structure
- [Contracts reference](contracts.md) — capability index by contract type

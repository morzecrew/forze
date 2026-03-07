# Layered Architecture

Forze organizes code into three layers with strict dependency rules. Dependencies flow **inward**: infrastructure depends on application, application depends on domain. The domain layer has no external dependencies.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/layered-architecture.svg" alt="Layered architecture">
  <img class="d2-dark" src="../../assets/diagrams/dark/layered-architecture.svg" alt="Layered architecture">
</div>

## Layer Responsibilities

| Layer | Responsibility | Dependencies |
|-------|----------------|--------------|
| **Domain** | Business logic, invariants, validation rules | None |
| **Application** | Orchestration, use cases, contracts (ports) | Domain |
| **Infrastructure** | Databases, caches, HTTP, external services | Application |

## Why It Matters

- **Domain logic stays pure and testable** — no database or HTTP concerns
- **Application orchestrates without knowing storage details** — it calls contracts, not implementations
- **Infrastructure can be swapped or extended** — change Postgres to Mongo by replacing adapters, not operations

## Dependency Rule

The dependency rule is enforced by design: the domain layer does not import from application or infrastructure. The application layer defines interfaces (contracts) and depends only on those abstractions. Infrastructure provides concrete implementations that satisfy the contracts.

# Core Concepts

This section explains the architectural model behind Forze: layered boundaries, domain primitives, the execution runtime, and port/adapter composition.

## When to read this section

- You need to understand where business logic should live
- You want to decide how to wire adapters and runtime hooks
- You are adding integrations without coupling domain logic to infrastructure
- You want to extend operations with guards, effects, or transaction middleware

## Architectural guarantees

Class names may evolve, but these invariants are stable:

- Dependencies flow **inward** -- infrastructure depends on application, application depends on domain
- Usecases resolve dependencies from `ExecutionContext`, never by importing adapters
- Integrations implement contracts (ports); the application depends on abstractions

## Mental model

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/layered-architecture.svg" alt="Layered architecture overview">
  <img class="d2-dark" src="../../assets/diagrams/dark/layered-architecture.svg" alt="Layered architecture overview">
</div>

## Section contents

| Page | What it covers |
|------|---------------|
| [Layered Architecture](layered-architecture.md) | Dependency rules and responsibilities by layer |
| [Domain Layer](domain-layer.md) | Document model, versioning, update semantics, mixins, validation |
| [Application Layer](application-layer.md) | Usecases, middleware chains, execution runtime, dependency and lifecycle plans |
| [Contracts and Adapters](contracts-adapters.md) | Port protocols, adapter implementations, dependency wiring |
| [Aggregate Specification](aggregate-specification.md) | `DocumentSpec`, `SearchSpec`, and how adapters consume them |
| [Usecase Composition](usecase-composition.md) | Registries, plans, guards, effects, transaction middleware |

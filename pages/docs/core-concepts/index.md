# Core Concepts

This section explains the architectural model behind Forze: layered boundaries, execution runtime, and port/adapter composition.

## Read this section when you need to

- understand where business logic should live
- decide how to wire adapters and runtime hooks
- add integrations without coupling domain logic to infrastructure

## What stays stable

Class names may evolve, but these guarantees are stable:

- dependencies flow inward (infra -> application -> domain)
- usecases resolve dependencies from `ExecutionContext`
- integrations implement contracts (ports), not the other way around

## Mental model in one diagram

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/layered-architecture.svg" alt="Layered architecture overview">
  <img class="d2-dark" src="../../assets/diagrams/dark/layered-architecture.svg" alt="Layered architecture overview">
</div>

## Documentation map

| Document | Contents |
|----------|----------|
| [Layered Architecture](layered-architecture.md) | Dependency rules and responsibilities by layer |
| [Domain Layer](domain-layer.md) | Domain model behavior, updates, invariants, mixins |
| [Application Layer](application-layer.md) | Usecases, plans, runtime scope, composition model |
| [Contracts and Adapters](contracts-adapters.md) | Ports, adapters, dependency wiring strategy |
| [Aggregate Specification](aggregate-specification.md) | `DocumentSpec` and how adapters use it |

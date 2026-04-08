---
title: Overview
summary: Architectural model behind Forze
---

This section explains the architectural model behind Forze: layered boundaries, domain primitives, the execution runtime, and port/adapter composition.

## When to read this section

- You need to understand where business logic should live
- You want to decide how to wire adapters and runtime hooks
- You are adding integrations without coupling domain logic to infrastructure
- You want to extend operations with guards, effects, or transaction middleware

## Architectural guarantees

Class names may evolve, but these invariants are stable:

- Dependencies flow **inward**: interface and infrastructure depend on application, application depends on domain
- Usecases resolve dependencies from execution context, never by importing adapters
- Integrations implement contracts (ports); the application depends only on abstractions

## Section contents

| Page | What it covers |
|------|---------------|
| [Layered Architecture](layered-architecture.md) | Four layers, dependency rules, and responsibilities |
| [Domain Layer](domain-layer.md) | Document model, versioning, update semantics, mixins, validation |
| [Application Layer](application-layer.md) | Usecases, middleware chains, execution runtime, dependency<br>and lifecycle plans |
| [Contracts and Adapters](contracts-adapters.md) | Port protocols, adapter implementations, dependency wiring |
| [Specs and infrastructure wiring](specs-and-wiring.md) | Kernel specs vs integration configs, routed dependency keys |
| [Aggregate Specification](aggregate-specification.md) | How to define aggregates and how adapters consume them |
| [Usecase Composition](usecase-composition.md) | Registries, plans, guards, effects, transaction middleware |

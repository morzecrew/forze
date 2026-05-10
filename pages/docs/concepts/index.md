---
title: Overview
summary: Architectural model behind Forze
---

## What problem this solves

Architecture can feel abstract when you only want to ship a backend feature. This section gives names to the boundaries Forze enforces so code stays maintainable as projects grow.

## When you need this

Read these pages when you are choosing where code belongs, wiring adapters, or extending execution behavior.


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
| [Authentication Pipeline](authentication.md) | Verify-then-resolve seam, ``VerifiedAssertion``, principal resolvers, UUID-native rationale |
| [Multi-tenancy](multi-tenancy.md) | Authn vs tenant identity, bootstrap, JWT ``tid``, reference package |
| [Specs and infrastructure wiring](specs-and-wiring.md) | Kernel specs vs integration configs, routed dependency keys |
| [Aggregate Specification](aggregate-specification.md) | How to define aggregates and how adapters consume them |
| [Usecase Composition](usecase-composition.md) | Registries, plans, guards, effects, transaction middleware |

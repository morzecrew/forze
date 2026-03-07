# Core Concepts

This section explains the foundational ideas behind Forze: how it structures backend services, what patterns it uses, and why they matter. It is intended for both **users** integrating Forze into their projects and **contributors** extending or maintaining the library.

The concepts described here are **stable** — they reflect the design philosophy and architectural choices that persist even as the implementation evolves. Specific class names, module paths, or API details may change; the underlying ideas should not.

## What Forze Provides

Forze is a **structural foundation** for backend services. It does not impose a specific framework or database. Instead, it provides:

| Concept | Description |
|---------|-------------|
| **Layered architecture** | Domain, application, and infrastructure with clear boundaries<br>and inward dependency flow |
| **Contracts and adapters** | Interfaces that decouple business logic from technology choices |
| **Composable operations** | Business logic as first-class, testable units with guards, effects,<br>and middlewares |
| **Declarative configuration** | Plans and specs instead of imperative wiring |

The result: you can swap databases, add caching, or change web frameworks without rewriting business logic.

## Documentation Structure

| Document | Contents |
|----------|----------|
| [Layered Architecture](layered-architecture.md) | Three-layer structure, dependency flow, and layer responsibilities |
| [Domain Layer](domain-layer.md) | Versioned entities, value objects, validation, and mixins |
| [Application Layer](application-layer.md) | Operations, execution runtime, dependency plan, and operation registry |
| [Contracts and Adapters](contracts-adapters.md) | Hexagonal architecture, ports, and available contracts |
| [Aggregate Specification](aggregate-specification.md) | Document spec structure and adapter configuration |

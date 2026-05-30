---
title: Home
summary: Domain-Driven Design and Hexagonal Architecture for backend services
---

# Welcome to Forze <!-- markdownlint-disable-line -->

**Forze** is a Python toolkit for building backend services with clear boundaries:
domain-first models, application-level orchestration, and replaceable infrastructure adapters.

If you are new to the package, start with:

## Why use Forze?

Forze helps you keep business logic stable while storage and framework choices evolve.

- **Layered** — four clean layers (domain, application, infrastructure, interface) with strict dependency rules
- **Explicit** — contracts (ports) describe what the app needs; adapters deliver it
- **Composable** — adapters are wired declaratively via dependency plans
- **Testable** — handlers run with fake or in-memory dependencies
- **Framework-agnostic** — core modules are not tied to any web framework, database, or cloud service

Forze is not a full-stack framework. It provides architecture primitives and integration packages you compose.

## Architecture at a glance

Forze organizes code into four layers. Dependencies flow **inward**: the interface and infrastructure layers depend on the application layer, which depends on the domain layer.

| Layer | Responsibility | Examples |
|-------|----------------|----------|
| **Domain** | Business logic, invariants, validation | Models, commands, value objects |
| **Application** | Orchestration, contracts, composition | Handlers, ports, execution runtime |
| **Infrastructure** | Concrete adapter implementations | Postgres, Redis, S3, MongoDB |
| **Interface** | User-facing entry points | FastAPI routes, Socket.IO handlers |

<!--
![Getting started flow](/_diagrams/light/getting-started-flow.svg#only-light){ loading=lazy }
![Getting started flow](/_diagrams/dark/getting-started-flow.svg#only-dark){ loading=lazy }
-->

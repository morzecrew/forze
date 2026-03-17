# Forze

[![PyPI](https://img.shields.io/pypi/v/forze?label=PyPI)](https://pypi.org/project/forze/) [![Python](https://img.shields.io/pypi/pyversions/forze)](https://pypi.org/project/forze/) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/morzecrew/forze/badge)](https://scorecard.dev/viewer/?uri=github.com/morzecrew/forze) [![CodeFactor](https://www.codefactor.io/repository/github/morzecrew/forze/badge)](https://www.codefactor.io/repository/github/morzecrew/forze)

**Forze** is a lightweight infrastructure toolkit for building backend services
with **Domain-Driven Design (DDD)** and **Hexagonal Architecture**.

It provides a set of reusable primitives, contracts, and structural patterns
that help organize backend applications into clear, maintainable layers.

## Design Goals

Forze aims to support backend systems that are:

- **Layered** — clear separation between domain, application, and infrastructure
- **Explicit** — well-defined ports, adapters, and boundaries
- **Testable** — components can be tested in isolation
- **Composable** — infrastructure pieces can be replaced or extended
- **Framework-agnostic** — the core does not depend on a specific framework

The library focuses on providing **structure and contracts**, not a full-stack framework.

## Quick Start

Install the core package:

```bash
uv add forze
```

Install with optional integrations:

```bash
uv add 'forze[fastapi,postgres,socketio]'
```

## Documentation

Full documentation is available at [https://morzecrew.github.io/forze/](https://morzecrew.github.io/forze/).

## Agent Skills

Forze ships with AI agent skills that help assistants understand the framework's architecture, patterns, and conventions. Install them to improve code generation and refactoring when working with Forze.

Skills follow the [Agent Skills](https://agentskills.io/) format.

### Installation

```bash
# Install all skills
npx skills add morzecrew/forze

# Install a specific skill
npx skills add morzecrew/forze@forze-wiring
```

### Usage

Skills are automatically available once installed. The agent will use them when relevant tasks are detected.

### Available Skills

#### `forze-wiring`

Wire Forze runtime, dependency plan, lifecycle, usecase composition, and interface layer (e.g. FastAPI). Covers Deps.merge, LifecyclePlan, ExecutionRuntime, document/search registries, and FastAPI routers with context dependency.

Use when:

- Setting up the application bootstrap
- Configuring adapters and dependency plan
- Exposing endpoints (FastAPI routers)
- Building document or search composition
- Wiring lifecycle (startup/shutdown of connection pools)

Categories covered:

- Runtime & Lifecycle (Critical) — Deps, ExecutionRuntime, scope, connection pools
- Composition (High) — Document registry, usecase plans, middleware, transactions
- FastAPI Integration (High) — Routers, context dependency, lifespan
- Testing (Medium) — Mock adapters for local development

#### `forze-framework-usage`

Write code that uses the Forze framework correctly. Covers layered architecture, contracts vs adapters, ExecutionContext port resolution, usecase pattern, transactions, and query/storage patterns.

Use when:

- Implementing features or usecases
- Integrating Forze into an application
- Writing code that uses Forze (not developing new adapters)
- Working with ports, context, transactions, or query syntax

Categories covered:

- Architecture (Critical) — Layered boundaries, dependency flow inward
- Contracts & Ports (Critical) — Resolve ports via context, never import adapters
- Usecases (High) — Usecase pattern, transactions, port resolution
- Query & Storage (Medium) — CRUD, search, cache, counter, object storage patterns

#### `forze-domain-aggregates`

Define domain models, document aggregates, and specifications for Forze. Covers Document, CreateDocumentCmd, UpdateCmd, ReadModel, mixins, DocumentSpec, SearchSpec, and database schema alignment.

Use when:

- Creating entities, models, or domain objects
- Defining document aggregates
- Creating specs (DocumentSpec, SearchSpec)
- Creating DTOs (CreateCmd, UpdateCmd, ReadModel)

Categories covered:

- Aggregate Models (Critical) — Document, CreateCmd, UpdateCmd, ReadModel structure
- Specifications (Critical) — DocumentSpec, SearchSpec binding to storage and cache
- Domain Validation (Medium-High) — Update validators, mixins (SoftDeletion, Number, Creator)
- Schema Alignment (Medium) — DB schema matching Pydantic fields

## Versioning

Forze follows [Semantic Versioning (SemVer)](https://semver.org/).
Pre-release builds may include experimental APIs and are not guaranteed to be stable.

## Contributing

Contributions, issues, and feature requests are welcome.
See [CONTRIBUTING.md](https://github.com/morzecrew/forze/blob/main/CONTRIBUTING.md) for details.

## Security

Please report security vulnerabilities privately as described in [SECURITY.md](https://github.com/morzecrew/forze/blob/main/SECURITY.md).

## License

Forze is licensed under the MIT License - see [LICENSE](https://github.com/morzecrew/forze/blob/main/LICENSE) for details.

# Forze

[![PyPI](https://img.shields.io/pypi/v/forze?label=PyPI)](https://pypi.org/project/forze/) [![Python](https://img.shields.io/pypi/pyversions/forze)](https://pypi.org/project/forze/) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/morzecrew/forze/badge)](https://scorecard.dev/viewer/?uri=github.com/morzecrew/forze)

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

**Install:**

```bash
npx skills add morzecrew/forze
```

**Available skills:**

| Skill | Description |
|-------|-------------|
| `forze-architecture` | Understand the Forze framework's DDD/hexagonal architecture, layer boundaries, and import constraints. Use when creating new modules, refactoring code, or reviewing architecture compliance. |
| `forze-application-layer` | Work with Forze's application layer including usecases, middleware, CQRS, composition, execution context, and facades. Use when creating usecases, configuring middleware, or setting up document/storage/search operations. |
| `forze-domain-modeling` | Create and work with Forze domain models including Document, CoreModel, BaseDTO, mixins, and update validators. Use when defining new entities, value objects, or domain logic. |
| `forze-ports-and-adapters` | Work with Forze's ports (contracts), specs, dependency injection, and adapter patterns. Use when implementing new adapters, defining ports, configuring specs, or wiring dependencies. |
| `forze-fastapi-integration` | Build HTTP APIs with Forze's FastAPI integration including routers, document routes, route features, error handling, and dependency injection. Use when creating API endpoints, configuring routers, or integrating Forze with FastAPI. |

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

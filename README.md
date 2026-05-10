# Forze

[![PyPI](https://img.shields.io/pypi/v/forze?label=PyPI)](https://pypi.org/project/forze/)
[![Python](https://img.shields.io/pypi/pyversions/forze)](https://pypi.org/project/forze/)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/morzecrew/forze/badge)](https://scorecard.dev/viewer/?uri=github.com/morzecrew/forze)
[![CodeFactor](https://www.codefactor.io/repository/github/morzecrew/forze/badge)](https://www.codefactor.io/repository/github/morzecrew/forze)
[![codecov](https://codecov.io/github/morzecrew/forze/graph/badge.svg?token=WIKAC2IUS9)](https://codecov.io/github/morzecrew/forze)

**Forze** is a lightweight infrastructure toolkit for building backend services
with Domain-Driven Design and Hexagonal Architecture.

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

Authentication extras:

```bash
# First-party authn (Argon2 / PyJWT / email-validator)
uv add 'forze[authn]'

# Generic OIDC verifier (RS256/ES256/HS256, JWKS) for external IdPs
uv add 'forze[oidc]'
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

| Name | Description |
| -------- | -------- |
| **forze-framework-usage** | ExecutionContext, ports, direct dep-key resolution, identity, after-commit hooks, and transactions in usecases. |
| **forze-documents-search** | Document ports, query DSL, cache-aware document specs, search specs, and Postgres/Mongo/mock search behavior. |
| **forze-domain-aggregates** | Document aggregates, mixins, validators, kernel `DocumentSpec` / `SearchSpec`, and composition DTOs. |
| **forze-wiring** | Runtime, deps modules, lifecycle, document/search composition, `UsecasePlan`, FastAPI endpoints, mapping. |
| **forze-specs-infrastructure** | Mapping logical `StrEnum` spec names to Postgres/Mongo/Redis/S3/queue/workflow configs and routes. |
| **forze-deps-modules** | Custom dependency keys, `Deps`, routed/plain registrations, lifecycle separation, and `DepsModule` authoring. |
| **forze-fastapi-interface** | FastAPI context dependencies, document/search/custom endpoints, middleware, idempotency, ETags, forms, and docs. |
| **forze-storage-s3** | `StorageSpec`, `StoragePort`, `S3DepsModule`, tenant-aware buckets, lifecycle, and storage tests. |
| **forze-messaging-streaming** | Queue, pub/sub, stream contracts, SQS/RabbitMQ wiring, Redis adapters, and mock messaging tests. |
| **forze-temporal-workflows** | `WorkflowSpec`, Temporal deps, workflow command/query ports, lifecycle, context propagation, and tests. |
| **forze-auth-tenancy-secrets** | Authn (verify-then-resolve, AuthnSpec, AuthnDepsModule), authz contracts, identity binding, tenant-aware routing, secrets, FastAPI resolvers, and external IdP wiring (`forze_oidc`). |
| **forze-graph-contracts** | Graph module/node/edge specs, graph refs, query/command ports, and custom graph adapter wiring. |
| **forze-observability-errors** | Structured `CoreError` handling, adapter exception mapping, logging, call context, and FastAPI error responses. |

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

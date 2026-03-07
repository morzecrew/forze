# Forze

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
uv add 'forze[fastapi,postgres]'
```

## Versioning

Forze follows [Semantic Versioning (SemVer)](https://semver.org/). 
Pre-release builds may include experimental APIs and are not guaranteed to be stable.

## Contributing

Contributions, issues, and feature requests are welcome. 
See [CONTRIBUTING.md](./CONTRIBUTING.md) for details.

## Security

Please report security vulnerabilities privately as described in [SECURITY.md](./SECURITY.md).

## License

Forze is licensed under the MIT License - see [LICENSE](./LICENSE) for details.
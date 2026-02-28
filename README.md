# Forze

**Forze** is a lightweight infrastructure library for building backend services
using **Domain-Driven Design (DDD)** and **Hexagonal Architecture**.

It provides reusable primitives, contracts, and structural patterns
for building clean, modular, and maintainable backend systems.

## Philosophy

Forze focuses on:

- **Clear separation of concerns** — domain, application, and infrastructure layers
- **Explicit contracts and boundaries** — well-defined ports and adapters
- **Predictable dependency flow** — dependencies point inward toward the domain
- **Testability and composability** — easy to mock, extend, and test in isolation
- **Framework-agnostic core** — structure without lock-in

It is not just a framework; it is a structural foundation.

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or another PEP 517–compatible package manager

## Quick Start

Install via `uv`:

```bash
export UV_INDEX_PYOCI_USERNAME="github"
export UV_INDEX_PYOCI_PASSWORD=$(gh auth token)

uv add forze --index pyoci=https://pyoci.com/ghcr.io/morzecrew/
```

With optional extras (e.g. FastAPI, Postgres):

```bash
uv add 'forze[fastapi,postgres]' --index pyoci=https://pyoci.com/ghcr.io/morzecrew/
```

Authentication is required because the package is hosted in a private registry.
See the [Installation Guide](./docs/INSTALLATION.md) for details.

## Documentation

### Installation and Setup

- [📦 Installation Guide](./docs/INSTALLATION.md)
- [🐳 Using Forze in Docker](./docs/DOCKER.md)

### Architecture

- [🧠 Core Concepts](./docs/CORE_CONCEPTS.md)
- 🏗 (coming soon) Module Structure
- 🔌 (coming soon) Ports & Adapters
- 📚 (coming soon) Example Service

## Versioning

Forze follows [Semantic Versioning (SemVer)](https://semver.org/). Pre-release builds may include experimental APIs and are not guaranteed to be stable.
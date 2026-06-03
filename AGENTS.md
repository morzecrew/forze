# AGENTS.md

Routing guide for AI agents working in this repository.

This file is not the source of truth for project policy. It tells agents where
to look first for authoritative rules before editing code, tests, docs, or release assets.

## Project overview

Forze is a Python library for Domain-Driven Design and Hexagonal Architecture in backend services. It provides core framework layers (application, domain, utils, base) and optional integrations (FastAPI, Postgres, Redis, S3, Temporal, Mongo, RabbitMQ, Socket.IO, SQS). Development validation means running tests and quality checks—there is no runnable application or server to start.

## Agent workflow (quick checklist)

1. Identify task type (code, tests, docs, release, security, CI).
2. Open the canonical file(s) from the map below.
3. Apply the smallest scoped change in existing files when possible.
4. Run the relevant checks from `justfile`.
5. Keep tests/docs/changelog aligned when behavior changes.

## Source of truth map

### Contribution process and conventions

Read:
- `CONTRIBUTING.md`

Use it for:
- branch and contribution flow
- commit and PR title format (Conventional Commits + gitmoji)
- test expectations
- changelog and release preparation

### Architecture, packaging, and tool config

Read:
- `pyproject.toml`

Use it for:
- Python version and package metadata
- dependency groups and optional extras
- pytest and coverage configuration
- import-linter contracts and layering constraints
- lint/static-analysis tool configuration

### Commands and local quality gates

Read:
- `justfile`

Use it for:
- test entrypoints (`just test`, path-scoped tests)
- performance testing (`just perf`, performance benchmarks)
- quality checks (`just quality`, strict mode)
- security/dependency/dead-code checks

### Documentation structure and docs build

Read:
- `pages/mkdocs.yml`
- `pages/justfile`

Use them for:
- docs navigation and page structure
- mkdocs/mkdocstrings behavior
- docs build and serving commands

### Security handling

Read:
- `SECURITY.md`

Use it for:
- vulnerability reporting workflow
- disclosure expectations
- handling of security-sensitive fixes

### Code and tests behavior

Read:
- `src/`
- `tests/`

Use them for:
- runtime behavior and API contracts
- architecture boundaries in real code
- fixture conventions and test patterns

## Repository map (high signal paths)

- `src/forze/`: core framework layers (application/domain/utils/base).
- `src/forze_kits/`: pre-built wiring above contracts (`domain/`; `aggregates/` with per-aggregate `handlers/`; `mapping/`; `dto/`; `integrations/` outbox + notify; `adapters/` secrets; `scopes/` e.g. `DistributedLockScope`). Core `forze.application` keeps contracts, execution, hooks, integrations only—must not import `forze_kits`. Not `forze_identity` (separate plane).
- `src/forze_identity/`: identity plane (`authn/`, `authz/`, `tenancy/`, `oidc/`, `oauth/` subpackages; import as `forze_identity.authn`, etc.).
- `src/forze_identity/builtin/`: shipped-in identity presets (`local/` file/env API keys, `idp/` Google/VK/Telegram Login OIDC); not for production unless you accept each preset's trust model.
- `src/forze_fastapi/`: FastAPI integration package.
- `src/forze_postgres/`: Postgres integration package.
- `src/forze_redis/`: Redis integration package.
- `src/forze_s3/`: S3 integration package.
- `src/forze_gcs/`: Google Cloud Storage integration package.
- `src/forze_temporal/`: Temporal integration package.
- `src/forze_inngest/`: Inngest integration package (durable functions).
- `src/forze_mongo/`: Mongo integration package.
- `src/forze_mock/`: in-memory mock adapters (`MockState`, optional `MockRoutedStateRegistry`, `tenancy/`, `execution/` deps module with identity/durable/search/dlock stubs).
- `src/forze_rabbitmq/`: RabbitMQ integration package.
- `src/forze_socketio/`: Socket.IO integration package.
- `src/forze_sqs/`: SQS integration package.
- `tests/unit/`: unit tests, typically mirroring `src` layout.
- `tests/integration/`: integration tests with external dependencies.
- `tests/perf/`: performance benchmarks (`-m perf`, excluded from `just test`; many use Docker, some in-process only).
- `pages/`: documentation source and build files.
- `examples/`: usage examples and tutorials.
- `skills/`: published [Agent Skills](https://agentskills.io/) for **app authors** (`SKILL.md` per skill; see `skills/AUTHORING.md`); install via README **Agent Skills** (e.g. `npx skills add morzecrew/forze`). Framework contribution uses `.claude/skills/` and canonical docs.

## Operating rules for agents

1. Prefer editing existing files over creating new top-level process documents.
2. Do not duplicate policy text from canonical files; link and follow it instead.
3. Validate architecture and tool constraints in `pyproject.toml` before code changes.
4. Use `justfile` commands as the default way to run tests and quality checks.
5. For user-visible behavior changes, update tests and docs together.
6. Record user-facing changes in `CHANGELOG.md` under `[Unreleased]`.
7. For security-sensitive work, follow `SECURITY.md` and minimize public detail.

## Cross-tool compatibility

If tool-specific directories exist (for example `.agent/` or `.cursor/`), they
should reference this routing file and canonical policy files, not redefine them.

Preferred pattern:
- central routing in `AGENTS.md`
- authoritative policy in canonical files
- tool-specific overlays that only point back to those sources

## Cursor Cloud specific instructions

### Overview

Forze is a Python library (not a runnable application). Development validation means running tests and quality checks, not starting a server.

### Prerequisites

The VM update script installs `uv`, `just`, and Python 3.13 via `uv`. After the update script runs, all dependencies are installed and the environment is ready.

### Key commands

See `justfile` and `CONTRIBUTING.md` for the full list. Quick reference:

- **Unit tests:** `just test tests/unit`
- **All tests (unit + integration):** `just test` (integration tests need Docker for testcontainers)
- **Performance tests:** `just perf` (benchmarks; Docker for container-backed perf, not required for every file)
- **Quality checks (lint/imports/dead-code/deps/security):** `just quality` (or `just quality -s` for strict)
- **Docs:** `just pages serve`

### Caveats

- Integration tests (`tests/integration/`) require Docker (testcontainers). Many performance tests under `tests/perf/` also use Docker; perf tests without container fixtures (e.g. codec benchmarks) run in-process. Default CI (`just test`) excludes `-m perf`.
- The package version is derived from git tags via `hatch-vcs`; importing `forze.__version__` does not work—use `forze._version.__version__` instead.
- `uv sync` is called automatically by `justfile` recipes before test/quality commands, so manual re-sync is rarely needed.

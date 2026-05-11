# Agent Skills

Forze ships with AI agent skills that help assistants understand the framework's architecture, patterns, and conventions. Install them to improve code generation and refactoring when working with Forze.

Skills follow the [Agent Skills](https://agentskills.io/) format.

## Installation

```bash
# Install all skills
npx skills add morzecrew/forze

# Install a specific skill
npx skills add morzecrew/forze@forze-wiring
```

## Usage

Skills are automatically available once installed. The agent will use them when relevant tasks are detected.

## Available Skills

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

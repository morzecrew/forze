---
name: forze-skills
description: >-
  Build backend services with Forze — specs, deps modules, handlers, governed aggregates,
  FastAPI routes, identity and tenancy, encryption, messaging, realtime, durable execution,
  analytics, and deterministic simulation testing. Use when writing or wiring an application
  that depends on forze and its integration packages.
---

# Forze

Forze is a Python framework for Domain-Driven Design and Hexagonal Architecture in backend
services. This skill is a routing index: the reference files under `references/` hold the actual
material, and you read the ones a task needs.

## The mental model, in four sentences

Your application declares **logical specs** — a `DocumentSpec`, a `QueueSpec`, a `StorageSpec` —
that name a resource without naming any physical thing. **Deps modules** map those names to real
backends (a Postgres table, an SQS queue, an S3 bucket) at wiring time. **Handlers** receive an
`ExecutionContext` and reach infrastructure only through ports resolved from it. The **runtime**
composes the registries, freezes them, and runs operations through the pipeline.

Everything else is a consequence of that split. If a handler mentions a table name, a bucket, or
an adapter import, something has gone wrong — and most anti-patterns in these references are a
version of that one mistake.

## Read the bundle, not one file

**Most tasks need three to five references, and reading one is usually wrong.** These are
procedures, not independent rules: an aggregate declared without its backend config is an app
that does not start, and a handler written without the execution context is a handler that
reaches for the wrong things. Start from the table below and read the whole row.

| I want to… | Read, in order |
|---|---|
| Bootstrap a new service | `architecture` → `spec-naming-and-routes` → `deps-resolution` → `runtime-lifecycle` |
| Add a governed aggregate | `aggregate-models` → `document-spec` → `aggregate-kit` → `spec-to-backend-config` → `testing-with-mock` |
| Write a custom handler | `execution-context` → `handlers` → `query-dsl` |
| Expose it over HTTP | `fastapi-setup` → `fastapi-generated-routes` → `fastapi-identity` |
| Encrypt a field | `field-encryption` → `kms-backends` → `spec-to-backend-config` |
| Simulate my service under faults | `dst-simulation` → `dst-invariants` → `testing-with-mock` |

If the task is not one of these, pick from the index below — but check whether it decomposes into
a row first.

## Index

### Foundations

| Reference | Covers |
|---|---|
| [architecture](references/architecture.md) | Layered architecture, contracts and adapters, what may import what |
| [execution-context](references/execution-context.md) | `ExecutionContext`, the ports on it, transactions, identity and tenancy in a handler |
| [handlers](references/handlers.md) | Handler patterns for the common operation shapes, and the gotchas |

### Specs, deps and wiring

| Reference | Covers |
|---|---|
| [spec-naming-and-routes](references/spec-naming-and-routes.md) | `StrEnum` spec names, transaction routes, why one name is one route |
| [spec-to-backend-config](references/spec-to-backend-config.md) | Mapping a logical name to Postgres, Mongo, Redis, storage, queue and workflow config |
| [deps-resolution](references/deps-resolution.md) | Plain vs routed deps, how handlers resolve ports, merge conflicts |
| [deps-custom-module](references/deps-custom-module.md) | Writing your own `DepsModule` and `DepKey` for a private integration |
| [runtime-lifecycle](references/runtime-lifecycle.md) | `build_runtime`, lifecycle steps, the spec inventory |
| [operation-composition](references/operation-composition.md) | Operation registries, pipeline stages, hooks, mapping steps |
| [testing-with-mock](references/testing-with-mock.md) | `forze_mock` in tests — every port in memory, no containers |

### Domain

| Reference | Covers |
|---|---|
| [aggregate-models](references/aggregate-models.md) | The four models, base fields, mixins, update validators |
| [document-spec](references/document-spec.md) | `DocumentSpec` / `SearchSpec`, schema alignment, composition DTOs |
| [aggregate-kit](references/aggregate-kit.md) | `AggregateKit` — one declaration for a governed vertical slice |

### Reading and writing data

| Reference | Covers |
|---|---|
| [document-facade](references/document-facade.md) | `DocumentFacade`, raw ports, adapter boundaries, cache-aware reads |
| [query-dsl](references/query-dsl.md) | Filters, sorts, projections, cursor paging |
| [search](references/search.md) | `SearchFacade`, hub and federated search, rebuilding an index |

### Events, messaging and realtime

| Reference | Covers |
|---|---|
| [messaging-queues](references/messaging-queues.md) | Queue contracts, SQS and RabbitMQ wiring |
| [messaging-pubsub-streams](references/messaging-pubsub-streams.md) | Pub/sub, streams, consumer-group discipline, shutdown |
| [outbox-notifications](references/outbox-notifications.md) | Transactional notifications — stage in the transaction, relay after commit |
| [realtime-catalog](references/realtime-catalog.md) | Declaring an event catalog and publishing from a handler |
| [realtime-transports](references/realtime-transports.md) | Socket.IO, SSE and WebSocket behind one wire protocol; offline mailbox |

### Durable execution

| Reference | Covers |
|---|---|
| [temporal](references/temporal.md) | `DurableWorkflowSpec`, Temporal deps, schedules, worker context |
| [inngest](references/inngest.md) | Inngest events, functions, steps, serving; the self-hosted Postgres runner |

### Interface

| Reference | Covers |
|---|---|
| [fastapi-setup](references/fastapi-setup.md) | Context dependency, lifespan, middleware, error handlers |
| [fastapi-generated-routes](references/fastapi-generated-routes.md) | `attach_*_routes`, hand-written routes, deadline headers, MCP projection |
| [fastapi-identity](references/fastapi-identity.md) | Binding identity at the boundary, cookie mode, principal eligibility |

### Identity, tenancy and secrets

| Reference | Covers |
|---|---|
| [authn](references/authn.md) | Boundary binding, the verify-then-resolve pipeline, authn deps, authz |
| [oidc](references/oidc.md) | External IdPs, token verifiers, principal resolution |
| [tenancy](references/tenancy.md) | Tenant identity, routed clients, isolation tiers, the admin plane, provisioning |
| [secrets](references/secrets.md) | Secret-backed configuration and its backends |

### Encryption

| Reference | Covers |
|---|---|
| [field-encryption](references/field-encryption.md) | `FieldEncryption`, what gets sealed, strict mode after backfill |
| [kms-backends](references/kms-backends.md) | Vault and cloud KMS, per-tenant keys (BYOK), rotation vs replacement |

### Other planes

| Reference | Covers |
|---|---|
| [object-storage](references/object-storage.md) | `StorageSpec`, S3 and GCS, tenant-aware buckets, presigned and multipart |
| [http-outbound](references/http-outbound.md) | Declarative outbound HTTP integrations, auth, tenant routing |
| [analytics](references/analytics.md) | `AnalyticsSpec`, named SQL templates, BigQuery and ClickHouse |
| [graph](references/graph.md) | Node and edge specs, traversal ports, Neo4j |
| [inference](references/inference.md) | `InferenceSpec`, local / HTTP / SageMaker backends, capabilities |

### Testing under faults

| Reference | Covers |
|---|---|
| [dst-simulation](references/dst-simulation.md) | Declaring a simulation over your own operations, schedulers, fault and latency injection |
| [dst-invariants](references/dst-invariants.md) | Invariants, reachability targets, reading a `ViolationReport` |

### Running in production

| Reference | Covers |
|---|---|
| [errors](references/errors.md) | `CoreException`, adapter exception mapping, FastAPI error responses |
| [logging-metrics](references/logging-metrics.md) | Structured logging, call context, operation and resilience metrics |
| [resilience](references/resilience.md) | Retry, breaker, bulkhead, rate limit, hedging; invocation deadlines |
| [shutdown-fleet](references/shutdown-fleet.md) | Graceful drain, quiesce, readiness, fleet posture across replicas |

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older
> `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.6/...`), or use
> the version selector on the site.

- [Forze documentation](https://morzecrew.github.io/forze/latest/)
- [Quickstart](https://morzecrew.github.io/forze/latest/get-started/quickstart/)
- [Contracts overview](https://morzecrew.github.io/forze/latest/reference/contracts/)

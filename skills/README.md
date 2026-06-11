# Agent Skills

Forze ships with AI agent skills for **applications that use Forze as a dependency**. Install them so assistants follow correct wiring, specs, handlers, and integration patterns in your service repo.

Skills follow the [Agent Skills](https://agentskills.io/) format. Maintainers: see [AUTHORING.md](AUTHORING.md).

## Installation

```bash
# Install all skills
npx skills add morzecrew/forze

# Install a specific skill
npx skills add morzecrew/forze@forze-wiring
```

## Usage

Skills load automatically when the agent detects a relevant task (handlers, wiring, Temporal, auth, and so on).

## Available skills

| Name | Description |
| -------- | -------- |
| **forze-framework-usage** | ExecutionContext, ports, transactions, identity context, and query DSL in handlers. |
| **forze-domain-aggregates** | Document aggregates, mixins, validators, logical `DocumentSpec` / `SearchSpec`, composition DTOs. |
| **forze-wiring** | Runtime, `DepsRegistry`, lifecycle, composition, operation pipeline stages, FastAPI attach. |
| **forze-specs-infrastructure** | Map logical `StrEnum` spec names to Postgres/Mongo/Redis/S3/queue/workflow configs. |
| **forze-deps-consumption** | Plain vs routed deps, `route=spec.name`, built-in `*DepsModule`, merge debugging. |
| **forze-custom-deps** | Advanced: custom `DepKey` and `DepsModule` for private integrations. |
| **forze-documents-search** | Document/search ports, query DSL, cache-aware specs, Postgres/Mongo/Firestore + Meilisearch behavior. |
| **forze-fastapi-interface** | FastAPI context deps, generated routes (`attach_*_routes`), middleware, errors. |
| **forze-storage-s3** | `StorageSpec`, `S3DepsModule`, tenant-aware buckets, lifecycle, mock tests. |
| **forze-storage-gcs** | GCS storage with `GCSDepsModule`, emulator, tenant-aware buckets, mock tests. |
| **forze-http-outbound** | Outbound HTTP: declarative `BaseHttpIntegration` / `async_http_op`, `HttpServiceSpec`, `HttpDepsModule`, auth, tenant routing. |
| **forze-messaging-streaming** | Queue, pub/sub, stream contracts; SQS/RabbitMQ; Redis custom wiring notes. |
| **forze-temporal-workflows** | `DurableWorkflowSpec`, Temporal deps, workflow ports, schedules, worker context. |
| **forze-inngest-durable-functions** | Durable functions with Inngest: events, registration, steps, FastAPI serve. |
| **forze-auth-tenancy-secrets** | Authn/authz, tenancy, secrets, OIDC, FastAPI identity binding. |
| **forze-graph-contracts** | Graph specs and ports; custom adapter wiring (no official graph package). |
| **forze-observability-errors** | `CoreException` / `exc` factories, logging, call context, FastAPI error responses. |
| **forze-analytics-clickhouse** | `AnalyticsSpec`, ClickHouse query/ingest, local Docker setup. |
| **forze-analytics-bigquery** | BigQuery analytics, emulator env, streaming ingest. |

## Documentation

Skills link to the published docs at [morzecrew.github.io/forze](https://morzecrew.github.io/forze/). Framework contribution is documented in the Forze repository (`AGENTS.md`, `CONTRIBUTING.md`), not in these skills.

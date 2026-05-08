# PostgreSQL Integration

## Page opening

`forze_postgres` provides PostgreSQL-backed adapters for document storage, read-only projections, full-text/vector search, federated search, hub search, and transaction management. It keeps persistence code behind Forze contracts while using PostgreSQL tables, views, indexes, and connection pools at the infrastructure edge.

| Topic | Details |
|------|---------|
| What it provides | A `PostgresClient`, lifecycle hooks, dependency module, document adapters, search adapters, an introspector, and a transaction manager. |
| Supported Forze contracts | `DocumentQueryDepKey`, `DocumentCommandDepKey`, `SearchQueryDepKey`, `HubSearchQueryDepKey`, `FederatedSearchQueryDepKey`, and `TxManagerDepKey`. |
| When to use it | Use this integration when PostgreSQL is the system of record, when projections/search indexes live in PostgreSQL, or when usecases need transaction boundaries around Postgres-backed adapters. |

## Installation

```bash
uv add 'forze[postgres]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `postgres` installs `psycopg` and `psycopg-pool`. |
| Required service | PostgreSQL. Search features may require extensions such as PGroonga or pgvector depending on the selected engine. |
| Local development dependency | A local PostgreSQL server or a containerized PostgreSQL instance. Integration tests normally use testcontainers. |

## Minimal setup

### Client

```python
from forze_postgres import PostgresClient, PostgresConfig

pg = PostgresClient()
```

Use `RoutedPostgresClient` when the current tenant or route determines the DSN.

### Config

```python
from forze_postgres import PostgresDocumentConfig

project_document_config = PostgresDocumentConfig(
    read=("public", "project_reads"),
    write=("public", "projects"),
    history=("public", "project_history"),
    bookkeeping_strategy="application",
    batch_size=200,
)
```

For search, use `PostgresSearchConfig`, `PostgresHubSearchConfig`, or `PostgresFederatedSearchConfig` and choose `engine="pgroonga"`, `engine="fts"`, or `engine="vector"`.

### Deps module

```python
from forze.application.execution import DepsPlan
from forze_postgres import PostgresDepsModule

postgres_module = PostgresDepsModule(
    client=pg,
    rw_documents={"projects": project_document_config},
    searches={"projects": project_search_config},
    tx={"projects"},
)

deps_plan = DepsPlan.from_modules(postgres_module)
```

Routes such as `"projects"` should match the names used by your `DocumentSpec`, `SearchSpec`, and transaction wiring.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import postgres_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    postgres_lifecycle_step(
        dsn="postgresql://forze:forze@localhost:5432/forze",
        config=PostgresConfig(min_size=1, max_size=10),
    )
)
```

Use `routed_postgres_lifecycle_step(client=routed_pg)` with `RoutedPostgresClient` and do not combine routed and non-routed lifecycle steps for the same client.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Document queries | `ConfigurablePostgresReadOnlyDocument` / `PostgresDocumentAdapter` | `DocumentQueryDepKey`, route usually equal to `DocumentSpec.name`. | Requires a read relation; nested filtering may need `nested_field_hints` for ambiguous JSON/dict fields. |
| Document commands | `ConfigurablePostgresDocument` / `PostgresDocumentAdapter` | `DocumentCommandDepKey`, route usually equal to `DocumentSpec.name`. | Requires a write relation and bookkeeping strategy; history is used only when both schema and spec enable it. |
| Search queries | `ConfigurablePostgresSearch` with PGroonga, FTS, or vector adapter. | `SearchQueryDepKey`, route usually equal to `SearchSpec.name`. | Engine-specific schema/index requirements apply; vector search requires an embedding provider and matching dimensions. |
| Hub search | `ConfigurablePostgresHubSearch` | `HubSearchQueryDepKey`, route usually equal to the hub search spec name. | Member relations and hub foreign-key mappings must be configured consistently. |
| Federated search | `ConfigurablePostgresFederatedSearch` | `FederatedSearchQueryDepKey`, route usually equal to federated search spec name. | Requires at least two member configurations; result merging uses configured reciprocal-rank-fusion options. |
| Transactions | `postgres_txmanager` | `TxManagerDepKey`, route from the module `tx` set. | Only coordinates operations that use the same Postgres client/context. |
| Raw client/introspection | `PostgresClient` and `PostgresIntrospector` | `PostgresClientDepKey` and `PostgresIntrospectorDepKey`. | Use raw access sparingly; prefer contracts in usecases. |

## Complete recipe link

See [CRUD with FastAPI, Postgres, and Redis](../recipes/crud-fastapi-postgres-redis.md) for an end-to-end HTTP + PostgreSQL + cache recipe. Use [Read-only Document API](../recipes/read-only-document-api.md) when you only expose projections.

## Configuration reference

### Connection settings

`PostgresClient` connects with a DSN. For multi-tenant or per-route databases, use `RoutedPostgresClient` and pass `introspector_cache_partition_key` to `PostgresDepsModule` when catalog caching must be partitioned by tenant.

### Pool settings

`PostgresConfig` controls `min_size`, `max_size`, `max_lifetime`, `max_idle`, `reconnect_timeout`, `num_workers`, `pool_headroom`, and optional `max_concurrent_queries`. Keep `max_concurrent_queries` below pool capacity when large batch reads and writes run concurrently.

### Serialization settings

Document adapters map Pydantic read/create/update models to PostgreSQL rows. Search adapters map configured field names to heap/read columns with `field_map`, `join_pairs`, and optional nested field hints.

### Retry/timeout behavior

Connection recovery is bounded by `reconnect_timeout`. Query-level retries should be handled at usecase or adapter-call boundaries only when the operation is safe to repeat. Use transactions for multi-step writes that must commit atomically.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | Create read/write/history relations, indexes, extensions, and search indexes outside Forze with your migration tool. Forze introspects existing schema; it does not create application tables for you. |
| Cleanup/shutdown | Register `postgres_lifecycle_step` or `routed_postgres_lifecycle_step` so pools open on startup and close on shutdown. |
| Idempotency/caching behavior | Document adapters can coordinate with a cache specified on the document spec. Idempotency is a separate contract, commonly backed by Redis. |
| Production caveats | Size pools for concurrency, configure transaction isolation deliberately, monitor long-running queries, and validate PGroonga/pgvector extension versions before deploying search features. |

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| Revision/history reads fail because a history relation is missing. | `history_enabled` or a Postgres history config was enabled, but the history table was not created or mapped. | Create the history table with migrations and set the `history=(schema, table)` tuple in the document config. | [Operational notes](#operational-notes) |
| Reads or writes target the wrong table or schema. | The `(schema, table)` tuple in `PostgresDocumentConfig`, `PostgresReadOnlyDocumentConfig`, or `PostgresSearchConfig` points at the wrong relation. | Verify every `read`, `write`, `history`, and search tuple against your migrations and logical `DocumentSpec.name`. | [Config](#config) |
| `ctx.txmanager(route)` cannot resolve a transaction dependency. | The transaction route was not registered in `PostgresDepsModule.tx`, or the requested route name differs. | Add the route to `tx` and call `ctx.txmanager(...)` with the same route string. | [Deps module](#deps-module) |
| `Write relation is required for non read-only documents`. | A read-write document was registered without `write`. | Add `write=(schema, table)` or register the document under `ro_documents`. | [Contract coverage table](#contract-coverage-table) |
| Search configuration validation fails. | Engine-specific fields are missing, such as `fts_groups`, `vector_column`, `embedding_dimensions`, or `embeddings_name`. | Add the required fields for the selected search engine. | [Configuration reference](#configuration-reference) |
| Tenant A sees schema metadata from tenant B. | A routed client uses introspection cache without a partition key. | Set `introspector_cache_partition_key` to the same tenant or route identity used for routing. | [Operational notes](#operational-notes) |
| Pool exhaustion or slow batch operations. | Pool sizes and batch concurrency are too small for workload. | Increase `max_size`, tune `pool_headroom`/`max_concurrent_queries`, or reduce batch size. | [Pool settings](#pool-settings) |

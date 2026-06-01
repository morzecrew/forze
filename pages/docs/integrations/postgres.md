# PostgreSQL Integration

## Page opening

`forze_postgres` provides PostgreSQL-backed adapters for document storage, read-only projections, full-text/vector search, federated search, hub search, transaction management, and optional analytics (warehouse-style named SQL). It keeps persistence code behind Forze contracts while using PostgreSQL tables, views, indexes, and connection pools at the infrastructure edge.

| Topic | Details |
|------|---------|
| What it provides | A `PostgresClient`, lifecycle hooks, dependency module, document adapters, search adapters, an introspector, a transaction manager, and optional `PostgresAnalyticsAdapter`. |
| Supported Forze contracts | `DocumentQueryDepKey`, `DocumentCommandDepKey`, `SearchQueryDepKey`, `HubSearchQueryDepKey`, `FederatedSearchQueryDepKey`, `TxManagerDepKey`, and (when configured) `AnalyticsQueryDepKey` / `AnalyticsIngestDepKey`. |
| When to use it | Use this integration when PostgreSQL is the system of record, when projections/search indexes live in PostgreSQL, when handlers need transaction boundaries around Postgres-backed adapters, or when pre-provisioned analytics tables/views are queried via `AnalyticsSpec` (not OLTP document aggregates). |

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/document-cache-flow.svg" alt="Document read path with cache hit, cache miss, and database fallback">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/document-cache-flow.svg" alt="Document read path with cache hit, cache miss, and database fallback">
</div>

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

#### `read_validation` (read throughput)

`PostgresReadOnlyDocumentConfig` and `PostgresDocumentConfig` accept `read_validation`:

| Value | Behavior |
|-------|----------|
| `"strict"` (default) | Full Pydantic validation on every row returned from SELECT. |
| `"trusted"` | Build read models with `model_construct` when row keys match the read model (no validator run). |

Use `"trusted"` only when the read relation columns match `DocumentSpec.read` and psycopg already returns correct Python types. Extra columns not on the read model raise a precondition error. History blobs, cache payloads, and write `RETURNING` paths stay strict.

Document `read`, `write`, and optional `history`, search `index` / `read` / `heap`, and hub `hub` (plus per-leg search relations) accept a static `(schema, relation)` tuple or a tenant resolver (`ValueResolver` from `forze.application.contracts.resolution`). Resolvers run on async I/O (not at deps wiring time). Startup document schema validation and catalog warmup require static tuples (or skip those lifecycle steps). Example schema-per-tenant document:

```python
PostgresDocumentConfig(
    read=lambda tid: (f"tenant_{tid.hex[:8]}", "projects"),
    write=lambda tid: (f"tenant_{tid.hex[:8]}", "projects"),
    bookkeeping_strategy="application",
    tenant_aware=False,
)
```

`bookkeeping_strategy` controls who bumps `rev` and timestamps:

| Strategy | Application | Database |
|----------|-------------|----------|
| Revision bumps | Write gateway (`__bump_rev` / SQL increment) | `BEFORE UPDATE` trigger on write table |
| History rows | `PostgresHistoryGateway` writes on mutation | Triggers (app history gateway is no-op) |
| Startup validation | Warns if UPDATE triggers exist | **Fails** if no UPDATE trigger on write table |

Example trigger name: `{table}_bump_rev` (see [First project walkthrough](../first-project-walkthrough.md)).

#### `conflict_target` (ensure / upsert)

Optional `conflict_target: tuple[str, ...]` on `PostgresDocumentConfig` sets the column list for `INSERT … ON CONFLICT (…) DO NOTHING` in `ensure`, `ensure_many`, `upsert`, and `upsert_many`. When omitted, the adapter infers the write table **PRIMARY KEY** from Postgres catalogs at runtime (and validates it during optional schema validation startup).

- Use an explicit value when inference is insufficient (for example expression or partial primary indexes).
- Additional **UNIQUE** constraints on the write table are supported: existing primary keys are detected via `ON CONFLICT`; a **new** primary key that violates another UNIQUE column still raises a conflict error.
- Composite primary keys (for example `(tenant_id, id)`) are inferred automatically when all PK columns appear in the insert payload.

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

For framework tests or advanced wiring, prefer `from forze_postgres.execution.deps import ConfigurablePostgresDocument` (and siblings) rather than removed `forze_postgres.execution.deps.deps` paths.

Integration configs are frozen `attrs` classes (not dicts). Example:

```python
from forze_postgres import PostgresDocumentConfig, PostgresSearchConfig

PostgresDocumentConfig(
    read=("public", "projects"),
    write=("public", "projects"),
    bookkeeping_strategy="database",
    history=("public", "projects_history"),
)

PostgresSearchConfig(
    index=("public", "projects_search_idx"),
    read=("public", "projects"),
    engine="pgroonga",
)
```

### Lifecycle plan

Prefer `PostgresLifecycleModule` when wiring pool startup plus optional catalog warmup and schema validation (same `client` / search maps as `PostgresDepsModule`):

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import PostgresConfig, PostgresLifecycleModule

lifecycle = LifecyclePlan.from_modules(
    PostgresLifecycleModule(
        client=pg,
        dsn="postgresql://forze:forze@localhost:5432/forze",
        config=PostgresConfig(min_size=1, max_size=10),
        searches=search_configs_by_route,
        schema_specs=(...),  # optional
    ),
)
```

For a pool only, use `postgres_lifecycle_step` or `LifecyclePlan.from_steps(...)`. Use `routed_postgres_lifecycle_step(client=routed_pg)` / `PostgresLifecycleModule(client=routed_pg)` with `RoutedPostgresClient` and do not combine routed and non-routed lifecycle steps for the same client.

### Catalog warmup, introspector TTL, and document schema checks

`PostgresLifecycleModule` registers follow-up steps when search maps or `schema_specs` are set. You can also compose step factories manually; capability metadata (`postgres.client`) orders the pool before warmup and validation:

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import (
    PostgresConfig,
    postgres_catalog_warmup_lifecycle_step,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
    postgres_lifecycle_step,
)

lifecycle = LifecyclePlan.from_steps(
    postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
    postgres_catalog_warmup_lifecycle_step(
        searches=search_configs_by_route,
        hub_searches=hub_search_configs_by_route,
        federated_searches=federated_search_configs_by_route,
    ),
    postgres_document_schema_validation_lifecycle_step(
        specs=(
            postgres_document_schema_spec_for_binding(
                "orders",
                spec=orders_document_spec,
                config=orders_pg_config,
            ),
        ),
    ),
)
```

- **`warm_postgres_catalog` / `postgres_catalog_warmup_lifecycle_step`** prefetch relation column types and index catalog metadata used by FTS/PGroonga search (and vector read/heap relations). Dynamic `RelationSpec` resolvers are skipped at warmup (trace log per relation). They are safe no-ops when `PostgresDepsModule.introspector_cache_partition_key` is set but no tenant is available during startup (trace log only).
- **`PostgresDepsModule.introspector_cache_ttl`** passes a TTL into `PostgresIntrospector` so cached catalog rows expire without a process restart (useful after migrations).
- **`postgres_document_schema_spec_for_binding`** and **`postgres_document_schema_validation_lifecycle_step`** optionally assert that read (and write/history) relations expose the columns implied by your `DocumentSpec` and Pydantic models. Relations must be **static** `("schema", "table")` tuples (or string literals coerced to tuples); dynamic `RelationSpec` resolvers raise at spec build time because startup validation cannot pick a tenant. Omit the schema validation lifecycle step for schema-per-tenant documents and validate relations in your own migration or deploy hook. Pydantic `@computed_field` values are derived in Python only (not selected or persisted). Use `read_omit_fields` / `write_omit_fields` / `history_omit_fields` on `PostgresDocumentSchemaSpec` when a **non-computed** model field is not stored as its own column (for example a read-view column supplied elsewhere).
- **Tenancy wiring validation:** `PostgresDepsModule` fails at build time when `RoutedPostgresClient` is used without `introspector_cache_partition_key`. It warns when any route has `tenant_aware=True` on a routed client (redundant row filter). Schema validation warns when a write table has `tenant_id` but `tenant_aware=False`.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Document queries | `ConfigurablePostgresReadOnlyDocument` / `PostgresDocumentAdapter` | `DocumentQueryDepKey`, route usually equal to `DocumentSpec.name`. | Requires a read relation; `nested_field_hints` may still be needed for bare `dict` / `Any` / ambiguous unions on JSON columns, or to override a path's inferred type. |
| Document commands | `ConfigurablePostgresDocument` / `PostgresDocumentAdapter` | `DocumentCommandDepKey`, route usually equal to `DocumentSpec.name`. | Requires a write relation and bookkeeping strategy; history is used only when both schema and spec enable it. |
| Search queries | `ConfigurablePostgresSearch` with PGroonga, FTS, or vector adapter. | `SearchQueryDepKey`, route usually equal to `SearchSpec.name`. | Engine-specific schema/index requirements apply; vector search requires an embedding provider and matching dimensions. |
| Hub search | `ConfigurablePostgresHubSearch` | `HubSearchQueryDepKey`, route usually equal to the hub search spec name. | Member relations and hub foreign-key mappings must be configured consistently. |
| Federated search | `ConfigurablePostgresFederatedSearch` | `FederatedSearchQueryDepKey`, route usually equal to federated search spec name. | Requires at least two member configurations; result merging uses configured reciprocal-rank-fusion options. |
| Transactions | `postgres_txmanager` | `TxManagerDepKey`, route from the module `tx` set. | Only coordinates operations that use the same Postgres client/context. |
| Raw client/introspection | `PostgresClient` and `PostgresIntrospector` | `PostgresClientDepKey` and `PostgresIntrospectorDepKey`. | Use raw access sparingly; prefer contracts in handlers. |

## Complete recipe link

See [CRUD with FastAPI, Postgres, and Redis](../recipes/crud-fastapi-postgres-redis.md) for an end-to-end HTTP + PostgreSQL + cache recipe. Use [Read-only Document API](../recipes/read-only-document-api.md) when you only expose projections.

## Configuration reference

### Connection settings

`PostgresClient` connects with a DSN. For multi-tenant or per-route databases, use `RoutedPostgresClient` and pass `introspector_cache_partition_key` to `PostgresDepsModule` when catalog caching must be partitioned by tenant. Optional `introspector_cache_ttl` expires cached catalog metadata after a duration (see the lifecycle subsection above).

### Pool settings

`PostgresConfig` controls `min_size`, `max_size`, `max_lifetime`, `max_idle`, `reconnect_timeout`, `num_workers`, `pool_headroom`, and optional `max_concurrent_queries`. Keep `max_concurrent_queries` below pool capacity when large batch reads and writes run concurrently. Parallel batch helpers (`gather_db_work`) share one **pool-wide** semaphore from the active `PostgresClient` / `RoutedPostgresClient`, so many concurrent HTTP handlers cannot each multiply concurrent catalog or batch work against the same pool.

### Serialization settings

Document adapters map Pydantic read/create/update models to PostgreSQL rows. Search adapters map configured field names to heap/read columns with `field_map`, `join_pairs`, and optional nested field hints.

### PGroonga search fields

For `engine="pgroonga"`, multi-column indexes use `ARRAY[col1, col2, ...]` in migrations. Forze reads that order from the index catalog at query time and aligns heap columns and PGroonga `weights` to it. **`SearchSpec.fields` order does not matter**; per-field weights (`default_weights`, `options.weights`) stay keyed by logical field name. Every column in the index must be listed in `SearchSpec.fields` (use `field_map` in `PostgresSearchConfig` when logical names differ from heap columns). Extra spec fields are allowed and are not passed to the PGroonga match clause. If the index expression cannot be parsed (`ARRAY[...]` or a single column reference), search raises `CoreError` at query time.

### JSON filters and GIN-friendly indexes

Filters that drill into JSON/JSONB with nested `->` / `->>` paths are rendered as plain SQL expressions by the Postgres query layer (`PsycopgQueryRenderer`, nested field helpers). Read-model types drive scalar coercion: nested Pydantic fields use model metadata; parameterized `dict[str, V]` / `Mapping[str, V]` treat one dot segment as the JSON object key and infer `V` (including nested models and nested mappings). A **generic** GIN index on the whole JSON column only helps when the indexed expression matches how you filter (for example `@>` / containment-style predicates with `jsonb_ops` or `jsonb_path_ops`). Dot-path filters on nested keys often need a **matching** expression index or a dedicated generated column that you query instead of ad-hoc `->>` chains, or the planner may fall back to sequential scans.

### Retry/timeout behavior

Connection recovery is bounded by `reconnect_timeout`. Query-level retries should be handled at handler or adapter-call boundaries only when the operation is safe to repeat. Use transactions for multi-step writes that must commit atomically.

## Analytics

Use analytics when handlers need **named, parameterized SQL** against pre-provisioned tables or materialized views (warehouse-style reads and optional small batch append). This is separate from `DocumentQueryPort` OLTP aggregates.

1. Declare `AnalyticsSpec` routes and named queries in application code.
2. Map each route in `PostgresDepsModule.analytics` to SQL templates and optional ingest target (`ingest_relation` or legacy `ingest_table`).
3. Resolve ports from `ExecutionContext`; do not import adapters in handlers.

`PostgresAnalyticsAdapter` implements `AnalyticsQueryPort` and, when configured, `AnalyticsIngestPort` on the same adapter instance.

### Analytics configuration

Physical mapping lives on `PostgresDepsModule.analytics`, keyed by `AnalyticsSpec.name`:

| Field | Purpose |
|-------|---------|
| `queries` | Map of `query_key` → `{ "sql": "...", "skip_total"?: bool, "cursor_column"?: str }`. Keys must match `AnalyticsSpec.queries`. |
| `ingest_relation` | Ingest target as static `(schema, table)` or tenant `ValueResolver` (schema-per-tenant). Preferred when `AnalyticsSpec.ingest` is set. |
| `schema` | Legacy schema for `ingest_table` when `ingest_relation` is omitted (default `public`). |
| `ingest_table` | Legacy table name for `append`; use `ingest_relation` for relation-level isolation. |
| `max_append_rows` | Cap per `append` call (default 10_000). |

Query SQL should use fully qualified `schema.table` names as needed; ingest resolution is independent of query templates.

### SQL templates

Use **psycopg named placeholders** `%(param)s` bound from each query’s Pydantic params model (`model_dump()`). For keyset cursors, set `cursor_column` on the query config and include `%(forze_after)s` in the SQL predicate.

    :::python
    PostgresDepsModule(
        client=pg,
        analytics={
            "events": {
                "queries": {
                    "daily": {
                        "sql": (
                            "SELECT event, value FROM public.metrics "
                            "WHERE day = %(day)s"
                        ),
                    },
                },
                "ingest_relation": ("public", "metrics_raw"),
            },
        },
    )

Pass `AnalyticsRunOptions` (`dry_run`, `max_rows`, `timeout`) per request. When `timeout` is set, the adapter applies `SET LOCAL statement_timeout` for that query. `dry_run` returns empty pages without executing SQL.

### Analytics queries and tenancy

Forze resolves **ingest** targets via `ingest_relation` (or legacy `ingest_table`). **Query** SQL in `queries.*.sql` is passed to Postgres **verbatim**—there is no `RelationSpec` on query templates.

| Concern | Framework | Application |
|---------|-----------|-------------|
| Append target | `ingest_relation` / `resolved_ingest_relation()` | Static tuple or `ValueResolver` for schema-per-tenant tables |
| Read SQL | Not rewritten | You own qualification and filters |

Recommended patterns:

1. **Database-per-tenant** — `RoutedPostgresClient` with per-tenant DSN; keep static `schema.table` in SQL inside that database.
2. **Schema-per-tenant** — `RelationSpec` on document routes and `ingest_relation` for append; point analytics SQL at views or tables in each tenant schema (fully qualified names in SQL).
3. **Shared database** — embed `schema.table` in each query string, or register separate `AnalyticsSpec` routes with different static SQL per layout.
4. **Row scope in SQL** — add `WHERE tenant_id = %(tenant_id)s` (or equivalent) and bind from the query params model; this complements document `tenant_aware`, it does not replace missing table qualification.

See [Multi-tenancy — relation-level isolation](../concepts/multi-tenancy.md#relation-level-isolation-all-integrations).

See [Analytics contracts](../core-package/contracts/analytics.md).

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
| `ctx.tx_ctx.resolver(route)` cannot resolve a transaction dependency. | The transaction route was not registered in `PostgresDepsModule.tx`, or the requested route name differs. | Add the route to `tx` and use the same route string in `ctx.tx_ctx.scope(...)` / `bind_tx().set_route(...)`. | [Deps module](#deps-module) |
| `Write relation is required for non read-only documents`. | A read-write document was registered without `write`. | Add `write=(schema, table)` or register the document under `ro_documents`. | [Contract coverage table](#contract-coverage-table) |
| Search configuration validation fails. | Engine-specific fields are missing, such as `fts_groups`, `vector_column`, `embedding_dimensions`, or `embeddings_name`. | Add the required fields for the selected search engine. | [Configuration reference](#configuration-reference) |
| Tenant A sees schema metadata from tenant B. | A routed client uses introspection cache without a partition key. | Set `introspector_cache_partition_key` to the same tenant or route identity used for routing. | [Operational notes](#operational-notes) |
| `PostgresDepsModule` raises `postgres_tenancy_validation_failed` at import/wiring. | `RoutedPostgresClient` registered without `introspector_cache_partition_key`. | Pass a callable that returns the current tenant id (same as routing). | [Catalog warmup](#catalog-warmup-introspector-ttl-and-document-schema-checks) |
| Pool exhaustion or slow batch operations. | Pool sizes and batch concurrency are too small for workload. | Increase `max_size`, tune `pool_headroom`/`max_concurrent_queries`, or reduce batch size. | [Pool settings](#pool-settings) |

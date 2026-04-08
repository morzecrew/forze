# PostgreSQL Integration

`forze_postgres` provides document storage, full-text search, and transaction management backed by PostgreSQL. It implements `DocumentQueryPort`, `DocumentCommandPort`, `SearchQueryPort`, and `TxManagerPort` using async `psycopg` with connection pooling.

Kernel specs (`DocumentSpec`, `SearchSpec`) describe **models and logical names**. **Table and index locations** are supplied separately via `PostgresDepsModule` (`rw_documents`, `searches`, …). See [Specs and infrastructure wiring](../core-concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[postgres]'

Requires PostgreSQL 14 or later.

## Runtime wiring

Create a client, pass **per-aggregate** Postgres configs into `PostgresDepsModule`, and add a lifecycle step for pool management:

    :::python
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_postgres import (
        PostgresClient,
        PostgresConfig,
        PostgresDepsModule,
        postgres_lifecycle_step,
    )

    client = PostgresClient()

    module = PostgresDepsModule(
        client=client,
        rw_documents={
            "projects": {
                "read": ("public", "projects"),
                "write": ("public", "projects"),
                "bookkeeping_strategy": "database",
                "history": ("public", "projects_history"),
            },
        },
        searches={
            "projects": {
                "engine": "pgroonga",
                "index": ("public", "idx_projects_content"),
                "source": ("public", "projects"),
            },
        },
        tx={"default"},
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            postgres_lifecycle_step(
                dsn="postgresql://user:pass@localhost:5432/mydb",
                config=PostgresConfig(min_size=2, max_size=15),
            )
        ),
    )

Keys in `rw_documents`, `ro_documents`, and `searches` must match `DocumentSpec.name` and `SearchSpec.name` on the kernel side.

### PostgresConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| `min_size` | `int` | `2` | Minimum connections in the pool |
| `max_size` | `int` | `10` | Maximum connections in the pool |
| `timeout` | `float` | `30.0` | Connection acquisition timeout (seconds) |

### What gets registered

`PostgresDepsModule` registers:

| Key | Capability |
|-----|-----------|
| `PostgresClientDepKey` | Async `psycopg` client (pool) |
| `PostgresIntrospectorDepKey` | Catalog introspection (search, types) |
| `DocumentQueryDepKey` | Routed document **query** factories (`ConfigurablePostgresReadOnlyDocument` / derived read side) |
| `DocumentCommandDepKey` | Routed document **command** factories (`ConfigurablePostgresDocument`) |
| `SearchQueryDepKey` | Routed search factories (`ConfigurablePostgresSearch`) |
| `TxManagerDepKey` | Transaction managers per route in `tx` |

## DocumentSpec vs Postgres config

`DocumentSpec` carries **read model type**, **write model types**, `history_enabled`, and optional `CacheSpec`. It does **not** contain SQL identifiers.

`PostgresDocumentConfig` supplies:

| Field | Purpose |
|-------|---------|
| `read` | `(schema, relation)` for reads (table, view, or materialized view) |
| `write` | `(schema, table)` for mutations |
| `history` | Optional `(schema, table)` when history is stored in Postgres |
| `bookkeeping_strategy` | `"database"` or `"application"` — who bumps `rev` and timestamps |
| `batch_size` | Optional write batch size (default 200) |
| `tenant_aware` | Optional multi-tenant column handling |

Read-only documents use `ro_documents` with `PostgresReadOnlyDocumentConfig` (`read` only).

## Document table schema

Every document table must include core columns matching the domain model fields.

### Required columns

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `id` | `uuid` | `gen_random_uuid()` | Primary key |
| `rev` | `integer` | `1` | Revision counter |
| `created_at` | `timestamptz` | `now()` | Creation timestamp |
| `last_update_at` | `timestamptz` | `now()` | Last update timestamp |

### Optional columns (by mixin)

| Column | Type | Mixin | Purpose |
|--------|------|-------|---------|
| `is_deleted` | `boolean` | `SoftDeletionMixin` | Soft delete flag |
| `number_id` | `bigint` | `NumberMixin` | Human-readable sequence |
| `creator_id` | `uuid` | `CreatorMixin` | Creator reference |
| `tenant_id` | `uuid` | Multi-tenancy | Tenant partition |

Add domain-specific columns as needed. Column names must match Pydantic model field names (snake_case).

### Example DDL

    :::sql
    CREATE TABLE public.projects (
        id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
        rev             integer     NOT NULL DEFAULT 1,
        created_at      timestamptz NOT NULL DEFAULT now(),
        last_update_at  timestamptz NOT NULL DEFAULT now(),
        is_deleted      boolean     NOT NULL DEFAULT false,
        title           text        NOT NULL,
        description     text        NOT NULL
    );

## Revision strategy

`bookkeeping_strategy` on `PostgresDocumentConfig` controls how `rev` and `last_update_at` advance:

| Strategy | Behavior |
|----------|----------|
| `"database"` | Prefer triggers (or DB defaults) to bump `rev` and touch timestamps |
| `"application"` | Adapter supplies the next `rev` in the update path |

### Database trigger for revision bumping

When using `"database"` bookkeeping with triggers, for example:

    :::sql
    CREATE OR REPLACE FUNCTION bump_rev()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.rev := OLD.rev + 1;
        NEW.last_update_at := now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER projects_bump_rev
    BEFORE UPDATE ON public.projects
    FOR EACH ROW EXECUTE FUNCTION bump_rev();

## History table (audit trail)

Set `history_enabled=True` on `DocumentSpec` and provide `history` in `PostgresDocumentConfig` when you use a history table. If `history` is missing while `history_enabled` is true (or the reverse), the adapter logs a warning and skips the history gateway.

### History table schema

| Column | Type | Purpose |
|--------|------|---------|
| `source` | `text` | Source relation name (e.g. `public.projects`) |
| `id` | `uuid` | Document identifier |
| `rev` | `integer` | Revision number |
| `data` | `jsonb` | Full document snapshot at that revision |

### Example DDL

    :::sql
    CREATE TABLE public.projects_history (
        source  text    NOT NULL,
        id      uuid    NOT NULL,
        rev     integer NOT NULL,
        data    jsonb   NOT NULL,
        PRIMARY KEY (source, id, rev)
    );

    CREATE INDEX idx_projects_history_lookup
    ON public.projects_history (source, id, rev);

### History writes

Use either database triggers (copy `OLD` row before update) or application-level inserts, consistent with `bookkeeping_strategy` and your operational preferences.

    :::sql
    CREATE OR REPLACE FUNCTION write_document_history()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO public.projects_history (source, id, rev, data)
        VALUES ('public.projects', OLD.id, OLD.rev, to_jsonb(OLD));
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER projects_history_trigger
    BEFORE UPDATE ON public.projects
    FOR EACH ROW EXECUTE FUNCTION write_document_history();

## Transactions

The Postgres adapter uses `psycopg` async connections with context-variable scoping. Within an `ExecutionContext.transaction()` scope, document operations share the same connection when resolved through the Postgres tx manager route you registered in `PostgresDepsModule.tx`.

Nested `transaction()` calls create savepoints:

    :::python
    async with ctx.transaction("default"):
        await doc_c.create(cmd_1)

        async with ctx.transaction("default"):
            await doc_c.create(cmd_2)

If the inner block raises, only the savepoint is rolled back.

## Document operations

Resolve ports from `ExecutionContext` using the same `DocumentSpec` you use in the kernel:

    :::python
    doc_q = ctx.doc_query(project_spec)
    doc_c = ctx.doc_command(project_spec)

    project = await doc_q.get(project_id)
    projects, total = await doc_q.find_many(
        filters={"$fields": {"is_deleted": False}},
        sorts={"created_at": "desc"},
        limit=20,
        offset=0,
    )
    count = await doc_q.count({"$fields": {"is_deleted": False}})

    created = await doc_c.create(CreateProjectCmd(title="New", description="..."))
    updated = await doc_c.update(project_id, UpdateProjectCmd(title="Updated"), rev=1)
    await doc_c.delete(project_id)
    await doc_c.restore(project_id)
    await doc_c.kill(project_id)

The adapter handles revision checks, cache coordination when `DocumentSpec.cache` is set, history when configured, and query rendering via the shared query DSL.

## Full-text search

The search stack has two layers:

1. **`SearchSpec`** (kernel) — `name`, `model_type`, `fields`, weights, optional `fuzzy`
2. **`PostgresSearchConfig`** — `engine` (`"pgroonga"` or `"fts"`), `index` and `source` as `(schema, name)` tuples, optional `fts_groups` for native FTS

| Engine | Extension | Best for |
|--------|-----------|----------|
| **PGroonga** | `pgroonga` | CJK, fuzzy, array expressions |
| **Native FTS** | Built-in | GIN + `tsvector` |

### Example kernel + infra

    :::python
    from forze.application.contracts.search import SearchSpec

    project_search = SearchSpec(
        name="projects",
        model_type=ProjectReadModel,
        fields=("title", "description"),
        default_weights={"title": 0.6, "description": 0.4},
    )

    # PostgresDepsModule.searches["projects"] must match project_search.name
    searches={
        "projects": {
            "engine": "fts",
            "index": ("public", "idx_projects_fts"),
            "source": ("public", "projects"),
            "fts_groups": {
                "A": ("title",),
                "B": ("description",),
            },
        },
    }

For `"fts"`, every field in `SearchSpec.fields` must appear in `fts_groups`. For `"pgroonga"`, `fts_groups` is omitted.

### PGroonga indexes

    :::sql
    CREATE EXTENSION IF NOT EXISTS pgroonga;

    CREATE INDEX idx_projects_title ON public.projects
    USING pgroonga (title pgroonga_text_full_text_search_ops);

### Native FTS (GIN + tsvector)

    :::sql
    ALTER TABLE public.projects
    ADD COLUMN doc_tsv tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A')
        || setweight(to_tsvector('english', coalesce(description, '')), 'B')
    ) STORED;

    CREATE INDEX idx_projects_fts ON public.projects USING gin (doc_tsv);

### Using the search port

    :::python
    search = ctx.search_query(project_search)

    hits, total = await search.search(
        query="roadmap",
        filters={"$fields": {"is_deleted": False}},
        limit=20,
        offset=0,
    )

See [Query Syntax](../core-package/query-syntax.md) for filter and sort expressions.

## Combining with Redis

Typical stack: Postgres for persistence, Redis for cache and idempotency. Use the **same logical names** on `CacheSpec` / `DocumentSpec` and in `RedisDepsModule.caches`:

    :::python
    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            PostgresDepsModule(client=pg, rw_documents={...}, searches={...})(),
            RedisDepsModule(
                client=redis,
                caches={"projects": {"namespace": "app:projects"}},
            )(),
        ),
    )

    lifecycle = LifecyclePlan.from_steps(
        postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
        redis_lifecycle_step(dsn="redis://...", config=RedisConfig()),
    )

When `DocumentSpec.cache` is set, `doc_query` / `doc_command` resolve a cache port for that `CacheSpec.name` and pass it into the Postgres adapter.

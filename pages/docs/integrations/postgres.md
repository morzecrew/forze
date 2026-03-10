# PostgreSQL Integration

`forze_postgres` provides document storage, full-text search, and transaction management backed by PostgreSQL. It implements `DocumentReadPort`, `DocumentWritePort`, `SearchReadPort`, and `TxManagerPort` using async `psycopg` with connection pooling.

## Installation

```bash
uv add 'forze[postgres]'
```

Requires PostgreSQL 14 or later.

## Runtime wiring

Create a client, register it via the dependency module, and add a lifecycle step for pool management:

```python
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
    rev_bump_strategy="database",
    history_write_strategy="database",
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
```

### PostgresConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| `min_size` | `int` | `2` | Minimum connections in the pool |
| `max_size` | `int` | `10` | Maximum connections in the pool |
| `timeout` | `float` | `30.0` | Connection acquisition timeout (seconds) |

### DepsModule options

| Option | Type | Values | Purpose |
|--------|------|--------|---------|
| `rev_bump_strategy` | `str` | `"database"`, `"application"` | Who increments `rev` on update |
| `history_write_strategy` | `str` | `"database"`, `"application"` | Who writes to the history table |

### What gets registered

`PostgresDepsModule` registers these dependency keys:

| Key | Capability |
|-----|-----------|
| `PostgresClientDepKey` | Raw Postgres client for direct queries |
| `DocumentReadDepKey` | Document read adapter factory |
| `DocumentWriteDepKey` | Document write adapter factory |
| `TxManagerDepKey` | Transaction manager adapter |
| `SearchReadDepKey` | Full-text search adapter factory |

## Document sources

Document specs use **source names** in `schema.table` format. The adapter splits on `.` to derive the schema and table name.

```python
from forze.application.contracts.document import DocumentSpec

spec = DocumentSpec(
    namespace="projects",
    read={"source": "public.projects", "model": ProjectReadModel},
    write={
        "source": "public.projects",
        "models": {
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
    },
    history={"source": "public.projects_history"},
    cache={"enabled": True},
)
```

Read and write can point to the same table or different relations (e.g. a view for reads, a table for writes).

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

```sql
CREATE TABLE public.projects (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    rev             integer     NOT NULL DEFAULT 1,
    created_at      timestamptz NOT NULL DEFAULT now(),
    last_update_at  timestamptz NOT NULL DEFAULT now(),
    is_deleted      boolean     NOT NULL DEFAULT false,
    title           text        NOT NULL,
    description     text        NOT NULL
);
```

## Revision strategy

The write adapter supports two strategies for bumping `rev` on update:

| Strategy | Behavior |
|----------|----------|
| `"database"` | A database trigger increments `rev`. The application does not set it. |
| `"application"` | The application computes `rev + 1` in the update payload. |

### Database trigger for revision bumping

When using `rev_bump_strategy="database"`, create a trigger:

```sql
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
```

The trigger also updates `last_update_at` to ensure consistency between application and database timestamps.

## History table (audit trail)

When `history` is specified in the document spec, the adapter stores previous document revisions for audit and historical consistency checks.

### History table schema

| Column | Type | Purpose |
|--------|------|---------|
| `source` | `text` | Source relation name (e.g. `public.projects`) |
| `id` | `uuid` | Document identifier |
| `rev` | `integer` | Revision number |
| `data` | `jsonb` | Full document snapshot at that revision |

### Example DDL

```sql
CREATE TABLE public.projects_history (
    source  text    NOT NULL,
    id      uuid    NOT NULL,
    rev     integer NOT NULL,
    data    jsonb   NOT NULL,
    PRIMARY KEY (source, id, rev)
);

CREATE INDEX idx_projects_history_lookup
ON public.projects_history (source, id, rev);
```

### History write strategy

| Strategy | Behavior |
|----------|----------|
| `"database"` | A trigger copies the old row into the history table before each update |
| `"application"` | The adapter inserts history rows after each update |

### Database trigger for history

```sql
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
```

## Transactions

The Postgres adapter uses `psycopg` async connections with context-variable scoping. Within an `ExecutionContext.transaction()` scope, all document operations share the same connection and participate in the same database transaction.

Nested `transaction()` calls create savepoints:

```python
async with ctx.transaction():
    await doc.create(cmd_1)

    async with ctx.transaction():
        await doc.create(cmd_2)
        # This uses a savepoint inside the outer transaction
```

If the inner block raises, only the savepoint is rolled back. The outer transaction can still commit successfully.

## Document operations

Once wired, resolve and use document ports from the execution context:

```python
doc_read = ctx.doc_read(project_spec)
doc_write = ctx.doc_write(project_spec)

# Read operations
project = await doc_read.get(project_id)
projects, total = await doc_read.find_many(
    filters={"$fields": {"is_deleted": False}},
    sorts={"created_at": "desc"},
    limit=20,
    offset=0,
)
count = await doc_read.count({"$fields": {"is_deleted": False}})

# Write operations
created = await doc_write.create(CreateProjectCmd(title="New", description="..."))
updated = await doc_write.update(project_id, UpdateProjectCmd(title="Updated"), rev=1)
await doc_write.delete(project_id)
await doc_write.restore(project_id)
await doc_write.kill(project_id)
```

The adapter automatically handles:

- Revision checking for optimistic concurrency (when `rev` is passed)
- Cache invalidation (when cache is enabled in the spec)
- History recording (when history is configured)
- Query filter/sort rendering using the shared query DSL

## Full-text search

The Postgres search adapter supports two full-text search engines:

| Engine | Extension | Best for |
|--------|-----------|----------|
| **PGroonga** | `pgroonga` | CJK languages, fuzzy matching, array field search |
| **Native FTS** | Built-in | Standard PostgreSQL GIN + tsvector search |

The adapter introspects index definitions from the PostgreSQL catalog to auto-detect which engine to use.

### Search specification

Search is configured via `SearchSpec`, separate from `DocumentSpec`:

```python
from forze.application.contracts.search import SearchSpec

project_search = SearchSpec(
    namespace="projects",
    model=ProjectReadModel,
    indexes={
        "public.idx_projects_title": {
            "fields": [{"path": "title"}],
            "source": "public.projects",
        },
        "public.idx_projects_content": {
            "fields": [
                {"path": "title", "weight": 2.0},
                {"path": "description", "weight": 1.0},
            ],
            "source": "public.projects",
        },
    },
    default_index="public.idx_projects_title",
)
```

Index names in the spec must match the actual PostgreSQL index names. Use `schema.indexname` when the index is not in the `public` schema.

### PGroonga indexes

Install the extension and create indexes:

```sql
CREATE EXTENSION IF NOT EXISTS pgroonga;

-- Single field index
CREATE INDEX idx_projects_title ON public.projects
USING pgroonga (title pgroonga_text_full_text_search_ops);

-- Multi-field index using array expression
CREATE INDEX idx_projects_content ON public.projects
USING pgroonga (
    (ARRAY[title::text, description::text])
    pgroonga_text_array_full_text_search_ops
);
```

### Native FTS indexes (GIN + tsvector)

No extension required. Create a generated tsvector column or an expression index:

```sql
-- Option 1: Generated tsvector column
ALTER TABLE public.projects
ADD COLUMN title_tsv tsvector
GENERATED ALWAYS AS (
    to_tsvector('english', coalesce(title, ''))
) STORED;

CREATE INDEX idx_projects_fts ON public.projects
USING gin (title_tsv);

-- Option 2: Expression index
CREATE INDEX idx_projects_fts ON public.projects
USING gin (
    to_tsvector('english',
        coalesce(title, '') || ' ' || coalesce(description, ''))
);
```

### Search groups (FTS ranking)

For native FTS, define weight groups to control `ts_rank` weighting:

```python
indexes={
    "public.idx_projects_weighted": {
        "fields": [
            {"path": "title", "group": "title"},
            {"path": "description", "group": "body"},
        ],
        "groups": [
            {"name": "title", "weight": 1.0},
            {"name": "body", "weight": 0.4},
        ],
        "default_group": "title",
        "source": "public.projects",
    },
}
```

### Using the search port

```python
search = ctx.search(project_search)

hits, total = await search.search(
    query="roadmap",
    filters={"$fields": {"is_deleted": False}},
    limit=20,
    offset=0,
)
```

The search port supports all the same filter and sort expressions as document ports (see [Query Syntax](../core-package/query-syntax.md)).

## Combining with other modules

Postgres is typically combined with Redis for caching and idempotency:

```python
deps_plan = DepsPlan.from_modules(
    lambda: Deps.merge(
        PostgresDepsModule(client=pg, rev_bump_strategy="database", history_write_strategy="database")(),
        RedisDepsModule(client=redis)(),
    ),
)

lifecycle = LifecyclePlan.from_steps(
    postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
    redis_lifecycle_step(dsn="redis://...", config=RedisConfig()),
)
```

When both modules are registered and the document spec has `cache.enabled = True`, the document adapter automatically uses Redis for caching reads.

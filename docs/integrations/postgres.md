# PostgreSQL Integration

This guide explains how to set up PostgreSQL so that Forze document adapters work correctly with your domain structures. It covers schema requirements, relation naming, revision handling, history tables, full-text search, and connection configuration.

## Prerequisites

- PostgreSQL 14+ (or compatible)
- `forze[postgres]` extra installed

## Connection

The Postgres client uses a DSN and optional pool configuration. Initialize before running operations:

```python
from forze_postgres import PostgresClient, PostgresConfig

client = PostgresClient()
await client.initialize(
    "postgresql://user:pass@localhost:5432/mydb",
    config=PostgresConfig(min_size=2, max_size=15),
)
```

The client supports connection pooling, context-bound transactions (including nested savepoints), and configurable timeouts. See :class:`forze_postgres.kernel.platform.client.PostgresConfig` for pool options.

## Relation Naming

Document specs use **relation names** in `schema.table` format. The read, write, and optional history relations must follow this convention:

| Relation | Purpose | Example |
|----------|---------|---------|
| `read` | Table or view used for reads | `public.documents` |
| `write` | Table used for writes | `public.documents` |
| `history` | Optional audit table | `public.documents_history` |

Read and write can point to the same table or to different relations (e.g. a view for reads, a table for writes). Both must use `schema.table`; splitting on `.` yields schema and table name.

## Document Table Schema

Every document table must include the core fields expected by the domain model. Column names must match the domain constants.

### Required Columns

| Column | Type | Purpose |
|--------|------|---------|
| `id` | `uuid` | Primary key, document identifier |
| `rev` | `integer` | Revision number for optimistic concurrency |
| `created_at` | `timestamp with time zone` | Creation timestamp |
| `last_update_at` | `timestamp with time zone` | Last update timestamp |

### Optional Columns

| Column | Type | Purpose |
|--------|------|---------|
| `is_deleted` | `boolean` | Soft delete flag (default `false`) |
| `number_id` | `bigint` | Human-readable sequence number |
| `creator_id` | `uuid` | Creator reference |
| `tenant_id` | `uuid` | Tenant for multi-tenancy |

Add any domain-specific columns as needed. Column names must match the Pydantic model field names (snake_case by default).

### Example DDL

```sql
CREATE TABLE public.documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    rev integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_update_at timestamptz NOT NULL DEFAULT now(),
    is_deleted boolean NOT NULL DEFAULT false,
    -- domain-specific columns
    title text,
    body text
);
```

## Revision Strategy

The write gateway supports two strategies for bumping `rev` on update:

| Strategy | Behavior |
|----------|----------|
| `"database"` | A trigger increments `rev` on update. The application does not set `rev`. |
| `"application"` | The application computes and sets `rev + 1` in the update payload. |

Use `"database"` when you want the database to own revision bumps (e.g. via trigger). Use `"application"` when you prefer application-side control.

### Trigger for `"database"` Strategy

When using `rev_bump_strategy="database"`, create a trigger that increments `rev` and updates `last_update_at`:

```sql
CREATE OR REPLACE FUNCTION bump_rev()
RETURNS TRIGGER AS $$
BEGIN
    NEW.rev := OLD.rev + 1;
    NEW.last_update_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER documents_bump_rev
    BEFORE UPDATE ON public.documents
    FOR EACH ROW
    EXECUTE FUNCTION bump_rev();
```

## History Table (Audit Trail)

When using historical consistency validation, configure an optional history table. The history table stores snapshots of document state per revision.

### History Table Schema

| Column | Type | Purpose |
|--------|------|---------|
| `source` | `text` | Source relation (e.g. `public.documents`) |
| `id` | `uuid` | Document identifier |
| `rev` | `integer` | Revision number |
| `data` | `jsonb` | Full document snapshot |

### Example DDL

```sql
CREATE TABLE public.documents_history (
    source text NOT NULL,
    id uuid NOT NULL,
    rev integer NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (source, id, rev)
);

CREATE INDEX idx_documents_history_lookup
    ON public.documents_history (source, id, rev);
```

### History Write Strategy

| Strategy | Behavior |
|----------|----------|
| `"database"` | A trigger on the write table inserts into the history table. The gateway does not write history. |
| `"application"` | The gateway inserts history rows after each update. |

For `"database"`, create a trigger that copies the previous row into the history table before update:

```sql
CREATE OR REPLACE FUNCTION write_document_history()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.documents_history (source, id, rev, data)
    VALUES ('public.documents', OLD.id, OLD.rev, to_jsonb(OLD));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER documents_history_trigger
    BEFORE UPDATE ON public.documents
    FOR EACH ROW
    EXECUTE FUNCTION write_document_history();
```

For `"application"`, no trigger is needed; the gateway writes history directly.

## Full-Text Search (pgroonga)

When using the search gateway, full-text search is powered by the **pgroonga** extension. Install and create indexes that match the search spec.

### Enable Extension

```sql
CREATE EXTENSION IF NOT EXISTS pgroonga;
```

### Search Index Spec

The `DocumentSearchSpec` maps index names to fields and optional weights:

- `{"idx_title": ("title",)}` — single field, default weight
- `{"idx_content": {"title": 2, "body": 1}}` — multiple fields with weights

Index names in the spec must match the actual PostgreSQL index names.

### Creating pgroonga Indexes

**Single field:**

```sql
CREATE INDEX idx_title ON public.documents
    USING pgroonga (title pgroonga_text_full_text_search_ops);
```

**Multiple fields (array expression):**

The search gateway uses `(ARRAY[field1::text, field2::text, ...])` for multi-field search. Create a matching index:

```sql
CREATE INDEX idx_title_body ON public.documents
    USING pgroonga (
        (ARRAY[title::text, body::text]) pgroonga_text_array_full_text_search_ops
    );
```

Ensure the index name and field order match your `DocumentSearchSpec`.

## DocumentSpec Configuration

Wire the schema into a :class:`DocumentSpec`:

```python
from forze.application.contracts.document import DocumentSpec, DocumentRelationSpec, DocumentModelSpec

spec = DocumentSpec(
    namespace="documents",
    relations=DocumentRelationSpec(
        read="public.documents",
        write="public.documents",
        history="public.documents_history",  # optional
    ),
    models=DocumentModelSpec(
        read=DocumentReadModel,
        domain=DocumentModel,
        create_cmd=CreateDocumentCmd,
        update_cmd=UpdateDocumentCmd,
    ),
    search={"idx_title": ("title",), "idx_content": {"title": 2, "body": 1}},  # optional
    enable_cache=False,
)
```

## Checklist

- [ ] PostgreSQL 14+ with `pgroonga` extension (if using search)
- [ ] Document table with `id`, `rev`, `created_at`, `last_update_at`
- [ ] Optional: `is_deleted` for soft delete, `number_id`, `creator_id`, `tenant_id`
- [ ] Relation names in `schema.table` format
- [ ] Trigger for `rev` bump when using `rev_bump_strategy="database"`
- [ ] History table and trigger when using `history_write_strategy="database"`
- [ ] pgroonga indexes matching search spec when using full-text search
- [ ] Postgres client initialized with DSN before operations

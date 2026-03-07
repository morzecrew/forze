# PostgreSQL Integration

This guide explains how to set up PostgreSQL for Forze document adapters and full-text search. It covers schema requirements, source naming, revision handling, history tables, search (PGroonga and native FTS), and connection configuration.

## Prerequisites

- PostgreSQL 14+ (or compatible)
- `forze[postgres]` extra installed

## Connection

The Postgres client uses a DSN and optional pool configuration. Initialize before running operations. Use `PostgresDepsModule` to register the client and ports, and `postgres_lifecycle_step` for startup/shutdown:

    :::python
    from forze.application.execution import Deps, LifecyclePlan
    from forze_postgres import (
        PostgresClient, 
        PostgresConfig, 
        PostgresDepsModule, 
        postgres_lifecycle_step,
    )

    client = PostgresClient()
    deps_module = PostgresDepsModule(
        client=client,
        rev_bump_strategy="database",
        history_write_strategy="database",
    )

    # Build deps and lifecycle
    deps = deps_module()
    lifecycle = LifecyclePlan.from_steps(
        postgres_lifecycle_step(
            dsn="postgresql://user:pass@localhost:5432/mydb",
            config=PostgresConfig(min_size=2, max_size=15),
        )
    )

The client supports connection pooling, context-bound transactions (including nested savepoints), and configurable timeouts. See `forze_postgres.kernel.platform.client.PostgresConfig` for pool options.

## Document Sources

Document specs use **source names** in `schema.table` format. The read, write, and optional history sources must follow this convention:

| Source | Purpose | Example |
|--------|---------|---------|
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

    :::sql
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

## Revision Strategy

The write gateway supports two strategies for bumping `rev` on update:

| Strategy | Behavior |
|----------|----------|
| `"database"` | A trigger increments `rev` on update. The application does not set `rev`. |
| `"application"` | The application computes and sets `rev + 1` in the update payload. |

Use `"database"` when you want the database to own revision bumps (e.g. via trigger). Use `"application"` when you prefer application-side control.

### Trigger for `"database"` Strategy

When using `rev_bump_strategy="database"`, create a trigger that increments `rev` and updates `last_update_at`:

    :::sql
    -- Create the function
    CREATE OR REPLACE FUNCTION bump_rev()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.rev := OLD.rev + 1;
        NEW.last_update_at := now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Create the trigger
    CREATE TRIGGER documents_bump_rev
    BEFORE UPDATE ON public.documents
    FOR EACH ROW
    EXECUTE FUNCTION bump_rev();

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

    :::sql
    CREATE TABLE public.documents_history (
        source text NOT NULL,
        id uuid NOT NULL,
        rev integer NOT NULL,
        data jsonb NOT NULL,
        PRIMARY KEY (source, id, rev)
    );

    CREATE INDEX idx_documents_history_lookup
    ON public.documents_history (source, id, rev);

### History Write Strategy

| Strategy | Behavior |
|----------|----------|
| `"database"` | A trigger on the write table inserts into the history table.<br>The gateway does not write history. |
| `"application"` | The gateway inserts history rows after each update. |

For `"database"`, create a trigger that copies the previous row into the history table before update:

    :::sql
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

For `"application"`, no trigger is needed; the gateway writes history directly.

## Full-Text Search

The Postgres search adapter supports two engines:

| Engine | Extension | Use case |
|--------|-----------|----------|
| **PGroonga** | `pgroonga` | Japanese/CJK, fuzzy search, array fields |
| **fts** | Built-in | Native PostgreSQL GIN + tsvector |

The adapter introspects index definitions to detect the engine and routes queries accordingly.

### Search Specification

Search is configured via :class:`SearchSpec`, separate from :class:`DocumentSpec`. Each index has a **source** (the table being indexed) and **fields** (column paths). Index names in the spec must match the actual PostgreSQL index names (use `schema.indexname` when the index is not in `public`).

    :::python
    from forze.application.contracts.search import SearchSpec
    from pydantic import BaseModel

    class DocumentReadModel(BaseModel):
        id: str
        title: str
        body: str

    search_spec = SearchSpec(
        namespace="documents",
        model=DocumentReadModel,
        indexes={
            "public.idx_title": {
                "fields": [{"path": "title"}],
                "source": "public.documents",
            },
            "public.idx_content": {
                "fields": [
                    {"path": "title", "weight": 2.0},
                    {"path": "body", "weight": 1.0},
                ],
                "source": "public.documents",
            },
        },
        default_index="public.idx_title",
    )

### PGroonga

Install and create indexes:

    :::sql
    CREATE EXTENSION IF NOT EXISTS pgroonga;

**Single field:**

    :::sql
    CREATE INDEX idx_title ON public.documents
    USING pgroonga (title pgroonga_text_full_text_search_ops);

**Multiple fields (array expression):**

    :::sql
    CREATE INDEX idx_title_body ON public.documents
    USING pgroonga (
        (ARRAY[title::text, body::text]) 
        pgroonga_text_array_full_text_search_ops
    );

Ensure index names and field order match your index specification. Each index must specify `source` (the table) in the spec.

### Native FTS (GIN + tsvector)

No extension required. Create a tsvector column or expression index:

    :::sql
    -- Option 1: tsvector column
    ALTER TABLE public.documents ADD COLUMN title_tsv tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title, ''))
    ) STORED;
    CREATE INDEX idx_documents_fts ON public.documents USING gin (title_tsv);

    -- Option 2: expression index
    CREATE INDEX idx_documents_fts ON public.documents
    USING gin (
        to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, ''))
    );

For expression indexes, the adapter infers the tsvector expression from the catalog. For generated columns, you can pass `hints={"tsvector_col": "title_tsv"}` in the field spec or index hints.

### Search Groups (FTS ranking)

For FTS, you can define **groups** (ordered by weight) to map fields to ts_rank weights A–D. Groups are part of the index specification:

    :::python
    # index specification
    {
        "fields": [
            {"path": "title", "group": "title"},
            {"path": "body", "group": "body"},
        ],
        "groups": [
            {"name": "title", "weight": 1.0},
            {"name": "body", "weight": 0.4},
        ],
        "default_group": "title",
        "source": "public.documents",
    }

## Document Specification

Wire the schema into a `DocumentSpec`:

    :::python
    from forze.application.contracts.document import (
        DocumentSpec, 
        DocumentModelSpec,
    )

    spec = DocumentSpec(
        namespace="documents",
        sources={
            "read": "public.documents",
            "write": "public.documents",
            "history": "public.documents_history",  # optional
        },
        models=DocumentModelSpec(
            read=DocumentReadModel,
            domain=DocumentModel,
            create_cmd=CreateDocumentCmd,
            update_cmd=UpdateDocumentCmd,
        ),
        cache=None,  # or DocumentCacheSpec for caching
    )

When `cache` is enabled, the cache port is wired automatically from the execution context: it is resolved via `CacheDepKey` (directly or through a router) and injected into the document adapter when the document port is resolved.

Search is configured separately via `SearchSpec` and resolved from the execution context via `SearchReadDepKey`.
---
title: PostgreSQL
icon: lucide/database
summary: Document storage, search, and transactions on PostgreSQL
---

`forze[postgres]` implements document storage (read + write), full-text / vector
/ hub / federated search, transaction coordination, the transactional outbox /
inbox, idempotency, and durable execution on PostgreSQL. Persistence stays behind
Forze contracts; PostgreSQL tables, indexes, and pools live at the edge.

## Install

```bash
uv add 'forze[postgres]'
```

Needs a reachable PostgreSQL. Search engines may need extensions (PGroonga for
ranked text, pgvector for similarity).

## The client

```python
from forze_postgres import PostgresClient

pg = PostgresClient()
```

Use `RoutedPostgresClient` when the tenant or route decides the DSN — see
[Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

### Settings

`PostgresSettings` builds the DSN from parts, so URI grammar — percent-encoding a
password that contains `@`, bracketing an IPv6 host, choosing an `sslmode` that actually
authenticates the server — stays in this package rather than in every application's
settings module:

```python
from forze_postgres import PostgresSettings

pg_settings = PostgresSettings(host="db.internal", port=5432, database="orders", ssl=True)

pg_settings.dsn      # SecretStr("postgresql://postgres:@db.internal:5432/orders?sslmode=verify-full")
pg_settings.config   # PostgresConfig, from the pool fields that are set
```

A plain `BaseModel`, not a `BaseSettings`: mount it on your own root settings class, which
owns the environment prefix and delimiter. `host` has no default — an unset one raises a
configuration error naming the setting rather than connecting to whatever is on localhost.

### Bulk loading

`copy_rows` runs `COPY … FROM STDIN`, the engine's own bulk path — no bind-parameter
ceiling, and 20–79× faster than a multi-VALUES `INSERT` at 10⁴–10⁵ rows:

```python
loaded = await pg.copy_rows(
    ("analytics", "events"),          # (schema, table)
    ("id", "occurred_at", "payload"), # columns, in row order
    rows,                             # tuples, sync or async iterable
)
```

The target is a tuple rather than a string so it cannot arrive pre-joined from an f-string;
both it and the column names are composed as identifiers, which is the part a hand-rolled
`COPY` gets wrong. Rows may be an async iterator and are consumed one at a time, so a
pipeline can stream decode → transform → load without holding the dataset.

One bad row aborts the whole load and nothing is written — there is no skip-bad-rows mode.
The error carries the server's line and column, so a rejected row in a million is locatable.
Inside `transaction()` the copy joins that transaction; a rollback removes every row.

Text format by default lets the server cast, which is what runtime-created tables want. Pass
`binary=True` with `column_types` when you control both sides — but note the two formats want
different Python values for `json`/`jsonb`: text takes JSON *text*, binary takes a mapping.
Passing text in binary mode is refused rather than silently stored as a quoted string.

## Wire it

Map each logical spec name to physical relations, register them on the deps
module, and open the pool from the lifecycle plan:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_postgres import (
    PostgresConfig,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresLifecycleModule,
)

orders_pg = PostgresDocumentConfig(
    write=("public", "orders"),
    read=("public", "orders"),
    bookkeeping_strategy="application",  # or "database" with an UPDATE trigger
)

deps = DepsRegistry.from_modules(
    PostgresDepsModule(client=pg, rw_documents={"orders": orders_pg}, tx={"orders"}),
)
lifecycle = LifecyclePlan.from_modules(
    PostgresLifecycleModule(client=pg, dsn="postgresql://…", config=PostgresConfig()),
)
```

## What it provides

| Contract | Keyed by | Notes |
|----------|----------|-------|
| Document query / command | `DocumentSpec.name` (`rw_documents`, `ro_documents`) | read-write or read-only relations |
| Search | `SearchSpec.name` (`searches`, `hub_searches`, `federated_searches`) | `engine="pgroonga"`, `FtsEngine(...)`, or `VectorEngine(...)` |
| Transactions | route in the module `tx` set | coordinates Postgres-backed ports on one connection |
| Analytics | `AnalyticsSpec.name` (`analytics`) | named, parameterized warehouse SQL — optional |
| Outbox / inbox | `OutboxSpec.name` (`outboxes`), `InboxSpec.name` (`inboxes`) | transactional outbox + consumer-side dedup inbox |
| Idempotency | `IdempotencySpec.name` (`idempotencies`) | co-located store — the record commits inside the business transaction; add the optional `owner uuid` column to fence a reclaimed claim ([idempotency](../writing-operation/idempotency.md)) |
| Counter | `CounterSpec.name` (`counters`) | atomic upsert-increment over an app-provided table (admin enumeration included) |
| Procedures | `ProcedureSpec.name` (`procedures`) | named, governed SQL command / compute |
| Durable execution | `durable_step` / `durable_run` / `durable_schedule` | step memo, run store, and cron schedules — optional |

## Notes

- **You own the schema.** Forze *introspects* existing relations; it never
  creates application tables. Provision read / write / history / search relations
  (and extensions) with your migration tool first.
- **Bookkeeping.** `bookkeeping_strategy="application"` bumps `rev` /
  `last_update_at` in the write gateway; `"database"` defers to a `BEFORE UPDATE`
  trigger you supply.
- **Routed clients** require `introspector_cache_partition_key` on the deps
  module so the catalog cache partitions by tenant.
- **Relations** can be static `(schema, table)` tuples or per-tenant resolvers —
  see [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).
- **Indexes for sorted pages.** A sort on a `NOT NULL` column, or on the record `id`,
  renders without a null placement, so a plain btree index serves it in either direction,
  through a view too. A nullable column keeps the canonical placement (ascending nulls
  first, descending nulls last), which a plain index does not hold: index it with the
  placement your pages use, for example `(created_at DESC NULLS LAST)`. A view's columns
  other than the `id` all read as nullable.
- **PGroonga indexes.** Index the searched columns as one column, `USING pgroonga (title)`,
  or as an array, `USING pgroonga ((ARRAY[title, content]))`; an element may also carry a
  cast or `COALESCE(col, '')`. The search matches against the index's own expression, so
  the planner can use the index. What matches, and in what order, is then the index's own
  tokenizer and normalizer: with the default ones, `python` matches the word `python` and
  not `pythonic`. An element Forze cannot rebuild around a column, such as `lower(title)`,
  is refused when the search first runs.
- **PGroonga and VACUUM.** On PGroonga 4.0.6 with Groonga 16.0.1 and PostgreSQL 18.1, a
  PGroonga index built while a `VACUUM` or `ANALYZE` (autovacuum included) runs on another
  PGroonga-indexed table can break: searches through it return no rows, or fail with
  `object isn't found: <Sources…>`, until you `REINDEX` it. Build PGroonga indexes when no
  vacuum is running, or reindex them afterwards
  ([pgroonga/pgroonga#1013](https://github.com/pgroonga/pgroonga/issues/1013)).

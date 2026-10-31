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

To check every bound document's relations at startup, build the schema specs from the spec
registry and the deps module:

```python
from forze_postgres import postgres_document_schema_specs

pg_deps = PostgresDepsModule(client=pg, rw_documents={"orders": orders_pg}, tx={"orders"})

PostgresLifecycleModule(
    client=pg,
    dsn="postgresql://…",
    schema_specs=postgres_document_schema_specs(spec_registry, pg_deps),
)
```

Each route in `ro_documents` and `rw_documents` is paired with the `DocumentSpec` of the same
name, so a document added later is validated too. A bound route with no spec in the registry
is refused with `postgres_schema_spec_uncatalogued`. Pass `exclude=` for routes you leave
unvalidated on purpose. A route on a per-tenant relation resolver is skipped with a warning,
because its relation names are known only per request. A catalogued document that nothing
binds is refused by the inventory reconciliation when the runtime is built with the registry.

Validation checks every document before startup fails, so one boot names all that need a
migration. One refused document raises its own error. Several raise one
`postgres_schema_documents_refused` error whose summary names each, with `details["failures"]`
holding each document's `code`, `summary` and `details`. An error that concerns no single
document, such as a lost connection or a missing tenant partition, stops the checks at once.
The documents refused before it are then raised, with that error as the cause.

A required field must map to a `NOT NULL` column. On a read relation that is a view or a
materialized view, nullability is not checked: Postgres records `NOT NULL` on tables only, so
each column of a view reads as nullable. A write or history relation keeps the check.

Catalog warmup takes the search configs the deps module already holds. Pass its mappings, so a
search added later is warmed too:

```python
PostgresLifecycleModule(
    client=pg,
    dsn="postgresql://…",
    searches=pg_deps.searches,
    hub_searches=pg_deps.hub_searches,
    federated_searches=pg_deps.federated_searches,
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
- **Counter lifetimes need one column.** A counter written with a `ttl` stores its lapse
  instant in an optional `expires_at timestamptz` column:

    ```sql
    ALTER TABLE <counters> ADD COLUMN expires_at timestamptz;
    ```

    A table without it keeps working for every call that does not pass a `ttl`; a call
    that does is refused with `configuration` (`counter_ttl_unsupported`) until the column
    exists. The login lockout is the exception: it writes without a `ttl` instead and warns
    once, so logins keep working while its old keys pile up. After the migration a `ttl`
    call picks the column up within a minute, without a restart. A
    lapsed row reads as absent but stays in the table. To delete lapsed rows on a schedule,
    add `postgres_counter_purge_lifecycle_step`. It calls
    `PostgresCounterAdminAdapter.purge_expired()` for each route on an interval, which walks
    the primary key in batches and needs no other index:

    ```python
    from forze.application.contracts.counter import CounterSpec
    from forze_postgres import postgres_counter_purge_lifecycle_step

    lifecycle = lifecycle.with_steps(
        postgres_counter_purge_lifecycle_step(
            specs=[CounterSpec(name="quota")],
            interval=timedelta(minutes=10),  # the default
        ),
    )
    ```

    Each route must be wired in `counters=`, or startup fails with `configuration`. On
    per-tenant routes (`tenant_aware`, a per-tenant table or a database per tenant) pass
    `tenants=`, such as `lambda: shard.tenants`, to purge each tenant's rows in turn; without
    it the step refuses them at startup. With it, a tenant-global route is refused, because
    every tenant's pass would walk the same rows: purge it in a step without `tenants=`. An
    empty shard logs a warning and purges nothing. Racing purges are safe, so run the step on
    every replica. Or schedule a table-wide purge (for example with pg_cron), with a partial
    index so it does not scan the live rows:

    ```sql
    CREATE INDEX ON <counters> (expires_at) WHERE expires_at IS NOT NULL;
    DELETE FROM <counters> WHERE expires_at <= now();
    ```

    See [Lifetimes](../reference/contracts/stores.md#lifetimes-ttl).
- **Bookkeeping.** `bookkeeping_strategy="application"` bumps `rev` /
  `last_update_at` in the write gateway; `"database"` defers to a
  `BEFORE UPDATE … FOR EACH ROW` trigger you supply. With `history_enabled`, your triggers also write the history row of
  every revision, the created one included; startup validation warns when no trigger on the
  write relation fires on `INSERT`.
- **Routed clients** require `introspector_cache_partition_key` on the deps
  module so the catalog cache partitions by tenant.
- **Connections the server ended.** A restart, a failover, `pg_terminate_backend` or
  `DROP DATABASE … WITH (FORCE)` ends every pooled session at once. The pool pings each
  connection before handing it out, and the first dead one makes it check all its idle
  connections, so requests get live connections instead of failing on dead ones. It then
  opens their replacements at once, up to `max_size` new connections, which is by design.
  The cost is one round trip per checkout; a transaction checks out once. The acquire timeout
  covers both the wait for a connection and its ping, except that a ping always gets at least
  a second, or the whole acquire timeout if that is shorter, so a connection handed over just
  before the deadline is not discarded for lack of time. A ping unanswered by then fails, and
  that connection is discarded. The check of the
  idle connections has no such bound: on one that stops answering it waits until TCP gives
  up, and the idle connections it holds stay out of the pool meanwhile. On a network that
  drops connections silently, bound that with libpq's `keepalives_idle` or `tcp_user_timeout`
  in the DSN. The ping is an empty query that carries no session state, so it is safe behind
  pgbouncer in transaction pooling. To skip it, pass `PostgresConfig(check_on_checkout=False)`:
  each dead connection then fails the one operation it is handed to. Routed clients take the
  same `pool_config`.
- **Relations** can be static `(schema, table)` tuples or per-tenant resolvers —
  see [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).
- **Locking reads when `read` differs from `write`** (a view, typically). A read with
  `for_update` locks the matching row on the `write` table, by id and tenant, and then reads
  it from `read`. Postgres refuses `FOR UPDATE` on a view that joins with `LEFT JOIN`, and
  reading after the lock returns the row as locked. A row that stops matching while its lock
  is awaited is skipped for the next match, but stays locked until the transaction ends, as
  in Postgres. A read-only document has no `write` table, so its locking read still goes to
  `read`.
- **Plain `Enum` fields.** A document or ingest column holds the member's value, as for a
  `StrEnum` or `IntEnum`, and a filter names that value. Earlier versions wrote
  `Status.OPEN` to a document column and `OPEN` to an ingest one; rewrite those rows, for
  example `UPDATE orders SET status = 'open' WHERE status = 'Status.OPEN'`.
- **Indexes for sorted pages.** A sort on a `NOT NULL` column, or on the record `id`,
  renders without a null placement, so a plain btree index serves it in either direction,
  through a view too. A nullable column keeps the canonical placement (ascending nulls
  first, descending nulls last), which a plain index does not hold: index it with the
  placement your pages use, for example `(created_at DESC NULLS LAST)`. A view's columns
  other than the `id` all read as nullable.
- **Partial indexes and bound filters.** Filter values are sent as parameters, including the
  `is_deleted = false` and `is_current = true` that the soft-deletion and versioned kits
  add. A generic plan, made without the values, cannot prove a partial index's predicate
  such as `WHERE is_deleted = false`. Under the default `plan_cache_mode`, Postgres picks a
  generic plan for a prepared statement only when it estimates no costlier than planning with
  the values, and one that loses such an index usually costs more: in a 100,000-row test the
  index was used on every execution. With `plan_cache_mode = force_generic_plan` the index is
  never used, so keep the default where such indexes matter, or index without the predicate.
- **PGroonga indexes.** Index the searched columns as one column, `USING pgroonga (title)`,
  or as an array, `USING pgroonga ((ARRAY[title, content]))`; each element is a column,
  optionally wrapped in casts or `COALESCE(col, '')`. The search matches against the
  index's own expression, so the planner can use the index. What matches, and in what
  order, is then the index's own tokenizer and normalizer: with the default ones, `python`
  matches the word `python` and not `pythonic`. An index with any other kind of element,
  such as a transform like `lower(title)`, is refused when the search first runs.
- **PGroonga and text outside ASCII.** The default normalizer folds case for ASCII only: on
  PGroonga 4.0.6, `плата` does not match `Плата`, while `plata` matches `PLATA`. For
  Cyrillic or other non-ASCII text, declare a Unicode-aware normalizer on the index, and
  the search, which follows the index's own options, matches regardless of case:

    ```sql
    CREATE INDEX items_title_search ON items
      USING pgroonga (title) WITH (normalizers = 'NormalizerNFKC150');
    ```

- **PGroonga and `ё`.** No NFKC normalizer treats `ё` as `е`, so `елка` does not match
  `Ёлка`. A `NormalizerTable` after the NFKC normalizer, reading a one-row mapping, does:

    ```sql
    CREATE TABLE public.yo_map (target text, normalized text);
    CREATE INDEX yo_map_index ON public.yo_map
      USING pgroonga (target pgroonga_text_term_search_ops_v2) INCLUDE (normalized);
    INSERT INTO public.yo_map VALUES ('ё', 'е');

    CREATE INDEX items_title_search ON items USING pgroonga (title) WITH (normalizers =
      'NormalizerNFKC150, NormalizerTable("normalized", "${table:public.yo_map_index}.normalized", "target", "target")');
    ```

    Name the mapping index with the schema its table is in: unqualified, `CREATE INDEX` fails
    with `relation "yo_map_index" does not exist`. A row is normalized when it is indexed, so fill
    the mapping before building the search index and `REINDEX` it after changing the mapping;
    until then, a row indexed earlier with `ё` matches neither spelling.

- **PGroonga and VACUUM.** On PGroonga 4.0.6 with Groonga 16.0.1 and PostgreSQL 18.1, a
  PGroonga index built while a `VACUUM` or `ANALYZE` (autovacuum included) runs on another
  PGroonga-indexed table can break: searches through it return no rows, or fail with
  `object isn't found: <Sources…>`, until you `REINDEX` it. Build PGroonga indexes when no
  vacuum is running, or reindex them afterwards
  ([pgroonga/pgroonga#1013](https://github.com/pgroonga/pgroonga/issues/1013)).
- **Vector search from stored vectors.** `VectorEngine(column=..., dimensions=...)` with no
  `embeddings_name` resolves no embeddings provider, so none has to be registered. It ranks by
  a caller's `query_vector` (cached embeddings, or vectors from another pipeline) and browses a
  blank query; a text query is refused as a `precondition` error with code
  `search_query_vector_required`, before any SQL runs. Hub and federated searches take no
  `query_vector`, so their vector legs still require `embeddings_name`.

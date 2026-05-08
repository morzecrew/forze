---
title: Specs and infrastructure wiring
summary: How kernel specs relate to integration configs and dependency routes
---

## What problem this solves

A logical aggregate name needs to map cleanly to physical infrastructure such as tables, collections, key prefixes, and buckets.

## When you need this

Use this when a spec and an integration config need to agree on routes and names.


Kernel **specs** (`DocumentSpec`, `SearchSpec`, `CacheSpec`, …) describe *what* the application works with: model types, logical `name`, optional cache/history flags. They stay free of database URLs and table names.

Integration packages register **infrastructure configs** on dependency modules. Those configs map each logical `name` to concrete relations, Redis key namespaces, S3 buckets, and so on. At runtime, `ExecutionContext` combines a spec with the matching routed adapter.

## Same `name` everywhere

Use the **same string** for:

- `DocumentSpec(name=..., ...)` / `SearchSpec(name=..., ...)` / `CacheSpec(name=..., ...)`
- The corresponding entry in `PostgresDepsModule.rw_documents`, `MongoDepsModule.rw_documents`, `RedisDepsModule.caches`, …

`ExecutionContext` resolves `DocumentQueryDepKey` / `DocumentCommandDepKey` (and other keys) **by route** `spec.name`, then passes the spec into the factory so adapters can read model types and flags.

## DocumentSpec (kernel)

`DocumentSpec` carries **only** domain-facing fields:

| Field | Purpose |
|-------|---------|
| `name` | Logical route id; must match infra config keys |
| `read` | Read model type (`ReadDocument` subclass) |
| `write` | `{"domain", "create_cmd", "update_cmd"}` or `None` for read-only |
| `history_enabled` | Whether revision history is active |
| `cache` | Optional `CacheSpec` — if set, document factories resolve a cache port while building query/command adapters |

It does **not** embed SQL table names or Mongo collections. Those belong in `PostgresDocumentConfig`, `MongoDocumentConfig`, etc.

## Postgres wiring

`PostgresDepsModule` takes a `PostgresClient` plus routed maps:

- `rw_documents: dict[str, PostgresDocumentConfig]` — read/write documents
- `ro_documents: dict[str, PostgresReadOnlyDocumentConfig]` — read-only
- `searches: dict[str, PostgresSearchConfig]` — full-text search per `SearchSpec.name`
- `tx: set[str]` — transaction manager routes for `ctx.txmanager(route)`

Each `PostgresDocumentConfig` supplies `(schema, table)` tuples for `read`, `write`, optional `history`, `bookkeeping_strategy` (`"database"` \| `"application"`), and optional `batch_size`.

## Mongo wiring

`MongoDepsModule` uses `rw_documents` / `ro_documents` with `(database, collection)` tuples in `MongoDocumentConfig` / `MongoReadOnlyDocumentConfig`, plus optional `tx` routes.

## Redis wiring

`RedisDepsModule` registers **routed** cache, counter, and idempotency factories from dicts keyed by logical name:

- `caches: dict[str, RedisCacheConfig]` — each config has a `namespace` string for key prefixing
- `counters`, `idempotency` — same pattern

`CacheSpec(name="projects", ...)` must match the key used in `caches` so `ctx.cache(spec)` resolves the right Redis adapter.

## Resolution sketch

    :::python
    # Kernel: models + logical name
    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

    # Infra: same "projects" key maps to tables / Redis namespace
    pg = PostgresDepsModule(
        client=pg_client,
        rw_documents={
            "projects": {
                "read": ("public", "projects"),
                "write": ("public", "projects"),
                "bookkeeping_strategy": "database",
                "history": ("public", "projects_history"),
            },
        },
    )
    redis_mod = RedisDepsModule(
        client=redis_client,
        caches={"projects": {"namespace": "app:projects"}},
    )

Inside the app you only pass `project_spec`; adapters receive both the spec and the infra config that was registered under `project_spec.name`.

## Related

- [Aggregate specification](aggregate-specification.md) — field-by-field spec reference
- [Contracts and adapters](contracts-adapters.md) — ports and dep keys
- Integration guides: [PostgreSQL](../integrations/postgres.md), [Redis](../integrations/redis.md), [MongoDB](../integrations/mongo.md)

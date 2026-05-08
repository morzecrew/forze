# Document contracts

Document contracts are the primary persistence abstraction for aggregate read
models and write commands. They are split into query and command ports so a
usecase can depend only on the side it needs.

## `DocumentSpec[R, D, C, U]`

| Section | Details |
|---------|---------|
| Purpose | Describes one logical document resource and the model types used to read or write it. |
| Import path | `from forze.application.contracts.document import DocumentSpec` |
| Type parameters | `R` read model, `D` domain document, `C` create command, `U` update DTO. |
| Required fields | `name`, `read`; `write` is required for command ports and may be `None` for read-only resources. |
| Returned values | The spec itself is passed to `ctx.doc_query(spec)` or `ctx.doc_command(spec)` to build ports. |
| Common implementations | Mock document adapter, Postgres document adapters, Mongo document adapters. |
| Related dependency keys | `DocumentQueryDepKey`, `DocumentCommandDepKey`; optional `CacheDepKey` when `cache` is set. |
| Minimal example | See below. |
| Related pages | [Contracts overview](../contracts.md), [PostgreSQL](../../integrations/postgres.md), [MongoDB](../../integrations/mongo.md), [Query Syntax](../query-syntax.md). |

Required fields:

| Field | Type | Notes |
|-------|------|-------|
| `name` | `str | StrEnum` | Logical route used to select infrastructure configuration. |
| `read` | `type[R]` | Pydantic read model for returned documents. |
| `write` | `DocumentWriteTypes[D, C, U] | None` | Domain/write model mapping, or `None` for read-only resources. |
| `history_enabled` | `bool` | Enables history when supported by the adapter. |
| `cache` | `CacheSpec | None` | Resolves a cache port while building document ports. |

Helper methods:

| Method | Returns |
|--------|---------|
| `supports_soft_delete()` | `bool` based on whether the domain type supports soft deletion. |
| `supports_update()` | `bool` based on whether the update command has writable fields. |

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec

    project_spec = DocumentSpec(
        name="projects",
        read=ProjectRead,
        write={
            "domain": Project,
            "create_cmd": CreateProject,
            "update_cmd": UpdateProject,
        },
        history_enabled=True,
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

## `DocumentWriteTypes[D, C, U]`

| Section | Details |
|---------|---------|
| Purpose | Groups the write-side model classes required by `DocumentSpec.write`. |
| Import path | `from forze.application.contracts.document import DocumentWriteTypes` |
| Type parameters | `D` domain document, `C` create command, `U` update DTO. |
| Required fields | `domain`, `create_cmd`, `update_cmd`. |
| Returned values | Not returned directly; consumed by adapters and coordinators. |
| Common implementations | Plain `dict` literals typed as `DocumentWriteTypes`. |
| Related dependency keys | Used indirectly by `DocumentCommandDepKey`. |
| Minimal example | `write={"domain": Project, "create_cmd": CreateProject, "update_cmd": UpdateProject}` |
| Related pages | [Domain Models](../domain-models.md). |

## `DocumentQueryPort[R]`

| Section | Details |
|---------|---------|
| Purpose | Reads documents by primary key, filters, sorting, and pagination. |
| Import path | `from forze.application.contracts.document import DocumentQueryPort` |
| Type parameters | `R`, the read model returned when no projection is requested. |
| Required methods | `get`, `get_many`, `find`, `find_many`, `count`. |
| Returned values | `R`, `Sequence[R]`, `Page[R]`, `CountlessPage[R]`, `int`, or JSON projections when `return_fields` is used. |
| Common implementations | Mock, Postgres, Mongo document query adapters. |
| Related dependency keys | `DocumentQueryDepKey`; resolve with `ctx.doc_query(spec)`. |
| Minimal example | `project = await ctx.doc_query(project_spec).get(project_id)` |
| Related pages | [Query Syntax](../query-syntax.md), [Cache contracts](cache.md). |

Required methods:

| Method | Parameters | Returns |
|--------|------------|---------|
| `get` | `pk`, optional `for_update`, `return_fields`, `skip_cache` | One model or JSON projection. |
| `get_many` | `pks`, optional `return_fields`, `skip_cache` | Sequence of models or projections. |
| `find` | `filters`, optional `for_update`, `return_fields` | One model/projection or `None`. |
| `find_many` | Optional `filters`, `pagination`, `sorts`, `return_count`, `return_fields`, `return_type` | Page-like result of models, projections, or aggregates. |
| `count` | Optional `filters` | Matching row count. |

## `DocumentCommandPort[R, D, C, U]`

| Section | Details |
|---------|---------|
| Purpose | Creates, updates, touches, soft-deletes, restores, hard-deletes, and ensures documents. |
| Import path | `from forze.application.contracts.document import DocumentCommandPort` |
| Type parameters | `R` read model, `D` domain document, `C` create command, `U` update DTO. |
| Required methods | `create`, `create_many`, `update`, `update_many`, `update_matching`, `touch`, `kill`, `delete`, `restore`, `ensure`, and batch variants. |
| Returned values | Usually the updated/read model `R`, sequences of `R`, counts for matching updates, or `None` for kill operations. |
| Common implementations | Mock, Postgres, Mongo document command adapters. |
| Related dependency keys | `DocumentCommandDepKey`; resolve with `ctx.doc_command(spec)`. |
| Minimal example | `created = await ctx.doc_command(project_spec).create(cmd)` |
| Related pages | [Domain Models](../domain-models.md), [Execution](../execution.md). |

Revision-bearing methods such as `update`, `delete`, and `restore` require the
expected `rev` to prevent lost updates. Batch methods accept sequences of tuples
containing the primary key, revision, and command payload where applicable.

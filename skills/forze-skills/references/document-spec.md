# Document and search specs

The logical `DocumentSpec` and `SearchSpec` that name a resource without naming a table, plus schema alignment and the DTOs composition needs. Physical backends are mapped in [spec to backend config](spec-to-backend-config.md).

## DocumentSpec (logical)

`DocumentSpec` binds **model types** and logical `name`. It does **not** embed SQL `source` strings or Mongo collection names — those live on `PostgresDocumentConfig`, `MongoDocumentConfig`, etc., keyed by the same `name`.

```python
from datetime import timedelta
from enum import StrEnum

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec


class ResourceName(StrEnum):
    PROJECTS = "projects"


project_spec = DocumentSpec(
    name=ResourceName.PROJECTS,
    read=ProjectReadModel,
    write={
        "domain": Project,
        "create_cmd": CreateProjectCmd,
        "update_cmd": UpdateProjectCmd,
    },
    history_enabled=True,
    cache=CacheSpec(name=ResourceName.PROJECTS, ttl=timedelta(minutes=5)),
)
```

Once a `DepsRegistry` registers document adapters for that `name`, handlers obtain **`DocumentQueryPort`** / **`DocumentCommandPort`** via **`ctx.document.query(project_spec)`** / **`ctx.document.command(project_spec)`** — see [execution context](execution-context.md) and [Document contracts](https://morzecrew.github.io/forze/latest/reference/contracts/document/).

| Field | Purpose |
|-------|---------|
| `name` | Logical route id (`str | StrEnum`); must match infra config keys |
| `read` | Read model type |
| `write` | `domain`, `create_cmd`, optional `update_cmd`; omit / shape for read-only |
| `history_enabled` | Adapter may persist revision history when infra provides it |
| `cache` | Optional `CacheSpec` for read-through caching |
| `encryption` | Optional `FieldEncryption` policy (fields sealed at rest) — see [field encryption](field-encryption.md) |

Use `spec.supports_update()` when branching composition logic on whether the spec declares an update command.

Once the four models and the spec exist, a **governed** aggregate (soft delete, search sync, invariants, outbox) is one `AggregateKit(spec=...)` declaration away — the kit composes the wiring, never the models. See [AggregateKit](aggregate-kit.md).

## SearchSpec (logical)

Search is separate from `DocumentSpec`:

```python
from forze.application.contracts.search import SearchSpec

project_search_spec = SearchSpec(
    name=ResourceName.PROJECTS,
    model_type=ProjectReadModel,
    fields=("title", "description"),
    default_weights={"title": 0.6, "description": 0.4},
)
```

Postgres index and heap layout are configured per integration (`PostgresSearchConfig` under the same `name`).

## Database schema alignment

Column names must match Pydantic field names. Core columns: `id`, `rev`, `created_at`, `last_update_at`. Add mixin fields (`is_deleted`, `number_id`, `creator_id`) and domain fields as needed.

The following illustrates a typical Postgres table; **table placement is infrastructure**, not part of `DocumentSpec`:

```sql
CREATE TABLE public.projects (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    rev              integer     NOT NULL DEFAULT 1,
    created_at       timestamptz NOT NULL DEFAULT now(),
    last_update_at   timestamptz NOT NULL DEFAULT now(),
    is_deleted       boolean     NOT NULL DEFAULT false,
    title            text        NOT NULL,
    description      text        NOT NULL
);
```

## DocumentDTOs for composition

When using `build_document_registry` and FastAPI routers:

```python
from forze_kits.aggregates.document import DocumentDTOs

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)
```

## Anti-patterns

- **Putting table/collection/index names in `DocumentSpec` or `SearchSpec`** — use deps-module configs.
- **Putting physical `source` / table names on `DocumentSpec`** — keep specs logical-only; wire tables in deps modules.

## Reference

- [Document contracts](https://morzecrew.github.io/forze/latest/reference/contracts/document/)

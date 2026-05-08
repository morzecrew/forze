---
name: forze-domain-aggregates
description: >-
  Defines Forze document aggregates (Document, commands, ReadDocument),
  mixins, validators, kernel DocumentSpec and SearchSpec. Use when modeling
  entities, DTOs, StrEnum-backed DocumentSpec, SearchSpec, CacheSpec, or
  aligning schemas with Forze domain and application contracts.
---

# Forze Domain Aggregates

Use when defining domain models, document aggregates, and **kernel** specifications. Physical tables, collections, Redis namespaces, buckets, and queues belong in integration configs — see [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md) and [`pages/docs/core-concepts/specs-and-wiring.md`](../../pages/docs/core-concepts/specs-and-wiring.md).

Pair with [`forze-framework-usage`](../forze-framework-usage/SKILL.md) for ports and [`forze-wiring`](../forze-wiring/SKILL.md) for composition and HTTP.

## Document aggregate structure

Every document aggregate typically defines four model types:

| Type | Base class | Purpose |
|------|------------|---------|
| **Domain model** | `Document` | Entity with business logic, validation |
| **Create command** | `CreateDocumentCmd` | Input for creation |
| **Update command** | `BaseDTO` | Partial update payload |
| **Read model** | `ReadDocument` | Frozen projection for queries |

```python
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

class Project(SoftDeletionMixin, Document):
    title: str
    description: str = ""

class CreateProjectCmd(CreateDocumentCmd):
    title: str
    description: str

class UpdateProjectCmd(BaseDTO):
    title: str | None = None
    description: str | None = None

class ProjectReadModel(ReadDocument):
    title: str
    description: str
    is_deleted: bool = False
```

## Document base fields

`Document` provides: `id`, `rev`, `created_at`, `last_update_at`. `CreateDocumentCmd` optionally accepts `id` and `created_at` for imports. `ReadDocument` carries the same core fields.

## Mixins

| Mixin | Adds | Use when |
|-------|------|----------|
| `SoftDeletionMixin` | `is_deleted` | Soft-delete support |
| `NumberMixin` | `number_id` | Human-readable IDs (combine with `NumberIdStep` in mapping) |
| `CreatorMixin` | `creator_id` | Audit (`CreatorIdStep`) |
| `NameMixin` | `name`, `display_name`, … | Named entities |

```python
from forze.domain.mixins import NumberCreateCmdMixin, NumberMixin, SoftDeletionMixin
from forze.domain.models import CreateDocumentCmd, Document

class Ticket(NumberMixin, SoftDeletionMixin, Document):
    title: str

class CreateTicketCmd(NumberCreateCmdMixin, CreateDocumentCmd):
    title: str
```

## Update validators

Enforce rules during `Document.update()`:

```python
from forze.domain.validation import update_validator
from forze.base.errors import ValidationError

class Project(Document):
    status: str = "draft"

    @update_validator(fields={"status"})
    def _validate_transition(before, after, diff):
        allowed = {"draft": {"active"}, "active": {"archived"}}
        if after.status not in allowed.get(before.status, set()):
            raise ValidationError("Invalid status transition.")
```

## DocumentSpec (kernel)

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

| Field | Purpose |
|-------|---------|
| `name` | Logical route id (`str | StrEnum`); must match infra config keys |
| `read` | Read model type |
| `write` | `domain`, `create_cmd`, optional `update_cmd`; omit / shape for read-only |
| `history_enabled` | Adapter may persist revision history when infra provides it |
| `cache` | Optional `CacheSpec` for read-through caching |

Use `spec.supports_soft_delete()`, `supports_update()`, `supports_number_id()` when branching composition logic.

## SearchSpec (kernel)

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
from forze.application.composition.document import DocumentDTOs

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)
```

## Anti-patterns

1. **Domain importing ports or adapters** — domain stays pure.
2. **Update command with required fields** — use optional fields with `None` defaults for partial patches.
3. **Mutable defaults on read models** — `ReadDocument` is frozen.
4. **Putting physical `source` / table names on `DocumentSpec`** — keep specs kernel-only; wire tables in deps modules.
5. **Scattering literal spec names** — put resource names in a shared `StrEnum` and reuse it in specs and deps modules.

## Reference

- [`pages/docs/core-concepts/aggregate-specification.md`](../../pages/docs/core-concepts/aggregate-specification.md)
- [`pages/docs/core-concepts/specs-and-wiring.md`](../../pages/docs/core-concepts/specs-and-wiring.md)

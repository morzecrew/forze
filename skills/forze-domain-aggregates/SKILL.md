---
name: forze-domain-aggregates
description: Define domain models, document aggregates, and specifications for Forze. Apply when the user asks to create entities, models, specs, or DTOs.
---

# Forze Domain Aggregates

Use this skill when defining domain models, document aggregates, and specifications. Focus on **usage** — creating aggregates that work with Forze's document and composition layers.

## Document aggregate structure

Every document aggregate needs four model types:

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

`Document` provides: `id`, `rev`, `created_at`, `last_update_at`. `CreateDocumentCmd` optionally accepts `id` and `created_at` for imports. `ReadDocument` includes the same core fields.

## Mixins

| Mixin | Adds | Use when |
|-------|------|----------|
| `SoftDeletionMixin` | `is_deleted` | Soft-delete support; blocks updates when deleted |
| `NumberMixin` | `number_id` | Human-readable IDs (use `NumberIdStep` in mapper) |
| `CreatorMixin` | `creator_id` | Audit trail (use `CreatorIdStep` in mapper) |
| `NameMixin` | `name`, `display_name`, etc. | Named entities |

```python
class Ticket(NumberMixin, SoftDeletionMixin, Document):
    title: str

class CreateTicketCmd(NumberCreateCmdMixin, CreateDocumentCmd):
    title: str
```

## Update validators

Enforce business rules during `Document.update()`:

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

## DocumentSpec

Binds the aggregate to storage, cache, and history. Adapters read it — you declare once:

```python
from datetime import timedelta
from forze.application.contracts.document import DocumentSpec

project_spec = DocumentSpec(
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
    cache={"enabled": True, "ttl": timedelta(minutes=5)},
)
```

| Field | Purpose |
|-------|---------|
| `namespace` | Cache key prefix, logical name |
| `read` | Source relation + read model |
| `write` | Source + domain, create_cmd, update_cmd |
| `history` | Optional audit trail source |
| `cache` | Optional cache (enabled, ttl) |

## SearchSpec

For full-text search, define separately:

```python
from forze.application.contracts.search import SearchSpec

project_search_spec = SearchSpec(
    namespace="projects",
    model=ProjectReadModel,
    indexes={
        "idx_title": {
            "source": "public.projects",
            "fields": [{"path": "title"}],
        },
    },
    default_index="idx_title",
)
```

## Database schema alignment

Column names must match Pydantic field names. Core fields: `id`, `rev`, `created_at`, `last_update_at`. Add mixin fields (`is_deleted`, `number_id`, `creator_id`) and domain fields.

```sql
CREATE TABLE public.projects (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    rev         integer     NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL DEFAULT now(),
    last_update_at timestamptz NOT NULL DEFAULT now(),
    is_deleted  boolean     NOT NULL DEFAULT false,
    title       text        NOT NULL,
    description text        NOT NULL
);
```

## DocumentDTOs for composition

When using `build_document_registry` and FastAPI router:

```python
from forze.application.composition.document import DocumentDTOs

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)
```

## Anti-patterns

1. **Domain model importing ports or adapters** — domain is pure.
2. **Update command with required fields** — use `None` defaults for partial updates.
3. **Read model with mutable defaults** — use `frozen=True` semantics (ReadDocument is frozen).
4. **Spec with wrong source** — `source` must match actual table/collection name.

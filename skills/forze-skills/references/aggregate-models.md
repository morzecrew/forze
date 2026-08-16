# Aggregate models

The four models every document aggregate declares, the base fields it inherits, the mixins worth knowing, and where update validation belongs. Pure domain: nothing here imports a port or an adapter. The logical specs that name them live in [document and search specs](document-spec.md).

## Document aggregate structure

Every document aggregate typically defines four model types:

| Type | Base class | Purpose |
|------|------------|---------|
| **Domain model** | `Document` | Entity with business logic, validation |
| **Create command** | `CreateDocumentCmd` | Input for creation |
| **Update command** | `BaseDTO` | Partial update payload |
| **Read model** | `ReadDocument` | Frozen projection for queries |

```python
from forze_kits.domain.soft_deletion import SoftDeletionMixin
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
| `NumberIdMixin` | `number_id` | Human-readable IDs (combine with `NumberIdMappingStep` in mapping) |
| `CreatorIdMixin` (`forze_kits.domain.creator_id`) | `creator_id` | Audit (`CreatorIdMappingStep`) |
| `MetadataMixin` (`forze_kits.domain.metadata`) | `name`, `display_name`, … | Named entities |

```python
from forze_kits.domain.number_id import NumberIdCreateCmdMixin, NumberIdMixin
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze.domain.models import CreateDocumentCmd, Document

class Ticket(NumberIdMixin, SoftDeletionMixin, Document):
    title: str

class CreateTicketCmd(NumberIdCreateCmdMixin, CreateDocumentCmd):
    title: str
```

## Update validators

Enforce rules during `Document.update()`:

```python
from forze.domain.validation import update_validator
from forze.base.exceptions import exc

class Project(Document):
    status: str = "draft"

    @update_validator(fields={"status"})
    def _validate_transition(before, after, diff):
        allowed = {"draft": {"active"}, "active": {"archived"}}
        if after.status not in allowed.get(before.status, set()):
            raise exc.validation("Invalid status transition.")
```

## Anti-patterns

- **Domain importing ports or adapters** — domain stays pure.
- **Update command with required fields** — use optional fields with `None` defaults for partial patches.
- **Mutable defaults on read models** — `ReadDocument` is frozen.

## Reference

- [Aggregate specification](https://morzecrew.github.io/forze/latest/core-concepts/application-layer/)

---
name: forze-domain-modeling
description: Create and work with Forze domain models including Document, CoreModel, BaseDTO, mixins, and update validators. Use when defining new entities, value objects, or domain logic.
---

# Forze Domain Modeling

## Base Models

### CoreModel

All domain models inherit from `CoreModel`, which extends Pydantic's `BaseModel` with framework defaults:

```python
from forze.domain.models import CoreModel

class MyValueObject(CoreModel):
    name: str
    value: int
```

`CoreModel` configuration:
- `use_attribute_docstrings=True` — trailing docstrings on fields become field descriptions
- `str_strip_whitespace=True` — strings are automatically stripped
- `json_encoders={set: sorted}` — sets serialize deterministically

### BaseDTO

Frozen (immutable) model for data transfer objects:

```python
from forze.domain.models import BaseDTO

class MyDTO(BaseDTO):
    name: str
    value: int
```

`BaseDTO` inherits `CoreModel` with `frozen=True`.

## Document

`Document` is the core aggregate root with built-in identity, revision tracking, timestamps, and update validation:

```python
from forze.domain.models import Document

class MyDocument(Document):
    name: str
    value: int = 0
```

### Built-in Fields

| Field | Type | Default | Frozen |
|-------|------|---------|--------|
| `id` | `UUID` | `uuid7()` | Yes |
| `rev` | `int` | `1` | Yes |
| `created_at` | `datetime` | `utcnow()` | Yes |
| `last_update_at` | `datetime` | `utcnow()` | No |

### Updating Documents

`Document.update(data)` applies a validated merge-patch and returns `(new_doc, diff)`:

```python
doc = MyDocument(name="old", value=1)
updated, diff = doc.update({"name": "new"})
# updated.name == "new", updated.rev == 2
# diff == {"name": "new", "last_update_at": ..., "rev": 2}
```

`Document.touch()` bumps only `last_update_at`:

```python
touched, diff = doc.touch()
```

### Update Validators

Decorate methods with `@update_validator` to enforce invariants during updates:

```python
from forze.domain.models import Document
from forze.domain.validation import update_validator

class MyDocument(Document):
    name: str
    status: str = "draft"

    @update_validator
    def _check_not_archived(self, after: "MyDocument", diff: dict) -> None:
        if self.status == "archived":
            raise ValidationError("Cannot update an archived document.")

    @update_validator(fields=["status"])
    def _validate_status_transition(self, after: "MyDocument", diff: dict) -> None:
        allowed = {"draft": {"active"}, "active": {"archived"}}
        if after.status not in allowed.get(self.status, set()):
            raise ValidationError(f"Invalid transition: {self.status} -> {after.status}")
```

Validators support 1–3 parameters: `(before)`, `(before, after)`, or `(before, after, diff)`.

Use `fields=` to restrict the validator to run only when specific fields change.

Validators run even when the diff is empty (allows enforcing state-based constraints).

## Read Models and Commands

### CreateDocumentCmd

Base for document creation commands:

```python
from forze.domain.models import CreateDocumentCmd

class CreateMyDocumentCmd(CreateDocumentCmd):
    name: str
    value: int = 0
```

`CreateDocumentCmd` includes optional `id` and `created_at` fields for caller-supplied values.

### ReadDocument

Base for read projections:

```python
from forze.domain.models import ReadDocument

class MyReadDocument(ReadDocument):
    name: str
    value: int
```

`ReadDocument` is a frozen DTO with `id`, `rev`, `created_at`, and `last_update_at`.

### DocumentHistory

Generic history entry for tracking document versions:

```python
from forze.domain.models import DocumentHistory

MyDocumentHistory = DocumentHistory[MyDocument]
```

Contains `source`, `id`, `rev`, `created_at`, and `data` (the full document snapshot).

## Mixins

### SoftDeletionMixin

Adds `is_deleted: bool = False` and prevents updates on soft-deleted documents:

```python
from forze.domain.mixins import SoftDeletionMixin

class MyDocument(SoftDeletionMixin, Document):
    name: str
```

### CreatorMixin

Adds `created_by: str` for tracking the creator:

```python
from forze.domain.mixins import CreatorMixin

class MyDocument(CreatorMixin, Document):
    name: str
```

### NameMixin

Adds `name: str`:

```python
from forze.domain.mixins import NameMixin

class MyDocument(NameMixin, Document):
    value: int
```

### NumberMixin

Adds numeric helper fields:

```python
from forze.domain.mixins import NumberMixin

class MyDocument(NumberMixin, Document):
    name: str
```

## Combining Mixins

Mixins can be composed freely:

```python
class MyDocument(SoftDeletionMixin, CreatorMixin, NameMixin, Document):
    value: int = 0
```

## Primitives

### UUID

Forze uses UUIDv7 (time-ordered) for document identifiers:

```python
from forze.base.primitives.uuid import uuid7
new_id = uuid7()
```

### Timestamps

```python
from forze.base.primitives.datetime import utcnow
now = utcnow()
```

## Checklist

When creating a new domain model:

1. Choose the base: `Document` (aggregate root), `CoreModel` (value object), or `BaseDTO` (immutable DTO)
2. Apply relevant mixins (`SoftDeletionMixin`, `CreatorMixin`, `NameMixin`, `NumberMixin`)
3. Add `@update_validator` methods for business invariants
4. Create corresponding `CreateDocumentCmd` and `ReadDocument` subclasses
5. Place models in `src/forze/domain/models/`
6. Place mixins in `src/forze/domain/mixins/`
7. Place validators in `src/forze/domain/validation/`

# Domain Models

The domain layer (`forze.domain`) holds business logic and data structures. It has no knowledge of databases, HTTP, or external services. For the conceptual overview, see [Domain Layer](../core-concepts/domain-layer.md). This page is the complete API reference.

## Base models

### CoreModel

Base for all domain models. Extends Pydantic's `BaseModel` with:

- Field docstrings enabled for schema generation
- Stripped string fields
- Sorted set serialization for stable JSON output

For example:

    :::python
    from forze.domain.models import CoreModel

    class Settings(CoreModel):
        name: str
        tags: set[str] = set()

### BaseDTO

Extends `CoreModel` with frozen semantics (immutable after creation). Use for command DTOs, update payloads, and read projections:

    :::python
    from forze.domain.models import BaseDTO

    class UpdateProjectCmd(BaseDTO):
        title: str | None = None
        description: str | None = None

## Document

`Document` is the base class for versioned aggregates. Every document carries identity, revision tracking, and timestamps.

    :::python
    from forze.domain.models import Document

    class Project(Document):
        title: str
        description: str = ""

### Built-in fields

| Field | Type | Default | Frozen | Purpose |
|-------|------|---------|--------|---------|
| `id` | `UUID` | `uuid7()` | Yes | Unique identifier |
| `rev` | `int` | `1` | Yes | Revision number (incremented by adapters) |
| `created_at` | `datetime` | `utcnow()` | Yes | Creation timestamp |
| `last_update_at` | `datetime` | `utcnow()` | No | Last modification timestamp |

Frozen fields raise `ValidationError` if an update attempts to change them.

### update()

Apply a validated update and return the new document plus a computed diff:

    :::python
    project = Project(title="Alpha", description="First")
    updated, diff = project.update({"title": "Beta"})
    # updated.title == "Beta"
    # diff == {"title": "Beta", "last_update_at": <timestamp>}

The update flow:

1. **Validate** — reject unknown and frozen fields
2. **Compute diff** — apply the patch to a JSON dump, calculate the minimal merge patch
3. **Bump timestamp** — set `last_update_at` to now
4. **Run validators** — execute registered `@update_validator` hooks
5. **Return** — new immutable copy and the diff

If the patch produces no changes, returns the original instance and an empty dict.

### touch()

Update only `last_update_at` without changing other fields:

    :::python
    touched, diff = project.touch()
    # diff == {"last_update_at": <timestamp>}

### validate_historical_consistency()

Check whether applying a patch to an older state would conflict with the current state:

    :::python
    is_safe = current.validate_historical_consistency(
        old_state, 
        incoming_patch,
    )

Returns `True` when the incoming patch does not touch fields that changed between `old_state` and `current`. Used by adapters reconstructing state from history to prevent conflicting concurrent merges.

## Commands and read models

### CreateDocumentCmd

Base for create commands. Frozen DTO that optionally accepts `id` and `created_at` for imports and migrations:

    :::python
    from forze.domain.models import CreateDocumentCmd

    class CreateProjectCmd(CreateDocumentCmd):
        title: str
        description: str

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `id` | `UUID | None` | `None` | Override the auto-generated ID |
| `created_at` | `datetime \| None` | `None` | Override the auto-generated timestamp |

### ReadDocument

Base for read projections returned by ports. Includes the standard document fields:

    :::python
    from forze.domain.models import ReadDocument

    class ProjectRead(ReadDocument):
        title: str
        description: str

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `UUID` | Document identifier |
| `rev` | `int` | Revision number |
| `created_at` | `datetime` | Creation timestamp |
| `last_update_at` | `datetime` | Last modification timestamp |

### DocumentHistory

Stores a snapshot of a document at a given revision:

    :::python
    from forze.domain.models import DocumentHistory

    history_entry: DocumentHistory[Project]

| Field | Type | Purpose |
|-------|------|---------|
| `source` | `str` | Source table or collection |
| `id` | `UUID` | Document identifier |
| `rev` | `int` | Revision at the time of snapshot |
| `created_at` | `datetime` | When the history entry was created |
| `data` | `D` | Full document snapshot |

## Update validators

Validators enforce business rules during `Document.update()`. Decorate methods with `@update_validator` to register them:

    :::python
    from forze.domain.validation import update_validator
    from forze.base.errors import ValidationError

    class Project(Document):
        title: str
        status: str = "draft"

        @update_validator
        def _block_published(before, after, diff):
            if before.status == "published" and "title" in diff:
                raise ValidationError("Cannot change title after publishing.")

### Validator signatures

The decorator normalizes three function signatures:

| Parameters | When to use |
|------------|-------------|
| `(before)` | Only need the state before the update |
| `(before, after)` | Need both before and after states |
| `(before, after, diff)` | Need the diff dict as well |

### Field-scoped validators

Restrict a validator to specific fields with the `fields` parameter. The validator only runs when the diff touches at least one of the listed fields:

    :::python
    @update_validator(fields={"status"})
    def _validate_transition(before, after, diff):
        allowed = {"draft": {"active"}, "active": {"archived"}}
        if after.status not in allowed.get(before.status, set()):
            raise ValidationError("Invalid status transition.")

### Validator collection

Validators are collected from the full class hierarchy at class definition time via `collect_update_validators()`. Conflict resolution when a subclass redefines a validator name:

| `on_conflict` | Behavior |
|---------------|----------|
| `"warn"` (default) | Emit a `RuntimeWarning` and use the subclass version |
| `"error"` | Raise `TypeError` |
| `"overwrite"` | Silently use the subclass version |

Override the default by setting `_update_validators_on_conflict` on the class:

    :::python
    class StrictProject(Document):
        _update_validators_on_conflict = "error"

## Mixins

Reusable domain concerns composed via multiple inheritance. Each mixin adds a focused capability.

### SoftDeletionMixin

Adds an `is_deleted` boolean and a validator that blocks updates to soft-deleted documents (except toggling the flag itself):

    :::python
    from forze.domain.mixins import SoftDeletionMixin

    class Project(SoftDeletionMixin, Document):
        title: str

Once `is_deleted` is `True`, updating any field other than `is_deleted` raises `ValidationError`.

| Field | Type | Default |
|-------|------|---------|
| `is_deleted` | `bool` | `False` |

### NameMixin

Adds a required `name` and optional `display_name`, `short_name`, and `description`:

    :::python
    from forze.domain.mixins import (
        NameMixin,
        NameCreateCmdMixin,
        NameUpdateCmdMixin,
    )

    class Workspace(NameMixin, Document):
        pass

    class CreateWorkspaceCmd(NameCreateCmdMixin, CreateDocumentCmd):
        pass

    class UpdateWorkspaceCmd(NameUpdateCmdMixin, BaseDTO):
        pass

| Field | Type | Required in model | Required in create | Required in update |
|-------|------|------|------|------|
| `name` | `String` | Yes | Yes | No (optional) |
| `display_name` | `String | None` | No | No | No |
| `short_name` | `String | None` | No | No | No |
| `description` | `LongString | None` | No | No | No |

### NumberMixin

Adds a required positive integer `number_id` for human-readable identification. Typically populated by a counter adapter during the create mapping step:

    :::python
    from forze.domain.mixins import NumberMixin, NumberCreateCmdMixin

    class Ticket(NumberMixin, Document):
        title: str

    class CreateTicketCmd(NumberCreateCmdMixin, CreateDocumentCmd):
        title: str

| Field | Type | Required |
|-------|------|----------|
| `number_id` | `PositiveInt` | Yes in model and create cmd, optional in update |

### CreatorMixin

Adds a frozen `creator_id` field (UUID). Typically injected by a mapping step that reads the current actor context:

    :::python
    from forze.domain.mixins import CreatorMixin, CreatorCreateCmdMixin

    class Comment(CreatorMixin, Document):
        body: str

    class CreateCommentCmd(CreatorCreateCmdMixin, CreateDocumentCmd):
        body: str

| Field | Type | Frozen |
|-------|------|--------|
| `creator_id` | `UUID` | Yes |

## Domain constants

String constants used across layers for consistent field naming:

    :::python
    from forze.domain.constants import ID_FIELD, REV_FIELD, SOFT_DELETE_FIELD

| Constant | Value | Purpose |
|----------|-------|---------|
| `ID_FIELD` | `"id"` | Document identifier field |
| `REV_FIELD` | `"rev"` | Revision field |
| `SOFT_DELETE_FIELD` | `"is_deleted"` | Soft deletion flag |
| `NUMBER_ID_FIELD` | `"number_id"` | Human-readable number |
| `CREATOR_ID_FIELD` | `"creator_id"` | Creator reference |
| `HISTORY_SOURCE_FIELD` | `"source"` | History source field |
| `HISTORY_DATA_FIELD` | `"data"` | History data field |
| `TENANT_ID_FIELD` | `"tenant_id"` | Tenant identifier |

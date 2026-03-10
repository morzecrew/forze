# Domain Layer

The domain layer holds **business logic and invariants**. It knows nothing about databases, HTTP, or external services. All domain code is pure: it operates on data structures and enforces rules without side effects.

## Key ideas

Every aggregate in Forze is built from a small family of types:

- **Document**: a versioned entity with identity, revision tracking, and timestamps. This is your aggregate root.
- **Commands**: frozen DTOs that carry intent across layer boundaries (`CreateDocumentCmd` for creation, `BaseDTO` for updates).
- **Read models**: frozen projections of the document state, used for query results (`ReadDocument`).
- **Mixins**: reusable concerns (soft deletion, human-readable IDs, creator tracking) composed via multiple inheritance.
- **Validators**: hooks that run during updates to enforce business rules.

The domain layer is the most stable part of the system. Changing a database engine or web framework never requires changes here.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/domain-models.svg" alt="Domain model family">
  <img class="d2-dark" src="../../assets/diagrams/dark/domain-models.svg" alt="Domain model family">
</div>

## Base models

Forze provides two base classes for all domain types:

- **`CoreModel`** is the base for all domain models. It configures Pydantic with field docstrings for schema generation, sorted set serialization, and stripped string fields.
- **`BaseDTO`** extends `CoreModel` with frozen-by-default semantics. Use it for command DTOs, update payloads, and read projections where immutability is desired.

## Document model

`Document` is the base class for versioned aggregates. It provides identity, revision tracking, timestamps, and a structured update mechanism.

    :::python
    from forze.domain.models import Document


    class Project(Document):
        title: str
        description: str

Every `Document` includes these built-in fields:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `id` | `UUID` | `uuid7()` | Unique identifier (frozen after creation) |
| `rev` | `int` | `1` | Revision number (frozen, incremented by adapters) |
| `created_at` | `datetime` | `utcnow()` | Creation timestamp (frozen) |
| `last_update_at` | `datetime` | `utcnow()` | Last modification timestamp |

Fields marked as frozen raise `ValidationError` if an update attempts to change them.

## Update semantics

Documents support structured, validated updates. The `update()` method applies a patch and returns the new state plus a computed diff:

    :::python
    project = Project(title="Alpha", description="First project")

    updated, diff = project.update({"title": "Beta"})

    # updated.title == "Beta"
    # updated.last_update_at > project.last_update_at
    # diff == {"title": "Beta", "last_update_at": <new timestamp>}

The update flow:

1. **Validate**: reject unknown fields and frozen fields
2. **Compute diff**: apply the patch to a JSON dump and calculate the minimal merge patch
3. **Bump timestamp**: set `last_update_at` to now
4. **Run validators**: execute registered `@update_validator` hooks
5. **Return**: produce a new immutable copy and the diff

If the patch produces no changes, `update()` returns the original instance and an empty diff.

The `touch()` method updates only `last_update_at` without changing any other fields:

    :::python
    touched, diff = project.touch()
    # diff == {"last_update_at": <new timestamp>}

## Historical consistency

The `validate_historical_consistency()` method checks whether a concurrent update would conflict with the current document state. This is used by adapters that reconstruct state from history:

    :::python
    is_safe = current.validate_historical_consistency(old_state, incoming_patch)

It returns `True` when the incoming patch does not touch the same fields that changed between `old_state` and `current`.

## Commands and read models

Commands and read models are frozen DTOs that travel across layer boundaries:

    :::python
    from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument


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

| Type | Purpose |
|------|---------|
| `CreateDocumentCmd` | Base for create commands. Optionally accepts `id` and `created_at` for imports/migrations. |
| `BaseDTO` | Base for update commands. All fields should be optional to allow partial updates. |
| `ReadDocument` | Base for read models. Includes `id`, `rev`, `created_at`, `last_update_at`. |
| `DocumentHistory[D]` | Stores a snapshot of a document at a given revision. |

## Update validators

Validators enforce business rules during `Document.update()`. They have access to the before state, after state, and the diff:

    :::python
    from forze.domain.validation import update_validator
    from forze.base.errors import ValidationError


    class Project(Document):
        title: str
        status: str = "draft"

        @update_validator
        def _no_title_change_when_published(before, after, diff):
            if before.status == "published" and "title" in diff:
                raise ValidationError(
                    "Cannot change title of a published project."
                )

Validators are collected from the class hierarchy at class definition time. They run only when the diff touches relevant fields. Multiple validators compose: they all run on every matching update.

You can restrict a validator to specific fields using the `fields` parameter:

    :::python
    @update_validator(fields={"status"})
    def _validate_status_transition(before, after, diff):
        allowed = {"draft": {"active"}, "active": {"archived"}}
        if after.status not in allowed.get(before.status, set()):
            raise ValidationError("Invalid status transition.")

## Mixins

Reusable domain concerns are composed via mixins. Each mixin adds a focused capability without deep inheritance chains.

### SoftDeletionMixin

Adds an `is_deleted` boolean field and an update validator that blocks updates to soft-deleted documents (except toggling the deletion flag itself):

    :::python
    from forze.domain.mixins import SoftDeletionMixin


    class Project(SoftDeletionMixin, Document):
        title: str

Once `is_deleted` is `True`, any update that modifies fields other than `is_deleted` raises `ValidationError`.

### NameMixin

Adds `name` (required), `display_name`, `short_name`, and `description` (all optional). Companion mixins `NameCreateCmdMixin` and `NameUpdateCmdMixin` mirror the fields for command DTOs:

    :::python
    from forze.domain.mixins import NameMixin, NameCreateCmdMixin, NameUpdateCmdMixin


    class Workspace(NameMixin, Document):
        pass


    class CreateWorkspaceCmd(NameCreateCmdMixin, CreateDocumentCmd):
        pass


    class UpdateWorkspaceCmd(NameUpdateCmdMixin, BaseDTO):
        pass

### NumberMixin

Adds a required `number_id` field (positive integer) for human-readable identifiers. Typically populated by a counter adapter during the create mapping step:

    :::python
    from forze.domain.mixins import NumberMixin, NumberCreateCmdMixin


    class Ticket(NumberMixin, Document):
        title: str


    class CreateTicketCmd(NumberCreateCmdMixin, CreateDocumentCmd):
        title: str

### CreatorMixin

Adds a frozen `creator_id` field (UUID). Typically injected by a mapping step that reads the current actor context:

    :::python
    from forze.domain.mixins import CreatorMixin, CreatorCreateCmdMixin


    class Comment(CreatorMixin, Document):
        body: str


    class CreateCommentCmd(CreatorCreateCmdMixin, CreateDocumentCmd):
        body: str

## Domain constants

Field name constants used across layers for consistent serialization:

| Constant | Value | Purpose |
|----------|-------|---------|
| `ID_FIELD` | `"id"` | Document identifier |
| `REV_FIELD` | `"rev"` | Revision number |
| `SOFT_DELETE_FIELD` | `"is_deleted"` | Soft deletion flag |
| `NUMBER_ID_FIELD` | `"number_id"` | Human-readable number |
| `CREATOR_ID_FIELD` | `"creator_id"` | Creator reference |

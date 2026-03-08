# Domain Layer

The domain layer holds **business logic and invariants**. It knows nothing about databases, HTTP, or external services. All domain code is pure: it operates on data structures and enforces rules without side effects.

## Versioned Entities

Aggregates in Forze are **versioned**: each entity has revisions, timestamps, and identity. Updates produce new revisions instead of in-place mutation.

    :::python
    from forze.domain.models import Document

    class Project(Document):
        title: str

| Aspect | Behavior |
|--------|----------|
| **Revisions** | Each update increments a revision; history is preserved |
| **Timestamps** | Creation and modification times are tracked |
| **Identity** | Each aggregate has a stable identifier (typically UUID) |
| **Optimistic concurrency** | Revision checks prevent lost updates |
| **Audit trails** | History relations store prior states |

## Value Objects and Commands

Domain inputs and outputs are **immutable** value objects and commands:

| Concept | Purpose |
|---------|---------|
| **Value objects** | Immutable data structures representing domain concepts |
| **Commands** | Create and update DTOs passed to operations |
| **Read models** | Projections for queries; may differ from domain models |
| **CQRS** | Read models evolve independently from write models |

Commands are validated before they reach the domain. Read models are optimized for query patterns and may denormalize or join data that the domain model keeps separate.

## Pluggable Validation

Validation runs on update with access to:

- **Previous state** — the aggregate before the change
- **New state** — the result of applying the patch
- **Patch** — the update command or delta

Invariants stay close to the model. Validation hooks are composable and can be extended without modifying core domain logic.

    :::python
    project = Project(title="Initial")
    updated, diff = project.update({"title": "Updated"})

## Mixins

Reusable concerns are composed via **mixins** instead of deep inheritance:

| Mixin | Purpose |
|-------|---------|
| **Soft delete** | Mark entities as deleted without physical removal |
| **Naming** | Name/slug fields and validation |
| **Numbering** | Sequence numbers or human-readable IDs |
| **Creator** | Track who created or last modified an entity |

Mixins are composed without deep inheritance chains. Each mixin adds a focused capability.

## Why It Matters

- **Business rules live in one place** — the domain is the single source of truth
- **Domain is testable in isolation** — no mocks for databases or HTTP
- **Infrastructure changes do not ripple** — swap storage without touching business logic

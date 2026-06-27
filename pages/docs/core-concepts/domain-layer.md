---
title: Domain
icon: lucide/box
summary: Aggregates, commands, and read models — the business entities you model first
---

The domain layer is the **stable center** of a Forze service. It holds your
business entities and the rules that govern them, in plain Python — no database
drivers, no HTTP, no adapters. Change the database engine or the web framework
and this layer doesn't move.

!!! abstract "The rule"

    Domain code imports from no other layer — only Pydantic models, dataclasses,
    and standard Python. If it needs a database to run, it doesn't belong here.

## The aggregate and its family

You model a business entity as an **aggregate**. Around it sits a small family of
frozen, purpose-built types that carry data across boundaries.

![The aggregate family: commands and mixins feed the aggregate; the aggregate projects to a read model](../_diagrams/light/domain-models.svg#only-light){ data-src="../_diagrams/light/domain-models.svg#only-light" }
![The aggregate family: commands and mixins feed the aggregate; the aggregate projects to a read model](../_diagrams/dark/domain-models.svg#only-dark){ data-src="../_diagrams/dark/domain-models.svg#only-dark" }

| Type | Role | Base class |
|------|------|-----------|
| **Aggregate** | The entity with identity, versioning, and rules | `Document` |
| **Create command** | Frozen input that creates one | `BaseDTO` |
| **Update command** | Frozen partial-update payload (all fields optional) | `BaseDTO` |
| **Read model** | Frozen projection returned from queries | `ReadDocument` |

How a create command becomes the domain model and is projected back to the read model —
the codecs that carry it — is the [mapping reference](../reference/mapping.md).

!!! note "CreateDocumentCmd is deprecated"

    `CreateDocumentCmd` still works as an alias for `BaseDTO` but is deprecated.
    Use `BaseDTO` for new create commands.

You build an aggregate by subclassing `Document`, which carries four built-in
fields so you don't redefine identity and versioning every time:

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `UUID` | Identity, assigned once (frozen) |
| `rev` | `int` | Revision, bumped on each write (frozen) |
| `created_at` | `datetime` | Creation time (frozen) |
| `last_update_at` | `datetime` | Last write time |

!!! note "Aggregates that emit events"

    Compose `AggregateRoot` alongside `Document` —
    `class Order(Document, AggregateRoot)` — to add an in-process event buffer.
    Behaviour methods record `DomainEvent`s that the application layer drains and
    dispatches after the operation commits.

## Rules live with the data

The payoff of a domain layer is that **invariants are enforced where the data
lives**, not scattered across handlers. Two mechanisms attach rules to an
aggregate:

- **`@invariant`** — a check enforced on create and after every update.
- **`@update_validator`** — a rule that runs only when an update touches
  relevant fields, with the before state, after state, and the diff in hand.

```python
from forze.domain.models import Document
from forze.domain.validation import update_validator
from forze.base.exceptions import exc


class Order(Document):
    customer: str
    total: int
    status: str = "pending"

    @update_validator(fields={"total"})
    def _total_is_final_once_shipped(before, after, diff):
        if before.status == "shipped":
            raise exc.domain("A shipped order's total is final.")
```

Updates are structured. `order.update({"total": 99})` returns a **new immutable
instance** and a minimal diff, runs the validators, and bumps `last_update_at`.
A patch that changes nothing returns the original and an empty diff.

These validate a *single* write. A state **transition** — its guard plus the change
it makes — belongs on the aggregate too, as a *decider* method that returns the patch
to persist; see [Aggregate decisions](../writing-operation/aggregate-decisions.md).

## Reusable concerns: mixins

Common domain concerns ship as composable mixins in `forze_kits` (included in the
default install). Each adds one focused capability — no deep inheritance chains.

| Mixin | Adds |
|-------|------|
| **Soft deletion** | An `is_deleted` flag plus a validator that blocks edits to deleted records |
| **Metadata** | `name`, `display_name`, and `description` with normalized string types |
| **Number id** | A human-readable `number_id`, populated by a counter on create |
| **Creator id** | A frozen `creator_id`, injected from the current actor context |

Aggregates define *what* your domain is and the rules it keeps; turning actions
on them into something the runtime can execute is the
[application layer](application-layer.md).

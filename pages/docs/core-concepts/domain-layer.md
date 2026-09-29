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
the codecs that carry it — is the [mapping reference](../reference/mapping.md). Why these
are *separate* models, rather than one with the rest derived, is
[Why four models](why-four-models.md).

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

## A period, and which end is in force

A span of time is four decisions — is the end included, can it be open-ended, what does overlap
mean where two spans touch, and what is a zero-length span — and a codebase that leaves them to
prose answers them differently in two modules. `Period` carries the answers in the value:

```python
from datetime import date
from forze.base.primitives import Period

quarter = Period(start=date(2026, 1, 1), end=date(2026, 4, 1))         # bounds="[)"
contract = Period(start=date(2026, 1, 1), end=date(2026, 3, 31), bounds="[]")
current = Period(start=date(2026, 1, 1))                                # open-ended
```

| Bounds | Reads as | Use it for |
|--------|----------|------------|
| `"[)"` *(default)* | start included, end excluded | anything that must tile — consecutive periods cover a timeline with no gap and no overlap |
| `"[]"` | both included | periods a person wrote: "valid through 31 March" |
| `"(]"` · `"()"` | start excluded | available for completeness; an excluded *start* on a `date` grain is the shape to avoid when a database constraint has to agree |

`contains(at)`, `overlaps(other)` and `intersects(start, end)` read the convention from the
value, so a shared endpoint is counted only when both sides have it in force: half-open
`[Jan, Feb)` and `[Feb, Mar)` do not overlap, and the inclusive pair does. An open end reaches
every point its own convention admits and no earlier one, so an open-ended period that excludes
its start still does not meet a point at that start. A zero-length period is accepted — a booking
cancelled in the instant it was made is a real row — and is empty unless both ends are included.

Endpoints must be comparable, and the type system cannot say so: `datetime` subclasses `date`, so
a type checker passes a `date`/`datetime` pair, and a naive and an aware `datetime` are one type
to it. `Period` checks both at construction — along with the endpoints being dates at all, since a
period is as often built from dynamic input as from typed code — rather than letting any of them
surface as a `TypeError` from inside a comparison later.

## Wall clocks and instants

A wall-clock time and a zone name one instant on almost every day of the year. On the two days a
zone changes offset they do not: a fall-back time happens **twice**, and a spring-forward time
**never**. The civil-time helpers refuse both rather than pick an hour, and take the zone as a
value you inject — `CivilZone("Europe/Berlin")`, validated where you declare it — never a module
constant:

```python
from datetime import date, datetime
from forze.base.primitives import CivilZone, elapsed_minutes, local_day_bounds, to_instant

berlin = CivilZone("Europe/Berlin")

to_instant(berlin, datetime(2026, 10, 25, 2, 30))          # refused: dst_ambiguous
to_instant(berlin, datetime(2026, 10, 25, 2, 30), fold=0)  # the first 02:30, summer time
local_day_bounds(berlin, date(2026, 10, 25))               # a 25-hour Period, bounds="[)"
```

| Function | Returns | Refuses |
|----------|---------|---------|
| `to_instant(zone, local, fold=None)` | the UTC instant a naive wall time names | `dst_ambiguous` without `fold`, `dst_nonexistent` always |
| `local_day_bounds(zone, day)` · `month_bounds(zone, year, month)` | a half-open `Period` of instants | a month outside 1–12 |
| `spanned_local_days(zone, start, end)` | the local days whose bounds `[start, end)` meets | `naive_datetime`, a range ending before it starts |
| `elapsed_minutes(start, end)` | whole minutes between two instants | `naive_datetime` |

A day whose midnight the zone skips starts at the first instant that exists, and a day whose
midnight it repeats starts at the first one: the day bounds resolve where `to_instant` refuses,
because a calendar with holes or overlaps is worse. A day the zone skips
entirely (Samoa's 30 December 2011) is an empty `Period`, and `spanned_local_days` never lists it.
Days tile: where a zone repeats the hours around a midnight, the repeat belongs to the new day,
so a range split by `spanned_local_days` and `local_day_bounds` keeps all its time, once. Durations are
computed on instants only: two wall-clock times across a transition are off by the shift, and the
wrong answer is a plausible number. `AwareDatetime` is for your boundary models: a naive value is
a validation error (`naive_datetime`) naming the field. Bind `fold` where the UI knows which
occurrence the user meant; otherwise expect a `dst_ambiguous` on one autumn night a year.

These sit beside two other time-zone paths the framework already has — the analytics
time-bucketing and the durable scheduler's cron — each shaped for its own plane. Use these for
converting domain wall-clock values.

Aggregates define *what* your domain is and the rules it keeps; turning actions
on them into something the runtime can execute is the
[application layer](application-layer.md).

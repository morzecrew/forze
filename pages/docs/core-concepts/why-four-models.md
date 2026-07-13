---
title: Why four models
icon: lucide/layers
summary: An entity is declared as a small family — domain, create, update, read. The fields overlap, but the four are distinct contracts, and the overlap is the price of two things Forze keeps at once.
---

Modeling an entity in Forze means declaring a small family of Pydantic models — the
domain model, a create command, an update command, and a read model — then wiring them
into a `DocumentSpec`. The fields overlap, so the family can read like boilerplate.

It isn't. The four are four *contracts*, and the overlap that remains is the price of two
guarantees Forze keeps **at the same time**: a domain decoupled from storage, and full
static typing. Give up either one and the family collapses to a single model — which is
exactly the trade other frameworks make.

## Four contracts, not four copies

The fields repeat, but the *shapes* differ, because each model answers a different question:

| Model | Question it answers | Why it diverges |
|-------|---------------------|-----------------|
| **Domain** (`Document`) | What is the entity, and what rules hold? | Carries identity, revision, and invariants. |
| **Create command** (`BaseDTO`) | What may a caller send to make one? | Omits server-set fields, applies defaults, validates inbound input on its own terms. |
| **Update command** (`BaseDTO`) | What may a caller change? | A **merge-patch**: every field optional, where omitting one means *"leave it alone"* — a different type from the domain. |
| **Read model** (`ReadDocument`) | What does a query return? | May add computed fields, hide secrets, rename, or project a subset. |

A slice of one entity makes the divergence concrete — the same `status` field has a
different shape in three of the four:

```python
class Order(Document):           # domain: the real state + its rules
    customer: str
    status: str = "pending"

class OrderCreate(BaseDTO):      # inbound: status is server-set, so it is absent
    customer: str

class OrderUpdate(BaseDTO):      # merge-patch: omit a field to leave it alone
    customer: str | None = None
    status: str | None = None

class OrderRead(ReadDocument):   # outbound: the projection a query returns
    customer: str
    status: str
```

Collapse these into one model and you lose the ability to let them **diverge** — and the
divergence is the point. The update command is intentionally *not* the domain model.

## Why not derive the repetitive ones?

Even granting they are distinct, much of the overlap is mechanical — the update command is
"the domain with every field made optional." So why not generate it? Two reasons, each tied
to a guarantee Forze keeps.

**Deriving at runtime erases static types.** The only way to build one model from another at
runtime is Pydantic's `create_model(...)`. It works when the program runs — but a
type-checker reads your code *without* running it, and to mypy or pyright `create_model(...)`
returns a model with no known fields. You would lose autocomplete and type-checking on the
very models you touch most. Forze is strict-typed end to end, so that is disqualifying.

!!! note "Why the type-checker can't rescue this"

    TypeScript can write `Partial<Order>` — "`Order` with every field optional" — and the
    compiler computes the derived type, keeping full field-level typing. Python's type
    system has no equivalent: there is no way to express *"this type, but all fields
    optional"* or *"this type, projected to these fields."* So even a hand-rolled helper
    can't be typed honestly — it either erases the fields or lies about them. The shape has
    to be **written out** to be seen.

## The same trade-off, everywhere

This is not a Forze quirk. To shrink the family, a framework has to give up one of the two
guarantees:

| Approach | Models per entity | What it gives up |
|----------|-------------------|------------------|
| Fused ORM model (SQLModel, Django, Beanie) | ~1 | The model **is** the storage row — domain coupled to persistence. |
| Dynamically typed (Ecto / Elixir) | 1 + changeset | No static type-checker to satisfy. |
| Decoupled **and** typed (SQLAlchemy 2.0, EF Core, **Forze**) | up to 4 | Nothing — it writes the family out. |

The frameworks that collapse to one model buy it by fusing the domain to the database —
precisely what hexagonal architecture, and Forze, refuse. The ones that keep a clean domain
*and* static types write the same family Forze does.

## When you can reduce it

The family is a floor, not a ceiling — a few things genuinely trim it:

- **Skip ops you don't have.** A read-only aggregate needs no update (or create) command;
  omit them. `build_document_registry(spec)` derives its DTO mapping from the spec, so you
  never re-list the models when wiring a registry.
- **Single-source field rules.** Reuse a field's type and validation across the family with
  an `Annotated` alias — `Money = Annotated[int, Field(ge=0)]` — so a constraint lives in
  one place even though each model still names the field.
- **Generate, don't abstract.** If the typing is tedious at scale, an editor snippet or a
  scaffold that *emits the four classes* keeps types perfect: it does the field-transform
  when you write the code — the one moment Python can.

The four models aren't ceremony left un-abstracted. They're the shape two commitments — a
decoupled domain and full static typing — leave behind. For the lifecycle and rules that
live *on* the domain model, see [the domain layer](domain-layer.md) and
[aggregate decisions](../writing-operation/aggregate-decisions.md); for why the update
command is a merge-patch under a revision, see
[concurrency conflicts](../writing-operation/concurrency-conflicts.md).

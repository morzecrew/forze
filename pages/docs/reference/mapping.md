---
title: Mapping & codecs
icon: lucide/arrow-left-right
summary: The model types, how they convert, and the codecs that serialize them
---

You send a create command and get back a read model; an event is staged as one
type and decoded as another. Two small layers handle all of it — and both default
to **field-name matching**, so the happy path needs no hand-written conversion.

## The model types

From `forze.domain.models`:

| Type | Role | Provides |
|------|------|----------|
| `Document` | the persisted domain model | `id` (uuid7), `rev`, `created_at`, `last_update_at` — with defaults; carries `update()` + invariants |
| `AggregateRoot` | mix in for event-emitting aggregates | a pending-events buffer + `@event_emitter` |
| `BaseDTO` | frozen input/output projection | nothing — you declare the fields |
| `ReadDocument` | read-model base | `id`, `rev`, `created_at`, `last_update_at` — **required** (filled from the row) |
| `DomainEvent` | frozen event base | `event_id` (uuid7), `occurred_at` |

A spec's write side is declared with **`DocumentWriteTypes`** (a `TypedDict` from
`forze.application.contracts.document`): `domain`, `create_cmd`, and an optional
`update_cmd`.

!!! note "`CreateDocumentCmd` is deprecated"

    It's an empty alias of `BaseDTO`. Declare create payloads as plain `BaseDTO`;
    identity isn't carried in the payload — `create(payload, id=…)` /
    `ensure(id, …)` take it explicitly.

```python
class Order(Document):          # domain: + id, rev, created_at, last_update_at
    customer_id: str
    total_cents: int

class CreateOrder(BaseDTO):     # create command — frozen, no id
    customer_id: str
    total_cents: int

class OrderRead(ReadDocument):  # read model — inherits the four metadata fields
    customer_id: str
    total_cents: int
```

## Codecs

A **codec** serializes a model to/from a mapping (and JSON bytes). Two ship:

```python
from forze.base.serialization import PydanticModelCodec, MsgspecModelCodec

PydanticModelCodec(OrderPlaced)   # for pydantic BaseModel
MsgspecModelCodec(OrderStruct)    # for msgspec Struct
```

Both take the model type positionally. `default_model_codec(model_type)`
auto-selects by base class — which is what specs use when you don't pass one.

A codec is **required** wherever a payload crosses a wire and the framework can't
infer the type from a domain model:

| Where | Field |
|-------|-------|
| Outbox | `OutboxSpec(codec=PydanticModelCodec(Payload))` |
| Queue / stream / pub-sub | `QueueSpec(codec=…)` |
| Idempotency | the wrap's `result_type` (a pydantic/msgspec model) |

Document, search, and analytics specs **derive** their codecs from the model
types automatically; override with `DocumentSpec(codecs=…)` only if you need to.

## How a write maps

A create flows through **two conversions**:

1. **DTO → command** *(handler layer, `forze_kits`)* — the public request DTO
   (`DocumentDTOs.create`) is mapped to the spec's `create_cmd`. The default is a
   field-name-matching mapper; override per-operation via `DocumentMappers`.
2. **command → domain → read** *(codec layer, in the port)* — the command is
   `transform`ed into the domain model (which stamps `id`/`rev`/timestamps),
   persisted, and the stored row is decoded back into the read model.

```python
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry

dtos = DocumentDTOs(read=OrderRead, create=CreateOrder, update=UpdateOrder)
registry = build_document_registry(order_spec, dtos)   # default mappers
```

Both layers use Pydantic `transform` (dump-then-validate), so conversion is **by
matching field names** — never positional. Supply a `DocumentMappers(create=…)`
hook only when the boundary DTO and the command genuinely differ in shape.

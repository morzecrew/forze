# Domain events

Tactical DDD building blocks: aggregates raise **domain events**, an in-process
dispatcher runs handlers within the operation's transaction, and a bridge maps a domain
event to an integration event staged in the transactional [outbox](outbox.md).

## Domain primitives

Both live in `forze.domain.models`.

| Type | Purpose |
|------|---------|
| `DomainEvent` | Frozen value object recording something that happened (`event_id`, `occurred_at`; subclasses add the aggregate id and payload). |
| `AggregateRoot` | Mixin collecting events in a transient, non-persisted buffer: `record_event(event)`, `collect_events()`, `has_pending_events`. |
| `event_emitter` | Decorator declaring which event an update transition raises (mirrors `update_validator`). |

Compose `AggregateRoot` with [`Document`](document.md) for a persisted aggregate. The
aggregate stays pure — it only records events; the application layer drains and dispatches
them. `AggregateRoot` overrides `model_copy` so events stay independent across
`Document.update` copies (no aliasing or double-dispatch).

**Declarative (preferred):** an `@event_emitter` is a pure
`(before, after, diff) -> DomainEvent | None` collected like `update_validator` and run on
`Document.update` — no bespoke behavior method that re-does update logic. It must be declared
on an `AggregateRoot` (enforced at class creation); optional `fields=` restricts it to updates
touching those fields.

    :::python
    from forze.domain.models import AggregateRoot, Document, DomainEvent, event_emitter

    class OrderConfirmed(DomainEvent):
        aggregate_id: UUID

    class Order(Document, AggregateRoot):
        status: str = "pending"

        @event_emitter(fields={"status"})
        def _on_confirm(before, after, diff) -> DomainEvent | None:
            if after.status == "confirmed" and before.status != "confirmed":
                return OrderConfirmed(aggregate_id=after.id)
            return None

    order, _ = order.update({"status": "confirmed"})   # auto-records OrderConfirmed

**Imperative (alternative):** call `record_event` directly inside a behavior method when the
event isn't a simple function of the state transition.

## Events flow on persistence (no manual dispatch)

You do **not** drain and dispatch events by hand. The document command flow does it:
persisting an aggregate (`ctx.document.command(spec).create/update/...`) drains the
aggregate's `collect_events()` and dispatches them **in the operation's transaction** via
the registered dispatcher — so an aggregate's `@event_emitter` reactions reach their
handlers (and the outbox) atomically with the write. Plain (non-aggregate) documents are
unaffected; an aggregate that emits events without a `DomainEventsDepsModule` registered
raises rather than dropping them.

    :::python
    # status pending -> confirmed fires the @event_emitter; the command flow dispatches it
    await ctx.document.command(order_spec).update(pk, rev, OrderUpdate(status="confirmed"))

`ctx.domain()` resolves the dispatcher directly if you ever need it, but handlers should
not call it — let persistence drive dispatch.

## Functional decider — `AggregateRepository`

For behavior-rich aggregates, `forze_kits.aggregates.AggregateRepository` lets a handler
decide *on the aggregate* while persistence stays command-shaped (merge-patch + OCC):

    :::python
    from forze_kits.aggregates import aggregate_repository

    class Order(Document, AggregateRoot):
        status: str = "pending"

        def confirm(self) -> OrderUpdate:        # decider: pure decision -> patch
            if self.status != "pending":
                raise exc.domain("only pending orders can be confirmed")
            return OrderUpdate(status="confirmed")

    # handler:
    repo  = aggregate_repository(ctx, order_spec)
    order = await repo.load(args.id)             # reconstruct the domain aggregate
    patch = order.confirm()                       # decision + invariants on the aggregate
    return await repo.apply(order, patch)         # command.update -> emitters -> dispatch

The decider is a *pure* method (decision + invariants, no I/O); the `@event_emitter`
reactions turn the resulting diff into events, dispatched by the command flow. `load`
reconstructs the aggregate from its read model, so the read model must carry the domain's
fields. **Invariants** use the existing pre-persist mechanisms — Pydantic `@model_validator`
(create-time) and [`@update_validator`](document.md) (update-time) — both raise before the
write.

## Dispatcher, registry, and the outbox bridge

`forze.application.execution.domain` provides the in-process implementation:

| Type | Purpose |
|------|---------|
| `DomainEventRegistry` | Maps domain-event types to handler **factories** (isinstance-matched — a base-type factory catches subclasses). |
| `InProcessDomainEventDispatcher` | Builds each handler from `ctx` and runs it in registration order, within the current scope. |
| `DomainEventsDepsModule(registry)` | Registers the dispatcher (built per scope from `ctx`). |
| `outbox_event_handler(spec, event_type, to_payload)` | Bridge factory: resolves the outbox port from `ctx`, returns a handler that stages an integration event. |

A registered handler is a **factory** `(ctx) -> (event) -> None`: the factory resolves the
narrow capabilities it needs from `ctx`, and the returned handler is invoked with only the
event — so running handlers never hold the execution context (boundary stays clean).

    :::python
    registry = DomainEventRegistry()
    registry.register(
        OrderConfirmed,
        outbox_event_handler(
            outbox_spec,
            "order.confirmed",
            lambda e: OrderConfirmedPayload(order_id=str(e.aggregate_id)),
        ),
    )

    # A custom factory resolves its port once; the handler closes over only that:
    def audit_factory(ctx):
        store = ctx.document.command(audit_spec)
        async def handler(event: OrderConfirmed) -> None:
            await store.create(dto=audit_dto(event))
        return handler
    registry.register(OrderConfirmed, audit_factory)

Because handlers run in the handler's transaction and the outbox flushes in-transaction,
a bridged integration event flushes atomically with the aggregate write.

## Notes

- Domain events suit **custom handlers** holding rich aggregates; the generic CRUD
  handlers return read models, not aggregates, and have no events to drain.
- Dispatch must run before the operation returns (so it is inside the transaction scope).

Related: [Outbox](outbox.md), [Document](document.md).

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

Compose `AggregateRoot` with [`Document`](document.md) for a persisted aggregate. The
aggregate stays pure — it only records events; the application layer drains and dispatches
them. `AggregateRoot` overrides `model_copy` so events stay independent across
`Document.update` copies (no aliasing or double-dispatch).

    :::python
    from forze.domain.models import AggregateRoot, Document, DomainEvent

    class OrderConfirmed(DomainEvent):
        aggregate_id: UUID

    class Order(Document, AggregateRoot):
        status: str = "pending"

        def confirm(self) -> "Order":
            new, _ = self.update({"status": "confirmed"})
            return new.record_event(OrderConfirmed(aggregate_id=new.id))

## `DomainEventDispatcherPort`

Resolved as `ctx.domain`. Call it from inside the handler (in-transaction) after
persisting the aggregate:

    :::python
    order = order.confirm()
    await self.doc.update(...)                            # persist
    await ctx.domain().dispatch(order.collect_events())   # drain + dispatch in-tx

`ctx.domain()` resolves the dispatcher; `dispatch(events)` runs the registered handlers.

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

---
title: Events & sagas
icon: lucide/radio
summary: Domain events, the transactional outbox, and sagas — turning local writes into reliable cross-system effects
---

A confirmed order should reliably create a shipment — even though the order and
the shipment live in different systems, even if the message is delivered twice,
and even if a step fails partway. Forze builds that reliability from three
pieces, all riding the after-commit deferral from [Transactions](transactions.md):
**domain events**, the **transactional outbox**, and **sagas**.

We'll trace one worked example throughout — the
[end-to-end recipe](../recipes/end-to-end-saga-outbox-inbox.md), a complete
program you can run.

![A state change and its event commit in one transaction, then relay and inbox carry it exactly-once to a shipment](../_diagrams/light/events-flow.svg#only-light){ data-src="../_diagrams/light/events-flow.svg#only-light" }
![A state change and its event commit in one transaction, then relay and inbox carry it exactly-once to a shipment](../_diagrams/dark/events-flow.svg#only-dark){ data-src="../_diagrams/dark/events-flow.svg#only-dark" }

## A state change worth announcing

An aggregate that extends `AggregateRoot` can emit a `DomainEvent` when its state
transitions. The `Order` records `OrderConfirmed` the moment its status becomes
`confirmed` — declared right next to the data it watches:

```python
--8<-- "recipes/order_fulfillment/app.py:order-aggregate"
```

The event is recorded on the instance during `update()`; the application layer
drains and dispatches it when the operation persists. The aggregate doesn't know
what happens to the event next — that's wired separately.

## From event to outbox

A domain event is only reliable if it can't be lost when the transaction commits,
nor sent when it rolls back. The bridge is a handler that turns the event into a
**staged outbox row**:

```python
--8<-- "recipes/order_fulfillment/app.py:outbox-bridge"
```

`outbox_event_handler` maps `OrderConfirmed` onto an `order.confirmed` integration
event with a typed payload. From now on, whenever an `OrderConfirmed` is
dispatched, it's staged to the outbox.

## The transactional outbox

The point of the outbox is that the event row and the state change **commit
together**. The confirm step updates the order and flushes the staged event
inside the same transaction:

```python
--8<-- "recipes/order_fulfillment/app.py:confirm-step"
```

Because both land in one transaction, you never get a confirmed order without its
event, or an event for an order that rolled back. That's the **dual-write
problem** — solved not with a distributed transaction, but with a local one plus
a relay.

A relay then moves staged rows onward. In production that's a worker handing off
to a broker; here it claims the pending rows and passes them along in-process:

```python
--8<-- "recipes/order_fulfillment/app.py:relay"
```

## Exactly-once on the way in: the inbox

Brokers redeliver. The consumer dedupes by event id through the **inbox**, so
handling the same message twice creates one shipment, not two:

```python
--8<-- "recipes/order_fulfillment/app.py:inbox"
```

`process_with_inbox` runs the handler only if that event id hasn't been seen
before; a redelivery returns `False` and does nothing. Combined with the outbox,
that's **exactly-once** effect delivery across a boundary that only offers
at-least-once.

## Sagas: many steps, one outcome

The `confirm` step above doesn't run alone — it's the final step of a checkout
**saga**. Reserving inventory and confirming the order can't share a single
database transaction; they're distinct steps that may each fail. A saga
sequences them with **compensation**: every step carries an undo, and a failure
rolls the completed steps back in reverse.

```python
--8<-- "recipes/order_fulfillment/app.py:saga"
```

- **`reserve`** is *compensatable* — if a later step fails, `_release` runs to
  undo the reservation.
- **`confirm`** is the **pivot** (`SagaStepKind.PIVOT`) — the point of no return.
  A failure *before* it compensates everything prior; once it commits, the saga
  is durable.

## Three behaviours

The same flow handles the cases that make event-driven systems hard:

| Scenario | Outcome |
|----------|---------|
| **Happy path** | Order confirmed, event relayed, exactly one shipment |
| **Redelivery** | Duplicate message skipped by the inbox — still one shipment |
| **Pivot fails** (payment declined) | Inventory released, order stays `pending`, nothing staged, relayed, or shipped |

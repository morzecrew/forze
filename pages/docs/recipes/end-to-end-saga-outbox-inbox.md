---
title: "End-to-end: saga → outbox → inbox"
icon: lucide/workflow
summary: Compose the whole stack — a saga orchestrates two aggregates, a domain event rides the outbox to a consumer, the inbox makes delivery exactly-once
---

The individual pieces — [domain events](../data-events/events-sagas.md),
the [outbox](transactional-outbox.md), sagas, the inbox — each solve one problem.
This recipe puts them together in one runnable program: a checkout that reserves
inventory, confirms an order, and ships it — reliably, across aggregates, with
compensation if a step fails.

The full program lives at `examples/recipes/order_fulfillment/` and runs in-process
on the mock — no Docker. The [Events & sagas](../data-events/events-sagas.md) chapter
explains *why* each piece works; this is the assembled flow.

## The aggregate announces a change

`Order` extends `AggregateRoot`, so it can record a domain event the moment its
state transitions — declared next to the data it watches:

```python
--8<-- "recipes/order_fulfillment/app.py:order-aggregate"
```

## A saga orchestrates the two aggregates

Checkout spans `Inventory` and `Order`. A saga runs the steps with a **pivot**:
everything before the pivot is compensatable, the pivot commits the outcome.
Reserve inventory first; confirming the order is the pivot.

```python
--8<-- "recipes/order_fulfillment/app.py:saga"
```

## The pivot step: change state and stage the event together

Confirming the order trips its event emitter; the application layer dispatches
that event **inside the step's transaction**, where the outbox bridge stages an
integration event. State change and event are one atomic write:

```python
--8<-- "recipes/order_fulfillment/app.py:confirm-step"
```

The bridge that turns the domain event into a staged outbox row is wired once:

```python
--8<-- "recipes/order_fulfillment/app.py:outbox-bridge"
```

## Relay carries it to the consumer

A relay claims staged rows and delivers them downstream — standing in for a broker
plus the outbox relay worker:

```python
--8<-- "recipes/order_fulfillment/app.py:relay"
```

## The inbox makes delivery exactly-once

Relays are at-least-once: the same event can arrive twice. The consumer records
each message id in the **inbox** and skips duplicates, so the `Shipment` is created
once no matter how many times the event is delivered:

```python
--8<-- "recipes/order_fulfillment/app.py:inbox"
```

## Notes

- **Compensation is automatic.** If the pivot fails, the saga runs the
  compensations for completed steps (here: release the reserved inventory) and
  stages nothing downstream.
- **The event commits with the write.** Because the dispatch happens in the step's
  transaction, a published `order.confirmed` always corresponds to a committed
  order — never a phantom event, never a silent loss.
- **Swap the mock for real backends** without touching this logic: a Postgres
  store + outbox, a real broker via `relay_outbox_to_queue`, and the same inbox
  dedup. The orchestration is backend-agnostic.

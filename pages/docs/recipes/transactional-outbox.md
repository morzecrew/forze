---
title: Transactional outbox
icon: lucide/send
summary: Publish integration events reliably — staged with the write, relayed to a broker, never lost
---

You can't atomically write to your database *and* publish to a broker — a crash
between the two loses or duplicates the event. The **outbox** makes it one
write: stage the event in the *same* transaction as the business change, then a
relay moves staged rows to the broker afterwards. The concept is in
[Events & sagas](../in-depth/events-sagas.md); this is the wiring.

The runnable version lives at `examples/recipes/outbox/` and runs on the
in-memory mock — no broker needed.

## The event and its destination

An `OutboxSpec` carries the payload codec and names the queue it relays to — the
destination `route` must equal the `QueueSpec.name`:

```python
--8<-- "recipes/outbox/app.py:event"
```

## Stage it with the write

Inside the business transaction, stage the event and flush — it commits (or rolls
back) together with the write, so a published event always corresponds to a
committed change:

```python
--8<-- "recipes/outbox/app.py:stage"
```

In a real handler you'd attach `outbox_flush_tx_on_success_factory` to the
operation so the flush fires automatically on transaction success, rather than
calling `flush()` by hand.

## Relay to the broker

A relay claims staged rows and publishes them to the queue, returning what it did:

```python
--8<-- "recipes/outbox/app.py:relay"
```

In production the relay runs continuously as a lifecycle step:

```python
from datetime import timedelta
from forze.application.execution import LifecyclePlan
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    outbox_relay_background_lifecycle_step(
        outbox_spec=ORDER_EVENTS,
        queue_spec=ORDERS_QUEUE,        # required for the queue transport
        interval=timedelta(seconds=5),
    ),
)
```

## Notes

- **Store the outbox where you store the data** so the stage shares the
  transaction — `PostgresOutboxConfig(relation=("app", "outbox"))` (from
  `forze_postgres.execution.deps.configs`) or `MongoOutboxConfig`.
- **At-least-once.** The relay can publish a row twice (claim, publish, crash
  before marking). Consumers dedupe with the [inbox](../in-depth/events-sagas.md).
- Permanently-failed rows are retried with `ctx.outbox.query(spec).requeue_failed([id])`.

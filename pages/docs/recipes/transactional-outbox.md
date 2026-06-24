---
title: Transactional outbox
icon: lucide/send
summary: Publish integration events reliably — staged with the write, relayed to a broker, never lost
---

You can't atomically write to your database *and* publish to a broker — a crash
between the two loses or duplicates the event. The **outbox** makes it one
write: stage the event in the *same* transaction as the business change, then a
relay moves staged rows to the broker afterwards. The concept is in
[Events & sagas](../data-events/events-sagas.md); this is the wiring.

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

## Consuming on the other side

`QueueConsumer` is the consumer-side counterpart — it replaces the hand-rolled
`consume → dedupe → ack/nack` loop with the decisions already made correctly.
Per message it: **parks** handler-poison (opt-in `max_deliveries`), runs the
handler exactly-once through the [inbox](../data-events/events-sagas.md)
(`process_with_inbox`, same dedup transaction, correlation rebound from the
envelope headers), **acks** both fresh *and* duplicate deliveries — a
redelivered already-processed message must leave the queue — and **nacks**
handler failures back (`requeue=True`) for redelivery. One message's failure
never kills the consumer.

```python
from datetime import timedelta

from forze_kits.integrations.consumer import QueueConsumer

consumer = QueueConsumer(
    queue="orders",                # the channel the relay published to
    queue_spec=ORDERS_QUEUE,
    handler=handle_order_event,    # async def (message: QueueMessage[OrderEvent]) -> None
    inbox_spec=ORDERS_INBOX,
    tx_route="postgres",           # dedup mark + handler commit together here
)

result = await consumer.run(ctx, timeout=timedelta(seconds=5))  # idle timeout; None = forever
# result.processed / result.duplicates / result.parked / result.failed
```

In production it runs continuously as a lifecycle step — one step per queue
(no in-process concurrency knob; scale out with more steps or processes):

```python
from forze_kits.integrations.consumer import queue_consumer_background_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    queue_consumer_background_lifecycle_step(
        queue="orders",
        queue_spec=ORDERS_QUEUE,
        handler=handle_order_event,
        inbox_spec=ORDERS_INBOX,
        tx_route="postgres",
    ),
)
```

A crash of the consume stream itself (broker connection loss) is logged and
the consume restarts after `restart_backoff` (default 5s); unacked in-flight
messages redeliver and the inbox dedupes them.

Two kinds of poison, two owners:

- **Decode-poison** (payload doesn't fit the codec model) never reaches your
  handler — the queue adapters reject it inside `consume` with
  `nack(requeue=False)` (RabbitMQ DLX, SQS redrive) and keep consuming.
- **Handler-poison** (decodes fine, handler always fails) is parked by the
  runner when `max_deliveries` is set: a message whose `delivery_count`
  *exceeds* it is `nack(requeue=False)`-ed **without running the handler**, so
  the handler gets at most `max_deliveries` attempts.

!!! warning "Parking is opt-in — and needs a delivery count"
    `max_deliveries` defaults to `None`: the broker's own redrive/DLX policy is
    the default safety net, and you should configure one. Parking also relies
    on the backend reporting `QueueMessage.delivery_count` (SQS
    `ApproximateReceiveCount`, RabbitMQ `x-death` approximation, mock exact) —
    when it's `None`, parking never triggers and a poison message keeps
    redelivering until the broker's policy catches it.

Transient blips can also be retried in-process before the message goes back to
the broker: pass `retry_policy="my-policy"` and the runner wraps each process
step (dedup mark + handler, one fresh transaction per attempt) in
`ctx.resilience().run(...)` under that named policy.

## Failures and retries

The relay classifies errors by **where** they arise:

- **Poison** — the payload can't be decoded into the codec model. The row can
  never publish, so it's marked `failed` immediately. Fix the cause, then
  re-drive with `ctx.outbox.query(spec).requeue_failed([id])` (this resets the
  retry counter).
- **Transient** — the broker publish call raised. The row is rescheduled with
  exponential backoff plus jitter (`retry_base_delay * 2**attempts`, capped at
  `retry_max_backoff`) and stays invisible to claims until its `available_at`.
  After `max_attempts` publish attempts it's marked `failed` (terminal).

Defaults: `max_attempts=5`, `retry_base_delay=1s`, `retry_max_backoff=5min` —
kw-only on every relay function and on the lifecycle step. One row's failure
never blocks the rest of the batch.

## Per-aggregate ordering

Stage with an `ordering_key` (typically the aggregate id) and the relay
publishes it as the transport `key` instead of the event id:

```python
await ctx.outbox.command(ORDER_EVENTS).stage(
    "order.shipped", payload, ordering_key=str(order_id),
)
```

On transports that honor `key` for partitioning — SQS FIFO (`MessageGroupId`),
stream partition keys — same-key events deliver in staged (`created_at`) order
on the happy path. Events staged without an `ordering_key` keep
`key=str(event_id)` as before. Either way the event id rides the
`forze_event_id` header, which is what consumers dedupe on.

!!! warning "Ordering is expressible, not guaranteed"
    Delivery is at-least-once and ordering is **not** guaranteed across
    failures/retries: a row rescheduled for retry (or parked as `failed`) does
    **not** stall later rows of the same `ordering_key` — deliberately, so one
    poison event never head-of-line blocks its aggregate. Consumers must
    dedupe on `event_id` (the `forze_event_id` header) and tolerate
    reordering as well as redelivery (dedupe with the
    [inbox](../data-events/events-sagas.md)).

## Table schema

The outbox table is application-owned — you create and migrate it. The full DDL,
indexes, migration steps, and the optional Hybrid Logical Clock ordering column
(for causal claim order across replicas) are in
[Outbox table schema](../reference/outbox-schema.md), for both Postgres and Mongo.

## Notes

- **Store the outbox where you store the data** so the stage shares the
  transaction — `PostgresOutboxConfig(relation=("app", "outbox"))` (from
  `forze_postgres.execution.deps.configs`) or `MongoOutboxConfig`.
- **At-least-once.** The relay can publish a row twice (claim, publish, crash
  before marking). Consumers dedupe with the [inbox](../data-events/events-sagas.md).
- The background lifecycle step drains the whole backlog each tick (batches
  until a short claim, capped at `max_batches_per_tick=100`), then sleeps
  `interval`.

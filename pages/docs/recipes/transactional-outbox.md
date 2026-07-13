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

The relay classifies a failed row by where it arose. A **poison** row (the payload
can't decode) can never publish, so it's marked `failed` immediately — fix the cause
and re-drive with `ctx.outbox.query(spec).requeue_failed([id])`. A **transient**
failure (the publish call raised) is rescheduled with exponential backoff + jitter and
retried, becoming `failed` only after `max_attempts` (default 5). One row's failure
never blocks the rest of the batch.

## Per-aggregate ordering

Stage with an `ordering_key` (typically the aggregate id) and the relay publishes it
as the transport `key` instead of the event id:

```python
await ctx.outbox.command(ORDER_EVENTS).stage(
    "order.shipped", payload, ordering_key=str(order_id),
)
```

On transports that honor `key` for partitioning (SQS FIFO `MessageGroupId`, stream
partition keys), same-key events deliver in staged order on the happy path.

!!! warning "Ordering is expressible, not guaranteed"
    Delivery is at-least-once and ordering is **not** guaranteed across retries — a
    rescheduled or `failed` row deliberately does not stall later rows of the same key,
    so one poison event never head-of-line blocks its aggregate. Consumers dedupe on
    `event_id` and tolerate reordering, via the [inbox](../data-events/events-sagas.md).

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

## Draining on shutdown

By default the relay task is cancelled at shutdown, so rows staged just before the
process goes away stay `pending` until a later process claims them — up to `interval`
later, or until the next deploy. Set `drain_on_shutdown=True` to publish what is still
claimable first:

```python
relay = outbox_relay_background_lifecycle_step(
    outbox_spec=OUTBOX,
    queue_spec=JOBS,
    drain_on_shutdown=True,
    requires=(POSTGRES_CLIENT_CAPABILITY,),   # order the relay ahead of the pool teardown
)
```

The drain burns **exactly one delivery attempt per row** — the same blast radius as one
ordinary tick. It ends the moment a batch reschedules a row, so it can never re-claim
(and re-attempt) what it just parked; a failing destination stops it instead of being
hammered; and it never opens a batch it does not expect to finish inside
`shutdown_drain_timeout` (default 5s). Anything it cannot reach stays `pending` for the
next process — exactly where it would have been without the drain.

Three constraints, two of which are enforced at wiring time:

- **Declare the ordering edge.** The drain touches the database *during* teardown, and
  lifecycle shutdown runs in reverse wave order — without `requires` (a capability) or
  `depends_on` (a step id) tying the relay to whatever owns its client, the pool can close
  underneath it. Enabling the drain without one is rejected.
- **`pubsub` is rejected.** It is at-most-once past the broker, so publishing while
  subscribers are going away turns a delayed delivery into a lost one. Leaving the rows
  pending is strictly safer there.
- **Keep `shutdown_drain_timeout` under the runtime's `shutdown_step_timeout`** (default
  10s). On expiry the runtime cancels and abandons the hook, and a batch cut between claim
  and mark leaves rows `processing` until `reclaim_stale_after` elapses — consider
  lowering that when draining.

The drain is a best-effort teardown courtesy, not a delivery guarantee: correctness still
rests on the relay claiming those rows eventually, from this process or the next.

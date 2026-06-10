---
name: forze-messaging-streaming
description: >-
  Uses Forze queue, pub/sub, and stream contracts with QueueSpec, PubSubSpec,
  StreamSpec, SQSDepsModule, RabbitMQDepsModule, Redis adapters, and Mock
  adapters. Use when producing, consuming, or testing async messages/events.
---

# Forze messaging and streaming

Use when adding asynchronous messages, event publication, or stream processing. For background-job design, pair with general Python async/job guidance; this skill covers Forze contracts and wiring.

## Queue contracts

`QueueSpec.name` is the logical route. Resolve query/command ports with `ctx.deps.resolve_configurable`.

```python
from datetime import timedelta
from enum import StrEnum

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.base.serialization import PydanticModelCodec


class ResourceName(StrEnum):
    ORDERS = "orders"


class OrderPayload(BaseModel):
    order_id: str
    customer_id: str


order_queue = QueueSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)

writer = ctx.deps.resolve_configurable(
    ctx, QueueCommandDepKey, order_queue, route=order_queue.name
)
await writer.enqueue("orders", OrderPayload(order_id="o-1", customer_id="c-1"))
await writer.enqueue(
    "reminders",
    ReminderPayload(user_id="u-1"),
    delay=timedelta(minutes=10),
)

reader = ctx.deps.resolve_configurable(
    ctx, QueueQueryDepKey, order_queue, route=order_queue.name
)
messages = await reader.receive("orders", limit=10)
await reader.ack("orders", [msg.id for msg in messages])
```

## SQS and RabbitMQ wiring

Both integrations register `QueueQueryDepKey` and `QueueCommandDepKey` through routed maps.

```python
sqs_module = SQSDepsModule(
    client=sqs_client,
    queue_readers={ResourceName.ORDERS: {"namespace": "app"}},
    queue_writers={ResourceName.ORDERS: {"namespace": "app"}},
)

rabbit_module = RabbitMQDepsModule(
    client=rabbit_client,
    queue_readers={ResourceName.ORDERS: {"namespace": "app"}},
    queue_writers={ResourceName.ORDERS: {"namespace": "app"}},
)
```

SQS supports long polling and FIFO `key` values. RabbitMQ uses durable queues, persistent messages, manual ack/nack, and publisher confirms by default.

## Pub/sub contracts

Use pub/sub for broadcast-style events where subscribers receive messages by topic.

```python
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec

events = PubSubSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)
publisher = ctx.deps.resolve_configurable(
    ctx, PubSubCommandDepKey, events, route=events.name
)
await publisher.publish("orders.created", payload, type="order.created")
```

`MockDepsModule` registers pub/sub factories. Redis pub/sub adapters exist, but `RedisDepsModule` does not currently expose pub/sub maps; register them through a custom deps module if needed.

## Stream contracts

Streams model append-only logs and consumer-group reads.

```python
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec

stream_spec = StreamSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)
stream = ctx.deps.resolve_configurable(
    ctx, StreamCommandDepKey, stream_spec, route=stream_spec.name
)
entry_id = await stream.append("orders", payload, type="order.created")
```

Use `StreamQueryDepKey` for `read` / `tail`, and `StreamGroupQueryDepKey` for consumer-group reads and acknowledgements. `MockDepsModule` registers stream factories. For Redis streams or pub/sub, `RedisDepsModule` does not register stream/pubsub maps yet — use [`forze-custom-deps`](../forze-custom-deps/SKILL.md) or `MockDepsModule`.

## Processing rules

- Ack only after the business operation succeeds.
- Use `nack(..., requeue=True)` for transient failures and `requeue=False` when the message should move toward DLQ/provider handling.
- Prefer idempotent consumers; message brokers can redeliver.
- Wrap document mutations and enqueue/outbox-style side effects with transactions and `defer_after_commit` when duplicate or premature events would hurt.

## Anti-patterns

1. **Resolving queue ports without `route=spec.name`** when using SQS/RabbitMQ routed modules.
2. **Using queue names as spec names by accident** — spec names route deps; queue/topic/stream names are provider-level names.
3. **Acknowledging before processing succeeds** — failures become data loss.
4. **Assuming Redis pub/sub/stream maps are wired in `RedisDepsModule`** — use [`forze-custom-deps`](../forze-custom-deps/SKILL.md) or `MockDepsModule`.
5. **Importing SQS/RabbitMQ adapters in handlers** — use contracts and dependency keys.

## Reference

- [SQS integration](https://morzecrew.github.io/forze/integrations/sqs/)
- [RabbitMQ integration](https://morzecrew.github.io/forze/integrations/rabbitmq/)
- [Queue contracts](https://morzecrew.github.io/forze/reference/contracts/messaging/)
- [Pub/Sub contracts](https://morzecrew.github.io/forze/reference/contracts/)
- [Stream contracts](https://morzecrew.github.io/forze/reference/contracts/)

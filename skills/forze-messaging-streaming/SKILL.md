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

`QueueSpec.name` is the logical route. Resolve query/command factories with `ctx.dep(..., route=spec.name)(ctx, spec)`.

```python
from enum import StrEnum

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)


class ResourceName(StrEnum):
    ORDERS = "orders"


class OrderPayload(BaseModel):
    order_id: str
    customer_id: str


order_queue = QueueSpec(name=ResourceName.ORDERS, model=OrderPayload)

writer = ctx.dep(QueueCommandDepKey, route=order_queue.name)(ctx, order_queue)
await writer.enqueue("orders", OrderPayload(order_id="o-1", customer_id="c-1"))

reader = ctx.dep(QueueQueryDepKey, route=order_queue.name)(ctx, order_queue)
messages = await reader.receive("orders", limit=10)
await reader.ack("orders", [msg["id"] for msg in messages])
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

events = PubSubSpec(name=ResourceName.ORDERS, model=OrderPayload)
publisher = ctx.dep(PubSubCommandDepKey, route=events.name)(ctx, events)
await publisher.publish("orders.created", payload, type="order.created")
```

`MockDepsModule` registers pub/sub factories. Redis pub/sub adapters exist, but `RedisDepsModule` does not currently expose pub/sub maps; register them through a custom deps module if needed.

## Stream contracts

Streams model append-only logs and consumer-group reads.

```python
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec

stream_spec = StreamSpec(name=ResourceName.ORDERS, model=OrderPayload)
stream = ctx.dep(StreamCommandDepKey, route=stream_spec.name)(ctx, stream_spec)
entry_id = await stream.append("orders", payload, type="order.created")
```

Use `StreamQueryDepKey` for `read` / `tail`, and `StreamGroupQueryDepKey` for consumer-group reads and acknowledgements. `MockDepsModule` registers stream factories. Redis stream adapters exist; use a custom deps module until `RedisDepsModule` exposes stream maps.

## Processing rules

- Ack only after the business operation succeeds.
- Use `nack(..., requeue=True)` for transient failures and `requeue=False` when the message should move toward DLQ/provider handling.
- Prefer idempotent consumers; message brokers can redeliver.
- Wrap document mutations and enqueue/outbox-style side effects with transactions and `defer_after_commit` when duplicate or premature events would hurt.

## Anti-patterns

1. **Resolving queue ports without `route=spec.name`** when using SQS/RabbitMQ routed modules.
2. **Using queue names as spec names by accident** — spec names route deps; queue/topic/stream names are provider-level names.
3. **Acknowledging before processing succeeds** — failures become data loss.
4. **Assuming Redis pub/sub/stream deps maps are wired in `RedisDepsModule`** — use a custom module or mock until maps exist.
5. **Importing SQS/RabbitMQ adapters in usecases** — use contracts and dependency keys.

## Reference

- [`pages/docs/integrations/sqs.md`](../../pages/docs/integrations/sqs.md)
- [`pages/docs/integrations/rabbitmq.md`](../../pages/docs/integrations/rabbitmq.md)
- [`src/forze/application/contracts/queue`](../../src/forze/application/contracts/queue)
- [`src/forze/application/contracts/pubsub`](../../src/forze/application/contracts/pubsub)
- [`src/forze/application/contracts/stream`](../../src/forze/application/contracts/stream)

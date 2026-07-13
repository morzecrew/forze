---
name: forze-messaging-streaming
description: >-
  Uses Forze queue, pub/sub, and stream contracts with QueueSpec, PubSubSpec,
  StreamSpec, SQSDepsModule, RabbitMQDepsModule, RedisDepsModule stream/pub-sub
  maps, KafkaDepsModule commit-stream groups, and Mock adapters. Use when
  producing, consuming, or testing async messages/events.
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
from forze_rabbitmq import RabbitMQDepsModule, RabbitMQQueueConfig
from forze_sqs import SQSDepsModule, SQSQueueConfig

sqs_module = SQSDepsModule(
    client=sqs_client,
    queue_readers={ResourceName.ORDERS: SQSQueueConfig(namespace="app")},
    queue_writers={ResourceName.ORDERS: SQSQueueConfig(namespace="app")},
)

rabbit_module = RabbitMQDepsModule(
    client=rabbit_client,
    queue_readers={ResourceName.ORDERS: RabbitMQQueueConfig(namespace="app")},
    queue_writers={ResourceName.ORDERS: RabbitMQQueueConfig(namespace="app")},
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

`MockDepsModule` registers pub/sub factories. For production, `RedisDepsModule` exposes a `pubsub={route: RedisPubSubConfig()}` map that registers `PubSubQueryDepKey` / `PubSubCommandDepKey` for those routes.

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

Use `StreamQueryDepKey` for `read` / `tail`. Consumer groups come in two disciplines: `AckStreamGroupQueryDepKey` for per-message ack + `claim` recovery (Redis-class), and `CommitStreamGroupQueryDepKey` for per-partition offset `commit` on a Kafka-class log (with a `CommitStreamGroupAdminDepKey` for `ensure_topic` / `ensure_group` / `reset_offsets` / `lag`). `MockDepsModule` registers all of them. In production, `RedisDepsModule` wires the ack discipline via `streams={route: RedisStreamConfig()}` (stream query/command + `AckStreamGroup*` keys), and `KafkaDepsModule` wires the commit discipline via `streams=` / `commit_groups=` (`CommitStreamGroupQueryDepKey` / `CommitStreamGroupAdminDepKey`).

## Processing rules

- Ack only after the business operation succeeds.
- Use `nack(..., requeue=True)` for transient failures and `requeue=False` when the message should move toward DLQ/provider handling.
- Prefer idempotent consumers; message brokers can redeliver.
- Wrap document mutations and enqueue/outbox-style side effects with transactions and `defer_after_commit` when duplicate or premature events would hurt.

## Anti-patterns

1. **Resolving queue ports without `route=spec.name`** when using SQS/RabbitMQ routed modules.
2. **Using queue names as spec names by accident** — spec names route deps; queue/topic/stream names are provider-level names.
3. **Acknowledging before processing succeeds** — failures become data loss.
4. **Mixing consumer-group disciplines** — Redis streams are ack-discipline (`AckStreamGroup*`), Kafka is commit-discipline (`CommitStreamGroup*`); a handler written for one does not port to the other unchanged.
5. **Importing SQS/RabbitMQ/Redis/Kafka adapters in handlers** — use contracts and dependency keys.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [SQS integration](https://morzecrew.github.io/forze/latest/integrations/sqs/)
- [RabbitMQ integration](https://morzecrew.github.io/forze/latest/integrations/rabbitmq/)
- [Redis integration](https://morzecrew.github.io/forze/latest/integrations/redis/)
- [Kafka integration](https://morzecrew.github.io/forze/latest/integrations/kafka/)
- [Messaging delivery models](https://morzecrew.github.io/forze/latest/data-events/messaging-delivery-models/)
- [Queue contracts](https://morzecrew.github.io/forze/latest/reference/contracts/messaging/)
- [Stream contracts](https://morzecrew.github.io/forze/latest/reference/contracts/streaming/)

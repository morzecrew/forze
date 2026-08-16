# Queues

Queue contracts and the SQS and RabbitMQ wiring behind them. Fan-out and log-shaped consumption are [pub/sub and streams](messaging-pubsub-streams.md).

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

## Anti-patterns

- **Resolving queue ports without `route=spec.name`** when using SQS/RabbitMQ routed modules.
- **Using queue names as spec names by accident** — spec names route deps; queue/topic/stream names are provider-level names.
- **Importing SQS/RabbitMQ/Redis/Kafka adapters in handlers** — use contracts and dependency keys.

## Reference

- [SQS integration](https://morzecrew.github.io/forze/latest/integrations/sqs/)
- [RabbitMQ integration](https://morzecrew.github.io/forze/latest/integrations/rabbitmq/)
- [Messaging delivery models](https://morzecrew.github.io/forze/latest/data-events/messaging-delivery-models/)
- [Queue contracts](https://morzecrew.github.io/forze/latest/reference/contracts/messaging/)

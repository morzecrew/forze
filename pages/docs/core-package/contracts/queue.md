# Queue contracts

Queue contracts model point-to-point messaging where workers receive,
acknowledge, and optionally requeue messages.

## `QueueSpec[M]`

| Section | Details |
|---------|---------|
| Purpose | Names a logical queue namespace and the Pydantic payload model. |
| Import path | `from forze.application.contracts.queue import QueueSpec` |
| Type parameters | `M`, the message payload model. |
| Required fields | `name`, `model`. |
| Returned values | Passed to queue dep factories to build query or command ports. |
| Common implementations | Mock queue adapter, SQS adapter, RabbitMQ adapter. |
| Related dependency keys | `QueueQueryDepKey`, `QueueCommandDepKey`. |
| Minimal example | `order_queue = QueueSpec(name="orders", model=OrderPayload)` |
| Related pages | [SQS](../../integrations/sqs.md), [RabbitMQ](../../integrations/rabbitmq.md). |

## `QueueQueryPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Receives, consumes, acknowledges, and negatively acknowledges queue messages. |
| Import path | `from forze.application.contracts.queue import QueueQueryPort` |
| Type parameters | `M`, the message payload model. |
| Required methods | `receive`, `consume`, `ack`, `nack`. |
| Returned values | Lists or async iterators of `QueueMessage[M]`; `ack`/`nack` return counts. |
| Common implementations | Mock, SQS, RabbitMQ. |
| Related dependency keys | `QueueQueryDepKey`. |
| Minimal example | `messages = await queue.receive("orders", limit=10)` |
| Related pages | [Background Workflow](../../recipes/background-workflow.md). |

## `QueueCommandPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Enqueues one or many queue messages. |
| Import path | `from forze.application.contracts.queue import QueueCommandPort` |
| Type parameters | `M`, the message payload model. |
| Required methods | `enqueue`, `enqueue_many`. |
| Returned values | Message id string or list of message ids. |
| Common implementations | Mock, SQS, RabbitMQ. |
| Related dependency keys | `QueueCommandDepKey`. |
| Minimal example | `message_id = await queue.enqueue("orders", payload)` |
| Related pages | [Contracts overview](../contracts.md). |

## `QueueMessage[M]`

| Section | Details |
|---------|---------|
| Purpose | Typed message shape returned by queue query ports. |
| Import path | `from forze.application.contracts.queue import QueueMessage` |
| Type parameters | `M`, the message payload model. |
| Required fields | `queue`, `id`, `payload`; optional `type`, `enqueued_at`, `key`. |
| Returned values | N/A; this is the returned value type. |
| Common implementations | `TypedDict` produced by queue adapters. |
| Related dependency keys | Produced through `QueueQueryDepKey` implementations. |
| Minimal example | `payload = message["payload"]` |
| Related pages | [Mock integration](../../integrations/mock.md). |

    :::python
    from forze.application.contracts.queue import QueueCommandDepKey, QueueSpec

    order_queue = QueueSpec(name="orders", model=OrderPayload)
    writer = ctx.dep(QueueCommandDepKey)(ctx, order_queue)
    message_id = await writer.enqueue("orders", OrderPayload(order_id="A-1"))

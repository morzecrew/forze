# RabbitMQ Integration

`forze_rabbitmq` provides message queue adapters backed by RabbitMQ. It implements `QueueReadPort` and `QueueWritePort` using `aio-pika` (an async AMQP client). The adapter uses work queue semantics with durable queues, persistent messages, and manual acknowledgement.

## Installation

    :::bash
    uv add 'forze[rabbitmq]'

## Runtime wiring

Create a client, register it via the dependency module, and add a lifecycle step for connection management:

    :::python
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_rabbitmq import (
        RabbitMQClient,
        RabbitMQConfig,
        RabbitMQDepsModule,
        rabbitmq_lifecycle_step,
    )

    client = RabbitMQClient()
    module = RabbitMQDepsModule(client=client)

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            rabbitmq_lifecycle_step(
                dsn="amqp://guest:guest@localhost:5672/",
                config=RabbitMQConfig(
                    heartbeat=60,
                    prefetch_count=100,
                ),
            )
        ),
    )

### RabbitMQConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| `heartbeat` | `int` | `60` | AMQP heartbeat interval (seconds) |
| `connect_timeout` | `float \| None` | `5.0` | Connection timeout (seconds) |
| `queue_durable` | `bool` | `True` | Declare queues as durable |
| `persistent_messages` | `bool` | `True` | Use persistent delivery mode |
| `publisher_confirms` | `bool` | `True` | Enable publisher confirms |
| `prefetch_count` | `int` | `100` | Consumer prefetch limit |

### What gets registered

`RabbitMQDepsModule` registers these dependency keys:

| Key | Capability |
|-----|------------|
| `RabbitMQClientDepKey` | Raw RabbitMQ client for direct operations |
| `QueueReadDepKey` | Queue read adapter factory |
| `QueueWriteDepKey` | Queue write adapter factory |

## Queue specification

A `QueueSpec` binds a queue namespace to a Pydantic message model:

    :::python
    from pydantic import BaseModel
    from forze.application.contracts.queue import QueueSpec


    class TaskPayload(BaseModel):
        task_id: str
        action: str
        params: dict


    task_queue = QueueSpec(name="tasks", model=TaskPayload)

## Producing messages

Resolve the write port and send messages:

    :::python
    from forze.application.contracts.queue import QueueWriteDepKey

    writer = ctx.dep(QueueWriteDepKey)(ctx, task_queue)

    # Send a single message
    message_id = await writer.enqueue(
        "tasks",
        TaskPayload(task_id="t-001", action="process", params={"retry": 3}),
        type="task.dispatch",
    )

    # Send a batch
    ids = await writer.enqueue_many(
        "tasks",
        [
            TaskPayload(task_id="t-002", action="index", params={}),
            TaskPayload(task_id="t-003", action="notify", params={}),
        ],
        type="task.dispatch",
    )

### Message attributes

| Parameter | Purpose | RabbitMQ mapping |
|-----------|---------|-----------------|
| `type` | Message type/category | AMQP `type` property |
| `key` | Routing or partition key | `forze_key` header |
| `enqueued_at` | Message timestamp | AMQP `timestamp` property |

Messages are sent with `content_type="application/json"` and persistent delivery mode by default.

## Consuming messages

### Receive a batch

    :::python
    from forze.application.contracts.queue import QueueReadDepKey

    reader = ctx.dep(QueueReadDepKey)(ctx, task_queue)

    messages = await reader.receive("tasks", limit=10)

    for msg in messages:
        print(f"Task: {msg['payload'].task_id} -> {msg['payload'].action}")

    # Acknowledge processed messages
    await reader.ack("tasks", [msg["id"] for msg in messages])

### Continuous consumption

    :::python
    async for msg in reader.consume("tasks"):
        try:
            await handle_task(msg["payload"])
            await reader.ack("tasks", [msg["id"]])
        except Exception:
            await reader.nack("tasks", [msg["id"]], requeue=True)

The `consume()` method returns an async iterator that continuously polls the queue. It uses `basic_get` under the hood with the configured timeout for each poll cycle.

### Acknowledgement

    :::python
    # Acknowledge (remove from queue)
    await reader.ack("tasks", [msg["id"]])

    # Negative acknowledge with requeue (make available again)
    await reader.nack("tasks", [msg["id"]], requeue=True)

    # Negative acknowledge without requeue (discard or route to DLX)
    await reader.nack("tasks", [msg["id"]], requeue=False)

The adapter tracks pending messages internally using delivery tags. Each message ID is unique within the client's lifetime and maps to the underlying AMQP message for ack/nack operations.

## QueueMessage fields

Each message is a `QueueMessage[M]` TypedDict:

| Field | Type | Description |
|-------|------|-------------|
| `queue` | `str` | Queue name |
| `id` | `str` | Internal message identifier (delivery tag based) |
| `payload` | `M` | Deserialized Pydantic model |
| `type` | `str \| None` | AMQP message type property |
| `enqueued_at` | `datetime \| None` | AMQP timestamp property |
| `key` | `str \| None` | Value from `forze_key` header |

## Using in usecases

### Producer usecase

    :::python
    from forze.application.contracts.queue import QueueWriteDepKey
    from forze.application.execution import Usecase


    class DispatchTask(Usecase[TaskPayload, str]):
        async def main(self, args: TaskPayload) -> str:
            writer = self.ctx.dep(QueueWriteDepKey)(self.ctx, task_queue)
            return await writer.enqueue("tasks", args, type="task.dispatch")

### Consumer usecase

    :::python
    from forze.application.contracts.queue import QueueReadDepKey


    class TaskWorker(Usecase[None, None]):
        async def main(self, args: None) -> None:
            reader = self.ctx.dep(QueueReadDepKey)(self.ctx, task_queue)

            async for msg in reader.consume("tasks"):
                try:
                    await self._handle(msg["payload"])
                    await reader.ack("tasks", [msg["id"]])
                except Exception:
                    await reader.nack("tasks", [msg["id"]], requeue=True)

        async def _handle(self, task: TaskPayload) -> None:
            pass

## Connection management

The client uses `aio_pika.connect_robust` for automatic reconnection. Key behaviors:

- **Channel reuse**: within a context scope, channels are reused via context variables to avoid overhead
- **Nested scopes**: nested `channel()` calls reuse the parent channel
- **Pending message tracking**: the client maintains a dedicated channel for consumer operations and tracks unacknowledged messages

The lifecycle step handles connection setup and teardown:

    :::python
    # Startup: connect_robust with heartbeat and timeout
    # Shutdown: close pending channel, close connection, clear state

## Queue declaration

Queues are declared automatically on first use with `durable=True` (configurable via `RabbitMQConfig.queue_durable`). The adapter uses the default exchange with the queue name as the routing key.

## Dead letter exchanges

Configure DLX on queue declaration for messages that fail processing. This is an infrastructure concern configured in RabbitMQ management or via the adapter:

    :::bash
    # In RabbitMQ management or via rabbitmqctl
    rabbitmqctl set_policy DLX ".*" '{"dead-letter-exchange": "dlx"}' --apply-to queues

Messages rejected with `requeue=False` are routed to the configured dead letter exchange.

## Combining with other modules

RabbitMQ is typically combined with Postgres and Redis:

    :::python
    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            PostgresDepsModule(client=pg, rw_documents={...})(),
            RedisDepsModule(client=redis, caches={...})(),
            RabbitMQDepsModule(client=rabbitmq)(),
        ),
    )

    lifecycle = LifecyclePlan.from_steps(
        postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
        redis_lifecycle_step(dsn="redis://...", config=RedisConfig()),
        rabbitmq_lifecycle_step(dsn="amqp://guest:guest@localhost:5672/", config=RabbitMQConfig()),
    )

### Event-driven pattern with after-commit effects

Use RabbitMQ as a reliable event bus by dispatching messages in after-commit effects:

    :::python
    from forze.application.composition.document import DocumentOperation, tx_document_plan


    def order_created_effect(ctx):
        async def effect(args, result):
            writer = ctx.dep(QueueWriteDepKey)(ctx, order_events_queue)
            await writer.enqueue(
                "order-events",
                OrderCreatedEvent(order_id=str(result.id)),
                type="order.created",
            )
            return result
        return effect


    plan = (
        tx_document_plan
        .after_commit(DocumentOperation.CREATE, order_created_effect)
    )

This ensures messages are only sent after the database transaction commits successfully, preventing phantom messages from rolled-back transactions.

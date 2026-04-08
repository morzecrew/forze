# SQS Integration

`forze_sqs` provides message queue adapters backed by Amazon SQS or any SQS-compatible service (Yandex Message Queue, LocalStack, etc.). It implements `QueueReadPort` and `QueueWritePort` using `aioboto3`.

## Installation

    :::bash
    uv add 'forze[sqs]'

## Runtime wiring

Create a client, register it via the dependency module, and add a lifecycle step for session management:

    :::python
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_sqs import SQSClient, SQSConfig, SQSDepsModule, sqs_lifecycle_step

    client = SQSClient()
    module = SQSDepsModule(client=client)

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            sqs_lifecycle_step(
                endpoint="https://sqs.us-east-1.amazonaws.com",
                region_name="us-east-1",
                access_key_id="your-access-key",
                secret_access_key="your-secret-key",
            )
        ),
    )

### LocalStack / Yandex Message Queue configuration

    :::python
    sqs_lifecycle_step(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )

### SQSConfig options

Optional tuning can be passed via `SQSConfig` (botocore `Config`-compatible):

| Option | Type | Purpose |
|--------|------|---------|
| `region_name` | `str` | AWS region |
| `connect_timeout` | `int \| float` | Connection timeout (seconds) |
| `read_timeout` | `int \| float` | Read timeout (seconds) |
| `max_pool_connections` | `int` | HTTP connection pool size |
| `tcp_keepalive` | `bool` | Enable TCP keepalive |

### What gets registered

`SQSDepsModule` registers these dependency keys:

| Key | Capability |
|-----|-----------|
| `SQSClientDepKey` | Raw SQS client for direct operations |
| `QueueReadDepKey` | Queue read adapter factory |
| `QueueWriteDepKey` | Queue write adapter factory |

## Queue specification

A `QueueSpec` binds a queue namespace to a Pydantic message model:

    :::python
    from pydantic import BaseModel
    from forze.application.contracts.queue import QueueSpec


    class OrderPayload(BaseModel):
        order_id: str
        customer_id: str
        total: float


    order_queue = QueueSpec(name="orders", model=OrderPayload)

## Producing messages

Resolve the write port via dependency key and send messages:

    :::python
    from forze.application.contracts.queue import QueueWriteDepKey

    writer = ctx.dep(QueueWriteDepKey)(ctx, order_queue)

    message_id = await writer.enqueue(
        "orders",
        OrderPayload(order_id="abc-123", customer_id="cust-1", total=99.99),
        type="order.created",
    )

### Queue name resolution

The adapter handles queue name resolution automatically:

- **Queue URLs** (starting with `http://` or `https://`) are used directly
- **Queue names** are resolved to URLs via the SQS `GetQueueUrl` API and cached

### FIFO queue support

Pass `key` to set the `MessageGroupId` for FIFO queues. The adapter generates a `MessageDeduplicationId` automatically:

    :::python
    await writer.enqueue(
        "orders.fifo",
        payload,
        key="customer-42",
        type="order.created",
    )

## Consuming messages

### Receive a batch

    :::python
    from forze.application.contracts.queue import QueueReadDepKey
    from datetime import timedelta

    reader = ctx.dep(QueueReadDepKey)(ctx, order_queue)

    messages = await reader.receive(
        "orders",
        limit=10,
        timeout=timedelta(seconds=20),
    )

    for msg in messages:
        print(f"Order: {msg['payload'].order_id}")

    await reader.ack("orders", [msg["id"] for msg in messages])

### Continuous consumption

    :::python
    async for msg in reader.consume("orders", timeout=timedelta(seconds=20)):
        try:
            await process_order(msg["payload"])
            await reader.ack("orders", [msg["id"]])
        except Exception:
            await reader.nack("orders", [msg["id"]], requeue=True)

### Negative acknowledgement

    :::python
    await reader.nack("orders", [msg["id"]], requeue=True)
    await reader.nack("orders", [msg["id"]], requeue=False)

When `requeue=True`, the adapter resets the visibility timeout to 0 so the message is immediately available for other consumers. When `requeue=False`, the message is deleted.

## QueueMessage fields

Each message is a `QueueMessage[M]` TypedDict:

| Field | Type | Description |
|-------|------|-------------|
| `queue` | `str` | Queue name or URL |
| `id` | `str` | SQS receipt handle (used for ack/nack) |
| `payload` | `M` | Deserialized Pydantic model |
| `type` | `str \| None` | Message type attribute |
| `enqueued_at` | `datetime \| None` | Timestamp from message attributes or SentTimestamp |
| `key` | `str \| None` | Message group ID (FIFO queues) |

## Using in usecases

    :::python
    from forze.application.contracts.queue import QueueWriteDepKey
    from forze.application.execution import Usecase


    class EnqueueOrder(Usecase[OrderPayload, str]):
        async def main(self, args: OrderPayload) -> str:
            writer = self.ctx.dep(QueueWriteDepKey)(self.ctx, order_queue)
            return await writer.enqueue("orders", args, type="order.created")

## SQS-specific behavior

### Message encoding

The adapter encodes message bodies as base64 to safely handle binary payloads in SQS (which only supports UTF-8 strings). A `forze_encoding=b64` message attribute is set so the decoder knows to base64-decode on receipt.

### Batch chunking

SQS limits batch operations to 10 messages. The adapter automatically chunks larger batches into multiple API calls. Failed entries within a batch raise `InfrastructureError`.

### Long polling

Pass `timeout` to `receive()` to enable SQS long polling. The maximum wait time is 20 seconds (SQS limit). Long polling reduces empty responses and API costs.

### Queue name sanitization

Queue names are automatically sanitized: unsupported characters are replaced with `_`, and the `.fifo` suffix is preserved for FIFO queues. Maximum name length is 80 characters.

### Dead letter queues

DLQ configuration is managed outside Forze via AWS console, CloudFormation, or Terraform. Messages that exceed `maxReceiveCount` are automatically moved to the DLQ by SQS.

## Combining with other modules

    :::python
    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            PostgresDepsModule(client=pg, rw_documents={...})(),
            RedisDepsModule(client=redis, caches={...})(),
            SQSDepsModule(client=sqs)(),
        ),
    )

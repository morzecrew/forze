# RabbitMQ Integration

## Page opening

`forze_rabbitmq` provides RabbitMQ-backed queue ports for Forze. It wraps `aio-pika` connection/channel management, queue naming, message encoding, acknowledgement, and dependency registration behind the queue contracts.

| Topic | Details |
|------|---------|
| What it provides | `RabbitMQClient`, optional routed client, queue read/write adapters, lifecycle hooks, and queue dependency registration. |
| Supported Forze contracts | `QueueQueryDepKey` and `QueueCommandDepKey`, plus `RabbitMQClientDepKey` for infrastructure access. |
| When to use it | Use this integration when RabbitMQ is your broker for command/event handoff, worker queues, routing-key based fan-out, or local broker-backed asynchronous processing. |

## Installation

```bash
uv add 'forze[rabbitmq]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `rabbitmq` installs `aio-pika`. |
| Required service | RabbitMQ. |
| Local development dependency | A local RabbitMQ broker or container. Integration tests normally use testcontainers. |

## Minimal setup

### Client

```python
from forze_rabbitmq import RabbitMQClient, RabbitMQConfig

rabbit = RabbitMQClient()
```

Use `RoutedRabbitMQClient` when tenant or route identity selects a broker connection.

### Config

```python
from forze_rabbitmq import RabbitMQQueueConfig

orders_queue = RabbitMQQueueConfig(namespace="orders", tenant_aware=True)
```

The namespace prefixes queue names and can include tenant identity when `tenant_aware=True`.

### Deps module

```python
from forze.application.execution import DepsPlan
from forze_rabbitmq import RabbitMQDepsModule

rabbit_module = RabbitMQDepsModule(
    client=rabbit,
    queue_readers={"orders": orders_queue},
    queue_writers={"orders": orders_queue},
)

deps_plan = DepsPlan.from_modules(rabbit_module)
```

The route key should match your `QueueSpec.name`.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_rabbitmq import rabbitmq_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    rabbitmq_lifecycle_step(
        dsn="amqp://guest:guest@localhost:5672/",
        config=RabbitMQConfig(prefetch_count=100),
    )
)
```

Use `routed_rabbitmq_lifecycle_step(client=routed_rabbit)` with `RoutedRabbitMQClient` and do not combine routed and non-routed lifecycle steps for the same client.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Queue reads | `ConfigurableRabbitMQQueueRead` / `RabbitMQQueueAdapter`. | `QueueQueryDepKey`, route usually equal to `QueueSpec.name`. | Requires queues/exchanges/routing choices to align with broker topology; consumers must ack/nack messages. |
| Queue writes | `ConfigurableRabbitMQQueueWrite` / `RabbitMQQueueAdapter`. | `QueueCommandDepKey`, route usually equal to `QueueSpec.name`. | Publisher confirms and persistence depend on `RabbitMQConfig` and broker policy. |
| Raw client | `RabbitMQClient` or `RoutedRabbitMQClient`. | `RabbitMQClientDepKey`. | Prefer queue contracts in usecases unless broker-specific operations are required. |

## Complete recipe link

See [Background Workflow](../recipes/background-workflow.md) for the long-form background-processing recipe pattern. Use this page for RabbitMQ-specific adapter and operations reference.

## Configuration reference

### Connection settings

`RabbitMQClient` connects with an AMQP URL. Configure credentials, vhost, TLS, and host failover in the URL/client construction supported by `aio-pika`.

### Pool settings

`RabbitMQConfig` controls connection heartbeat, `connect_timeout`, `prefetch_count`, queue durability, persistent messages, and publisher confirms. Prefetch is the main backpressure knob for consumers.

### Serialization settings

Queue adapters serialize Pydantic message payloads through the RabbitMQ queue codec. Keep queue message models versioned when producers and consumers deploy independently.

### Retry/timeout behavior

Connection establishment is bounded by `connect_timeout`; broker liveness uses `heartbeat`. Message retry behavior is controlled by your consumer's `ack`/`nack` decisions and broker dead-letter/redelivery policy.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | Broker topology is operational configuration. Declare exchanges, queues, bindings, dead-letter exchanges, and policies before or during deployment. |
| Cleanup/shutdown | Register `rabbitmq_lifecycle_step` or `routed_rabbitmq_lifecycle_step` so connections/channels close cleanly. Drain workers before shutdown when possible. |
| Idempotency/caching behavior | RabbitMQ may redeliver messages. Make consumers idempotent or use a Forze idempotency adapter for side-effecting handlers. |
| Production caveats | Use durable queues and persistent messages for at-least-once delivery, configure DLQs, monitor unacked messages, and tune prefetch for worker capacity. |

## Troubleshooting

| Common error | Likely cause | Fix |
|--------------|--------------|-----|
| Messages are delivered repeatedly | Consumer fails before ack or explicitly nacks with requeue. | Make processing idempotent, ack only after success, and route poison messages to a DLQ. |
| Publisher returns before messages are durable | Publisher confirms or persistent messages are disabled. | Keep `publisher_confirms=True`, `persistent_messages=True`, and durable queues for important messages. |
| Consumers process too many messages at once | `prefetch_count` is too high for worker capacity. | Lower `RabbitMQConfig.prefetch_count`. |
| Queue names collide across tenants | Shared namespace or `tenant_aware=False`. | Use distinct namespaces or enable tenant-aware queue configuration. |

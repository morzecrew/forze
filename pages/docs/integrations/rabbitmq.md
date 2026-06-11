---
title: RabbitMQ
icon: lucide/inbox
summary: Produce and consume queue messages over AMQP
---

`forze[rabbitmq]` implements the queue contracts on a RabbitMQ (AMQP) broker —
enqueue work on one side, consume and acknowledge it on the other, behind the
queue ports.

## Install

```bash
uv add 'forze[rabbitmq]'
```

Needs a RabbitMQ broker.

## The client

```python
from forze_rabbitmq import RabbitMQClient

rabbit = RabbitMQClient()
```

`RoutedRabbitMQClient` resolves a per-tenant connection.

## Wire it

Register the queues you read from and write to, keyed by `QueueSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_rabbitmq import RabbitMQClient, RabbitMQConfig, RabbitMQDepsModule, RabbitMQQueueConfig, rabbitmq_lifecycle_step

orders_q = RabbitMQQueueConfig(namespace="orders")

deps = DepsRegistry.from_modules(
    RabbitMQDepsModule(client=rabbit, queue_readers={"orders": orders_q}, queue_writers={"orders": orders_q}),
)
lifecycle = LifecyclePlan.from_steps(
    rabbitmq_lifecycle_step(dsn="amqp://guest:guest@localhost:5672/", config=RabbitMQConfig()),
)
```

## What it provides

| Contract | Keyed by | Module arg |
|----------|----------|------------|
| Queue consume (receive, ack, nack) | `QueueSpec.name` | `queue_readers` |
| Queue produce (enqueue) | `QueueSpec.name` | `queue_writers` |

## Notes

- **At-least-once** — the broker can redeliver, so make consumers idempotent and
  ack only after success. Defaults favour durability: durable queues, persistent
  messages, publisher confirms; `prefetch_count` is the consumer backpressure
  knob.
- **Delayed delivery** needs `delayed_delivery=True` on the queue config (it
  declares one delay sibling queue per distinct delay value, with a queue-level
  TTL so a long delay never blocks a shorter one; idle delay queues are
  auto-expired by the broker); enqueuing with a `delay` without it is a
  precondition error.
- Exchange/queue/binding topology and DLQs are operational config, managed
  outside Forze.

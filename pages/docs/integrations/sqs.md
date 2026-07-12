---
title: SQS
icon: lucide/inbox
summary: Produce and consume queue messages on Amazon SQS
---

`forze[sqs]` implements the queue contracts on Amazon SQS (or an SQS-compatible
endpoint like YMQ or the floci emulator) — the same produce/consume ports as RabbitMQ, on a
managed queue.

## Install

```bash
uv add 'forze[sqs]'
```

Needs AWS SQS or a compatible endpoint.

## The client

```python
from forze_sqs import SQSClient

sqs = SQSClient()
```

`RoutedSQSClient` (with `SQSRoutingCredentials`) resolves per-tenant
region/credentials.

## Wire it

Register the queues you read from and write to, keyed by `QueueSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_sqs import SQSClient, SQSConfig, SQSDepsModule, SQSQueueConfig, sqs_lifecycle_step

orders_q = SQSQueueConfig(namespace="orders")

deps = DepsRegistry.from_modules(
    SQSDepsModule(client=sqs, queue_readers={"orders": orders_q}, queue_writers={"orders": orders_q}),
)
lifecycle = LifecyclePlan.from_steps(
    sqs_lifecycle_step(
        endpoint="https://sqs.us-east-1.amazonaws.com",
        region_name="us-east-1",
        access_key_id="…",
        secret_access_key="…",
        config=SQSConfig(),
    ),
)
```

## What it provides

| Contract | Keyed by | Module arg |
|----------|----------|------------|
| Queue consume (receive, ack, nack) | `QueueSpec.name` | `queue_readers` |
| Queue produce (enqueue) | `QueueSpec.name` | `queue_writers` |

## Notes

- **At-least-once, unordered** on standard queues — duplicates and reordering
  happen, so consumers must be idempotent and delete (ack) before the visibility
  timeout expires.
- **FIFO** is automatic when the queue name ends in `.fifo` (the client sets a
  message group from the enqueue `key`).
- `delay` / `not_before` map to SQS `DelaySeconds` — capped at 15 minutes; use a
  scheduler or [Temporal](temporal.md) for longer waits.
- Visibility timeout, redrive/DLQ, and IAM are queue attributes managed outside
  Forze.

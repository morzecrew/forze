# Scheduled queue jobs

Use this recipe when work should run on a schedule or after a delay, without adding Celery Beat or a second orchestration product.

## Ingredients

- A `QueueSpec` and queue command port ([queue contracts](../core-package/contracts/queue.md))
- [SQS](../integrations/sqs.md) or [RabbitMQ](../integrations/rabbitmq.md) (or [Mock](../integrations/mock.md) in tests)
- An external scheduler (Kubernetes CronJob, systemd timer, cloud scheduler) **or** `delay` / `not_before` on enqueue

## Pattern A: recurring jobs (cron)

The scheduler lives **outside** Forze. It calls your API or a CLI that runs a handler with `ExecutionContext`.

```text
CronJob → POST /internal/jobs/daily-report → handler → queue.enqueue(...)
Worker deployment → queue.receive / consume → domain logic → ack
```

Use a **deterministic message key** or idempotent handler when the scheduler may fire twice:

```python
run_day = date.today().isoformat()
await writer.enqueue(
    "daily-report",
    DailyReportPayload(day=run_day),
    key=f"daily-{run_day}",
)
```

For durable multi-step work, start a [Temporal workflow](../integrations/temporal.md) instead of a queue message, with a stable `workflow_id` per run day.

Alternatively, use [Inngest](../integrations/inngest.md) cron triggers with
`DurableFunctionSpec.operation` so the worker runs the same frozen registry operation as HTTP
(no external cron hitting an internal route).

## Pattern B: defer after an event (delay)

When a user action should trigger work later (for example a reminder in ten minutes), enqueue from the request handler:

```python
await writer.enqueue(
    "reminders",
    ReminderPayload(user_id=user_id),
    delay=timedelta(minutes=10),
)
```

| Backend | Notes |
|---------|--------|
| SQS | Max delay 15 minutes (`SQS_MAX_DELAY`) |
| RabbitMQ | Requires `delayed_delivery=True` on `RabbitMQQueueConfig` for the writer route |
| Mock | In-memory visibility time for tests |

`enqueued_at` records metadata on the message; it does not delay delivery.

## When to use something else

| Need | Prefer |
|------|--------|
| Recurring **workflow** with pause/backfill | [Durable workflow schedule](../core-package/contracts/durable-workflow-schedule.md) or cron → `DurableWorkflowCommandPort.start` |
| Delay longer than 15 minutes on SQS | External scheduler, DB outbox, or RabbitMQ with `delayed_delivery` |
| Complex DAG / step UI | Temporal or a dedicated orchestrator |

## Learn more

- [Background workflow](background-workflow.md) — choosing queue vs Temporal
- [Queue contracts](../core-package/contracts/queue.md) — `delay`, `not_before`, and backend limits

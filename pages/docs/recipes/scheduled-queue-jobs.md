---
title: Scheduled & delayed jobs
icon: lucide/calendar-clock
summary: Delay a one-off job, or run work on a schedule — without Celery Beat
---

Two different needs: **delay** a single job ("retry this in 5 minutes") and
**recurring** schedules ("every night at 02:00"). Forze handles the first on the
queue itself, and the second through a durable backend's scheduler.

## Delay a one-off job

`enqueue` takes a `delay` (relative `timedelta`) or a `not_before` (absolute
tz-aware `datetime`) — the message stays invisible until then:

```python
from datetime import timedelta

queue = ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, JOBS_QUEUE, route=JOBS_QUEUE.name)

await queue.enqueue("jobs", RetryCharge(invoice_id=id), delay=timedelta(minutes=5))
# or a fixed time:
await queue.enqueue("jobs", SendReminder(id=id), not_before=due_at)  # tz-aware datetime
```

!!! warning "`delay` and `not_before` are mutually exclusive"

    Pass one or the other, never both. `delay` must be non-negative; `not_before`
    must be timezone-aware. (`enqueued_at` is metadata only — it does **not**
    delay delivery.)

Backend limits:

| Backend | Delayed delivery |
|---------|------------------|
| **SQS** | up to `SQS_MAX_DELAY` (15 minutes) — longer delays need a scheduler |
| **RabbitMQ** | requires `RabbitMQQueueConfig(delayed_delivery=True)` on the route |
| **Mock** | honoured in-memory |

## Recurring schedules (cron)

A queue delay is one-shot. For *recurring* work, register a **durable function**
with a cron trigger — the durable backend ([Temporal](../integrations/temporal.md)
/ [Inngest](../integrations/inngest.md)) owns the schedule:

```python
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionSpec,
)

nightly = DurableFunctionSpec(
    run=DurableFunctionInvokeSpec(...),
    triggers=(DurableFunctionCronTrigger(expression="0 2 * * *"),),  # 02:00 daily
    operation="reports.nightly",  # run the same frozen-registry operation as HTTP
)
```

Setting `operation` means the scheduled run executes the *same* handler your HTTP
routes do — one implementation, two triggers.

## Notes

- The **scheduler lives outside Forze** — the queue/durable backend fires the
  trigger; Forze runs the work. There's no Celery Beat to operate.
- To schedule a job *reliably* as part of a transaction, stage it through the
  [outbox](transactional-outbox.md) rather than enqueuing inline.
- For a single deferred run with no recurrence, the queue `delay` is simpler than
  a durable function.

---
title: Background work
icon: lucide/workflow
summary: Start work and return early — choosing between a queue task and a durable workflow
---

A request kicks off work that shouldn't block the response — sending a report,
transcoding a video, running a multi-step fulfilment. Forze gives you two tools;
the choice is about how much you need to *observe and orchestrate* the work.

## Which one

| | **Queue task** | **Durable workflow** |
|---|----------------|----------------------|
| Shape | one fire-and-forget unit | multi-step, long-running |
| Status / result | none (it just runs) | `describe` / `query` / `result` |
| Retries / timers / signals | basic redelivery | built in (Temporal / Inngest) |
| Use when | "do this once, soon" | "run this process and let me track it" |

Reach for a **queue** when the work is a single task you don't need to follow.
Reach for a **durable workflow** when it has steps, can take minutes to days, or
the caller needs to ask "is it done?".

## Fire-and-forget with a queue

Enqueue the work and return — a worker consumes it elsewhere:

```python
from forze.application.contracts.queue import QueueCommandDepKey

queue = ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, REPORTS_QUEUE, route=REPORTS_QUEUE.name)
await queue.enqueue("reports", GenerateReport(account_id=account_id))
# return 202 Accepted immediately
```

## Start a durable workflow

`start` returns a handle the moment the workflow is accepted; the work continues
in the durable backend:

```python
from forze.application.contracts.durable.workflow import DurableWorkflowCommandDepKey

workflows = ctx.deps.resolve_configurable(ctx, DurableWorkflowCommandDepKey, FULFIL_SPEC, route=FULFIL_SPEC.name)
handle = await workflows.start(FulfilOrder(order_id=order_id), workflow_id=f"fulfil-{order_id}")
```

Then observe it through the query port:

```python
from forze.application.contracts.durable.workflow import DurableWorkflowQueryDepKey, DurableWorkflowRunStatus

q = ctx.deps.resolve_configurable(ctx, DurableWorkflowQueryDepKey, FULFIL_SPEC, route=FULFIL_SPEC.name)
run = await q.describe(handle)                    # coarse lifecycle: RUNNING / COMPLETED / …
if run.status is DurableWorkflowRunStatus.COMPLETED:
    result = await q.result(handle)               # the typed return value
# q.query(handle, query=…, args=…) reads in-flight workflow state
```

## Notes

- A durable workflow needs a real backend — [Temporal](../integrations/temporal.md)
  or [Inngest](../integrations/inngest.md) — for its durability, retries, and
  timers. The queue path runs on any [queue backend](../integrations/rabbitmq.md).
- A stable `workflow_id` makes `start` idempotent (`raise_on_already_started`
  controls the collision behaviour).
- To kick the work off *reliably* from a request — only if the write commits —
  stage it through the [outbox](transactional-outbox.md) instead of enqueuing
  directly.

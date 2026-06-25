---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and functions on an external engine — orchestration that outlives a process
---

Some work outlives the request that starts it: a multi-step fulfilment that runs for days,
retries a flaky payment, waits on a human approval, and must survive a deploy or a crash in
the middle. In-process [sagas](events-sagas.md) coordinate steps *within* one process;
**durable execution** runs the orchestration on an external engine
([Temporal](../integrations/temporal.md) / [Inngest](../integrations/inngest.md)) that
persists every step — so a crash resumes exactly where it left off, not from the top.

## The mental model: journaled progress

A durable workflow is ordinary code whose progress the engine **journals**. Each step's
result is recorded, so after a crash the engine replays the workflow and skips the steps
already done — the slow external calls, the timers, the waits resume rather than repeat. You
write the orchestration; the engine owns the durability, retries, and timers. That's the
difference from a queue task, which runs once with basic redelivery and no memory of where
it was.

## Three forms

- **Workflows** — multi-step, long-running, and *observable*: a `start` returns a handle
  immediately, and a query port reads coarse status, the typed result, or in-flight state.
  Signals and updates push messages into a running workflow.
- **Schedules** — fire a workflow on a cron or interval; the durable counterpart to a
  queue's delayed jobs.
- **Functions** — event-triggered work composed of individually-retried, memoized **steps**
  (the Inngest model).

A workflow start returns a handle you observe through the query port:

```python
handle = await workflows.start(FulfilOrder(order_id=order_id), workflow_id=f"fulfil-{order_id}")
run = await queries.describe(handle)          # coarse status: RUNNING / COMPLETED / FAILED / …
result = await queries.result(handle)         # the typed return value, once complete
```

A stable `workflow_id` makes `start` idempotent — the same id won't launch a second run.

## When to reach for it

| You need | Use |
| --- | --- |
| Multi-step work that must survive crashes, with status / retries / timers | **durable execution** |
| A single fire-and-forget task | a [queue](../reference/contracts/messaging.md) |
| Step coordination *within* one process or transaction | a [saga](events-sagas.md) |

To start a workflow **reliably** from a request — only if the write commits — stage it
through the [outbox](events-sagas.md) instead of starting it directly.

The ports and dep keys are the [durable reference](../reference/contracts/durable.md); the
worked flows are the [background work](../recipes/background-workflow.md) and
[scheduled jobs](../recipes/scheduled-queue-jobs.md) recipes.

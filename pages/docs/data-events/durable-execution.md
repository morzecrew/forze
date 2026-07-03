---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and functions on an external engine — orchestration that outlives a process
---

Some work outlives the request that starts it: a multi-step fulfilment that runs for days,
retries a flaky payment, waits on a human approval, and must survive a deploy or a crash in
the middle. In-process [sagas](events-sagas.md) coordinate steps *within* one process;
**durable execution** runs the orchestration against a store that persists every step — so a
crash resumes exactly where it left off, not from the top. That store can be an external
engine ([Temporal](../integrations/temporal.md) / [Inngest](../integrations/inngest.md)) or,
for a deployment that runs **only Postgres**, the [self-hosted tier](#self-hosted-on-postgres)
on the database you already operate.

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

## Self-hosted on Postgres

The external engines are *operational* dependencies — a Temporal cluster or the Inngest
service. For the common deployment that runs **only Postgres**, the self-hosted tier gives
you the **functions** form (memoized steps + crash recovery) and crash-resumable **sagas**
on the same database, with no engine to stand up.

It reuses the journaled-progress model, backed by two app-provided tables: a `durable_step`
**memo journal** (each step's result recorded so a replay skips it) and a `durable_run`
**run store** (run instances, claimed for recovery with `FOR UPDATE SKIP LOCKED`). Wire both
on the Postgres module and drive them with the `forze_kits` runner:

```python
deps = PostgresDepsModule(
    client=client,
    durable_step=PostgresDurableStepConfig(relation=("public", "durable_step")),
    durable_run=PostgresDurableRunConfig(relation=("public", "durable_run")),
)

registry = DurableFunctionRegistry()
registry.register("fulfil-order", fulfil_order)      # async (ctx, input) -> output
runner = DurableFunctionRunner(registry=registry)

await runner.enqueue(ctx, "fulfil-order", {"order_id": str(order_id)})
```

A registered function does its work in **steps** via the step port; each step memoizes, so a
re-invocation after a crash replays completed steps and resumes at the first incomplete one:

```python
async def fulfil_order(ctx, input):
    step = resolve_durable_step(ctx)
    charge = await step.run("charge", lambda: charge_card(ctx, input))   # journaled once
    await step.run("ship", lambda: ship(ctx, charge))
    return {"shipped": True}
```

A background scanner re-claims runs abandoned by a crash and re-invokes them —
`durable_recovery_background_lifecycle_step(runner=runner)` (pair it with the singleton
lifecycle guard so one replica leads).

### Crash-resumable sagas

The self-hosted tier closes the "an in-process saga is not crash-resumable" gap. Swap the
saga executor for the durable one and run the saga as a durable function — each step **and
each compensation** is journaled, so a crash mid-saga (or mid-rollback) resumes instead of
leaving committed steps un-compensated:

```python
deps = SagaDepsModule(executor=DurableSagaExecutor())      # swap the seam
registry.register(str(saga.name), durable_saga_handler(saga, OrderCtx))
await runner.run_now(ctx, str(saga.name), initial.model_dump(mode="json"))
```

The saga context must be a serializable `pydantic.BaseModel` (it is journaled between
steps). This tier is self-hosted-Postgres only and single-leader — a full workflow engine
(timers, signals, versioning, multi-worker scheduling) is still Temporal/Inngest. The two
tables come from your migrations; their schema is documented on the adapter classes.

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

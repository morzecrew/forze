# Durable contracts

**Durable** contracts cover **platform-orchestrated runs**: work whose retries, timing,
and step state are owned by an external engine—not generic database or message
durability.

Forze splits durable orchestration into two kinds under
`forze.application.contracts.durable`:

| Kind | Package | Use when |
|------|---------|----------|
| **Workflow** | [`durable.workflow`](durable-workflow.md) | Long-running sagas: signals, queries, child workflows, Temporal Schedules |
| **Function** | [`durable.function`](durable-function.md) | Event- or cron-triggered step runs (future: Inngest and similar) |

## Choosing a kind

| Need | Prefer |
|------|--------|
| Multi-day process, compensation, signals, strict history | [Durable workflow](durable-workflow.md) → Temporal |
| Recurring **workflow** runs with pause/backfill | [Durable workflow schedule](durable-workflow-schedule.md) |
| `user.signed_up` → email + analytics + short sleeps | [Durable function](durable-function.md) (emit event + steps) |
| Fan-out thousands of lightweight jobs | [Queue](queue.md) |
| Every night at 2am, one shot | External cron → handler → workflow start, event send, or enqueue ([Scheduled queue jobs](../../recipes/scheduled-queue-jobs.md)) |

## Contract pattern

Each kind follows the usual Forze shape: **Spec** → **Port** → **DepKey** /
**ConfigurableDepPort**. Workflow kinds use routed factories (`route=spec.name`);
function event emit uses routed factories; function **steps** use
`DurableFunctionStepDepKey` (simple dep, bound per invocation in the worker).

## Related pages

- [Contracts overview](../contracts.md)
- [Temporal integration](../../integrations/temporal.md)
- [Background workflow](../../recipes/background-workflow.md)

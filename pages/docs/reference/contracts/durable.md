---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and event-driven functions on an external engine
---

Durable execution runs **crash-resumable** work on an external engine: a long-running
workflow that survives process restarts, a schedule that fires it on a cron, or an
event-driven function. These contracts are **resolved by dep key** — there is no short
`ctx.*` accessor — and the engine integration ([Temporal](../../integrations/temporal.md) /
[Inngest](../../integrations/inngest.md)) provides the adapter. The concept is
[Durable execution](../../data-events/durable-execution.md); worked flows are the
[background workflow](../../recipes/background-workflow.md) and
[scheduled queue jobs](../../recipes/scheduled-queue-jobs.md) recipes.

## Workflows

`DurableWorkflowSpec[In, Out]` describes one workflow and its interaction points (each a
nested `DurableWorkflowInvokeSpec`):

| Field | Type | Meaning |
|-------|------|---------|
| `run` | `DurableWorkflowInvokeSpec[In, Out]` | the main invocation — typed input → output |
| `signals` | `dict[str, DurableWorkflowSignalSpec]` | async fire-and-forget messages into a running workflow |
| `queries` | `dict[str, DurableWorkflowQuerySpec]` | synchronous reads of running state |
| `updates` | `dict[str, DurableWorkflowUpdateSpec]` | request/response mutations of running state |

Resolve the two ports by dep key:

| Dep key | Side |
|---------|------|
| `DurableWorkflowCommandDepKey` | start, signal, update, cancel, terminate |
| `DurableWorkflowQueryDepKey` | run status (`DurableWorkflowRunStatus`), result, query |

A start returns a `DurableWorkflowHandle` (`workflow_id`, optional `run_id`).

## Schedules

`DurableWorkflowScheduleCommandDepKey` / `DurableWorkflowScheduleQueryDepKey` create, pause,
and inspect cron / interval schedules that start a workflow — the durable counterpart to a
queue's delayed jobs.

## Event-driven functions

`DurableFunctionSpec[In, Out]` describes a function started by events or a cron:

| Field | Type | Meaning |
|-------|------|---------|
| `run` | `DurableFunctionInvokeSpec[In, Out]` | the main invocation |
| `triggers` | `tuple[DurableFunctionTrigger, ...]` | how it starts — events and/or cron (at least one) |
| `operation` | `StrKey \| None` | when set, run this operation key from a frozen registry at invoke time |

| Dep key | Side |
|---------|------|
| `DurableFunctionEventCommandDepKey` | emit events that trigger functions |
| `DurableFunctionStepDepKey` | run memoized, individually-retried steps inside a function |

A `DurableFunctionEventSpec` binds an event channel to its payload codec.

## Implemented by

| Surface | Engine | Integration |
|---------|--------|-------------|
| Workflows + schedules | Temporal | [Temporal](../../integrations/temporal.md) |
| Event-driven functions | Inngest | [Inngest](../../integrations/inngest.md) |

A mock implements the surfaces so durable flows are testable without an engine.

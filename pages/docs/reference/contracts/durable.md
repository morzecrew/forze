---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and event-driven functions on an external engine
---

Durable execution runs **crash-resumable** work: a long-running workflow that survives
process restarts, a schedule that fires it on a cron, or an event-driven function. These
contracts are **resolved by dep key** — there is no short `ctx.*` accessor — and an engine
integration ([Temporal](../../integrations/temporal.md) /
[Inngest](../../integrations/inngest.md)) or the self-hosted Postgres tier provides the
adapter. The concept is
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
| `DurableWorkflowCommandDepKey` | `start`, `signal`, `update`, `cancel`, `terminate` |
| `DurableWorkflowQueryDepKey` | `query`, `result`, `describe` (a `DurableWorkflowRunDescription` with the `DurableWorkflowRunStatus`) |

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
| `DurableRunStoreDepKey` | the run store behind the self-hosted tier — enqueue / begin / renew / complete / fail, `claim_abandoned` recovery |
| `DurableRunAdminDepKey` | run listing — `list_runs(status=None, name=None, limit=50, cursor=None)` returns a cursor-paged `DurableRunPage` of `DurableRunRecord`s |
| `DurableScheduleStoreDepKey` | the cron schedule store — put / claim_due / advance / load / delete |

A `DurableFunctionEventSpec` binds an event channel to its payload codec. A run's status is
a `DurableRunStatus` — `pending` / `running` / `completed` / `failed` / `forward_incomplete`.

## Implemented by

| Surface | Engine | Integration |
|---------|--------|-------------|
| Workflows + schedules | Temporal | [Temporal](../../integrations/temporal.md) |
| Event-driven functions | Inngest | [Inngest](../../integrations/inngest.md) |
| Functions + steps + cron (self-hosted: run/step/schedule stores + `forze_kits` runner) | Postgres | [Postgres](../../integrations/postgres.md) — see [Durable execution → Self-hosted](../../data-events/durable-execution.md#self-hosted-on-postgres) |

A mock implements the surfaces so durable flows are testable without an engine.

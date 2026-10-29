---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and event-driven functions on an external engine
---

Durable execution runs **crash-resumable** work: a long-running workflow that survives
process restarts, a schedule that fires it on a cron, or an event-driven function. These
contracts are **resolved by dep key** — there is no short `ctx.*` accessor — and an engine
integration ([Temporal](../../integrations/temporal.md) /
[Inngest](../../integrations/inngest.md)) or the self-hosted tier (Postgres or MongoDB) provides the
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

A start returns a `DurableWorkflowHandle` (`workflow_id`, optional `run_id`). Temporal and
the mock leave `run_id` unset, so the handle follows its workflow id's latest run; a handle
with a `run_id` (one read from `describe`) is pinned to that run.

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
| `DurableFunctionStepDepKey` | run memoized steps inside a function — an engine tier may retry each one; the self-hosted tier runs each once and can retry the run (`DurableFunctionRunner(retry=)`) |
| `DurableRunStoreDepKey` | the run store behind the self-hosted tier — enqueue (with an `idempotency_key`, or a `coalesce_key` that joins the run waiting unclaimed under it) / begin / renew / complete / fail, `claim_abandoned` recovery, the fenced terminal landings `mark_cancelled` / `mark_timed_out`, and `mark_retry`, which sends a running run back to `pending` for its retry |
| `DurableRunAdminDepKey` | run listing and control — `list_runs(status=None, name=None, limit=50, cursor=None)` returns a cursor-paged `DurableRunPage`; `request_cancel(run_id)` asks a run to stop; `requeue_quarantined(run_ids=None)` returns quarantined runs to `pending` and says how many — unbound, it must name them while any belongs to a tenant; `requeue_failed(run_ids=None, name=None)` returns the named `failed` and `timed_out` runs to `pending` under their own id, so the steps they journaled replay — it needs `run_ids` or `name`, and skips a run whose cancel ask is unanswered; `purge_finished(older_than, statuses=PURGEABLE_STATUSES, name=None, limit=1000)` deletes up to `limit` runs that landed before the window, with their step journal, and says how many |
| `DurableScheduleStoreDepKey` | the cron schedule store — put / claim_due / advance / load / delete, and `set_input`, which replaces only a schedule's input and keeps its next fire and pause |

A `DurableFunctionEventSpec` binds an event channel to its payload codec. A run's status is
a `DurableRunStatus` — `pending` / `running` / `completed` / `failed` / `forward_incomplete`
/ `cancelled` / `timed_out` / `quarantined`. `quarantined` is not terminal: recovery could not
decrypt the run for long enough, its key being gone, and its scan no longer claims it until
`requeue_quarantined()` puts it back.

## What a bound tenant reaches

Every store behind these ports answers this the same way, and the shared conformance battery
holds all three to it.

| Verbs | Reach when a tenant is bound |
|-------|------------------------------|
| `begin`, `renew`, `load`, `complete` / `fail` / `mark_*` | that tenant's runs **and untagged ones** |
| `claim_abandoned`, `list_runs`, `request_cancel`, `requeue_quarantined`, `requeue_failed`, `purge_finished`, `refuse_cancel` | that tenant's runs, exactly |
| all of them, unbound | everything — the recovery and single-tenant role — except `requeue_quarantined` |
| `requeue_quarantined`, unbound and naming no runs | untagged runs only, and refused while any quarantined run in scope carries a tenant |
| `requeue_failed` naming neither runs nor a function | refused, bound or not |

The untagged arm on the first row is liveness, not laxity. A run tagged with no tenant belongs
to none, and a terminal write that matched nothing would leave it running until its lease
expired, reclaimed, and re-run — forever. The second row has no such problem, and widening an
enumeration is a disclosure, so it stays exact.

`enqueue` and `put` accept an explicit tenant. It is used for the relation, the stored tag and
the scoped id **together**, or refused: naming a tenant that contradicts a bound one raises
`authentication` / `tenant_mismatch`, while naming one where nothing is bound is honoured in
full. A `tenant_aware` store reads its binding first, so naming a tenant does not stand in for
being bound to one.

The schedule store expresses tenancy in its key rather than in a predicate — the stored id is
the schedule id scoped by its tenant — so it has no untagged allowance: a fallback lookup
would let two keys answer one `load`, and the compare-and-set that makes firing exactly-once
depends on that key being exact.

## Stopping a run

`request_cancel(run_id)` is the self-hosted tier's answer to the workflow tier's `cancel`.
It is **cooperative and only cooperative**: it records an ask and returns whether the ask was
recorded, never a promise about when the body notices.

| Run state | What happens | Returns |
|-----------|--------------|---------|
| `pending`, never claimed | lands `cancelled` immediately — nothing has executed | `True` |
| `running` | stamps `cancel_requested_at`; the lease holder lands it on its next heartbeat | `True` |
| `quarantined`, or `pending` after a requeue | stamps `cancel_requested_at`; decided once a claim can decrypt the run | `True` |
| terminal | nothing | `False` |
| unknown / not visible to the bound tenant | nothing | `False` |

The ask is **unfenced** (anyone may ask, and asking twice changes nothing) while the landing
is **fenced**, so a stale worker cannot cancel a run out from under its new owner. If the
holder dies carrying the stamp, recovery claims the run and lands it *without invoking the
body*. Observation latency for a running body is one heartbeat interval
(`lease_for / heartbeat_divisor`); a body that never awaits is bounded only by the runner's
`max_run_duration`.

Backends declare whether they can honour this at all through `DurableRunControlAware`; read
it with `durable_run_control_capabilities(port).supports_cancel`. A port that does not report
is treated as unable, and `DurableFunctionRunner.request_cancel` refuses rather than
accepting a request it cannot deliver.

Two record fields carry the outcome: `cancel_requested_at` (when it was asked) and
`cancel_refused_at` (when the run declined — a durable saga past its pivot must complete
forward).

## Implemented by

| Surface | Engine | Integration |
|---------|--------|-------------|
| Workflows + schedules | Temporal | [Temporal](../../integrations/temporal.md) |
| Event-driven functions | Inngest | [Inngest](../../integrations/inngest.md) |
| Functions + steps + cron (self-hosted: run/step/schedule stores + `forze_kits` runner) | Postgres, MongoDB | [Postgres](../../integrations/postgres.md), [MongoDB](../../integrations/mongo.md) — see [Durable execution → Self-hosted](../../data-events/durable-execution.md#self-hosted-on-your-own-database) |

A mock implements the surfaces so durable flows are testable without an engine.

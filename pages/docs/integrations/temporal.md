---
title: Temporal
icon: lucide/workflow
summary: Durable workflows, schedules, and signals on Temporal
---

`forze[temporal]` implements the durable-workflow contracts on
[Temporal](https://temporal.io) — starting, signalling, querying, and scheduling
long-running workflows behind a stable port.

## What this package is

`forze_temporal` will **never** grow a workflow model, an activity registry, a step
DSL, or an opinion about task-queue topology. Temporal's SDK is a complete authoring
surface with its own determinism sandbox, test framework, and documentation; wrapping
it produces a worse Temporal.

What the framework owns is the *boundary*: how a process connects (config, mTLS/API
key, namespace), what rides on the wire (codec, data converter), what crosses into
execution context (identity, tenant, correlation, replay-safe time), when things start
and stop (lifecycle, drain), and the schedule control plane.

Your `@workflow.defn` and `@activity.defn` are raw `temporalio`, and that is the
design — not a gap waiting to be filled.

## Install

```bash
uv add 'forze[temporal]'
```

Needs a Temporal frontend service **and one or more workers** polling the task
queues your workflows use.

## The client

```python
from forze_temporal import TemporalClient

temporal = TemporalClient()
```

`RoutedTemporalClient` resolves a per-tenant cluster/namespace.

### Settings

`TemporalSettings` holds the address, namespace, TLS switch and API key, and joins host
and port into the `host=` this step takes — see
[connection settings](index.md#connection-settings).

## Wire it

Each workflow route names the **task queue** its workers poll, keyed by
`DurableWorkflowSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_temporal import (
    TemporalClient,
    TemporalConfig,
    TemporalDepsModule,
    TemporalWorkflowConfig,
    temporal_lifecycle_step,
)

orders_wf = TemporalWorkflowConfig(queue="orders-tq")
configs = {"orders": orders_wf}


deps = DepsRegistry.from_modules(TemporalDepsModule(client=temporal, workflows=configs))
lifecycle = LifecyclePlan.from_steps(
    temporal_lifecycle_step(
        host="localhost:7233",
        config=TemporalConfig(namespace="default"),
        workflow_configs=configs,
    ),
)
```

## Start options

Retry policy, timeouts and id-reuse are properties of a workflow *kind*, so they are
declared once beside the queue rather than passed at every call site:

```python
from datetime import timedelta

from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from forze_temporal import TemporalStartOptions, TemporalWorkflowConfig

reports_wf = TemporalWorkflowConfig(
    queue="reports-tq",
    start_options=TemporalStartOptions(
        execution_timeout=timedelta(hours=6),
        retry_policy=RetryPolicy(maximum_attempts=1),
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    ),
)
```

Every field is unset by default, and an unset field is omitted from the start call
rather than sent as a default — declaring no options produces exactly the request the
package sent before they existed.

`DurableWorkflowCommandPort.start` does **not** learn about them: these are engine
vocabulary. A caller holding the engine-agnostic port cannot reach them; one who knows
it is Temporal can pass `options=` to the adapter, and only the fields they set win —
the rest of the configured set stands.

Two limits worth knowing. A **schedule** for the same workflow carries its own action
and does not pick these up. And a non-positive timeout is refused at construction,
because Temporal reads one as *unset* — silently widening the bound you meant to
tighten.

## Run a worker

The command port talks to the cluster; something still has to poll the task queue.
`temporal_worker_lifecycle_step` runs that worker under the runtime's supervision and
drain — see [Run a Temporal worker](../recipes/temporal-worker.md) for the full
entrypoint.

## Activity heartbeats

`heartbeat_timeout` is Temporal's real dead-worker detector: an activity that stops
heartbeating is rescheduled in seconds instead of at `start_to_close`.

For an activity with incremental state to report, call `activity.heartbeat(details)`
yourself — details are authoring surface, and the framework puts nothing between you
and the SDK there. For the common case ("this is alive, stop killing it"), the context
interceptor can beat on the activity's behalf:

```python
ExecutionContextInterceptor(ctx_dep=lambda: ctx, auto_heartbeat=True)
```

It beats at a third of each activity's `heartbeat_timeout`, and does nothing for
activities that declare none or run locally.

**Off by default, and that is a judgement rather than caution.** An automatic heartbeat
says *the process is alive*, which is not the claim *the activity is making progress*.
Switch it on and a wedged activity holds its lease until `start_to_close` instead of
being rescheduled at `heartbeat_timeout`.

## The escape hatch

`TemporalClientPort.native` hands back the configured `temporalio.client.Client` for
SDK surface the port deliberately omits — child workflows, continue-as-new, update
polling, exotic start options:

```python
client = ctx.deps.provide(TemporalClientDepKey)
handle = await client.native.start_workflow(...)  # same connection, same codec
```

Reach for it instead of a second `Client.connect`. A hand-built client carries none of
the configured data converter, interceptor stack, or rpc metadata — so payloads a
deployment believes are sealed go to the datastore in plaintext, and nothing fails.

Anything reachable *through* the port goes through the port: that is what tenancy,
error mapping and the test doubles can see.

## Schedules

The schedule command and query ports create, update, pause, trigger, describe and list
the schedules that start a workflow. Temporal keeps a cron expression only as the
calendar it compiles it into, so forze stores the expression as that calendar's comment
(`forze:cron=<expression>`, visible in the Temporal UI). Describe and list report
`timing.cron_expressions` as you wrote them, and an update that changes the timing
replaces them.

The cron rebuilt from the calendar, equivalent but not always the same text, is
reported instead for:

- a schedule created by an earlier forze, or whose timing was rewritten outside forze;
- an expression with its own `# comment` or a `CRON_TZ=` / `TZ=` prefix.

An outside edit that changes a calendar but keeps its comment still reports the old
expression.

## What it provides

| Contract | Keyed by |
|----------|----------|
| Durable workflow command / query (start, signal, update, query, result, cancel) | `DurableWorkflowSpec.name` |
| Durable workflow schedule command / query | `DurableWorkflowSpec.name` (same workflow route) |
| Client (and, through `native`, the SDK client) | `TemporalClientDepKey` |

## Notes

- **Workers are separate processes**, by deployment design — the route's `queue` must
  match what the worker polls. Being a separate process does not mean being
  hand-written: use the worker step above.
- The cluster host is the lifecycle step's `host=`; `TemporalConfig` carries
  `namespace` and interceptors.
- A schedule id that does not exist raises `not_found` (`schedule_not_found`). So does
  a workflow id that does not exist, or a closed run you signal, update or terminate
  (`workflow_not_found`, "not found or already closed"): Temporal answers both alike.
  Starting or creating with a taken id raises `conflict` (`workflow_already_exists`,
  `schedule_already_exists`). The codes are constants in
  `forze.application.contracts.durable.workflow`, and the mock raises them too. Every
  other Temporal error, a missing namespace included, propagates unchanged.
- Schedules need a real Temporal server (not the time-skipping test env). Pass
  the same `workflow_configs` to the lifecycle step to bootstrap them.
- Worker-side helpers (`ExecutionContextInterceptor`, `sandboxed_workflow_runner`,
  `TemporalSaga`) are exported for the worker process.
- **The namespace is the trust boundary.** `ExecutionContextInterceptor` binds the
  principal, its actor chain and the tenant from `Forze-*` headers as sent, so anyone with
  write access to the Temporal namespace — able to start, signal or update a workflow — can
  run it under any identity. Grant that access accordingly. Execution and correlation ids
  are tracing context: the worker mints its own execution id, records the caller's as the
  causation, and drops a malformed one.

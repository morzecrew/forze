---
title: Temporal
icon: lucide/workflow
summary: Durable workflows, schedules, and signals on Temporal
---

`forze[temporal]` implements the durable-workflow contracts on
[Temporal](https://temporal.io) — starting, signalling, querying, and scheduling
long-running workflows behind a stable port.

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

## What it provides

| Contract | Keyed by |
|----------|----------|
| Durable workflow command / query (start, signal, update, query, result, cancel) | `DurableWorkflowSpec.name` |
| Durable workflow schedule command / query | `DurableWorkflowSpec.name` (same workflow route) |
| Raw client | `TemporalClientDepKey` |

## Notes

- **Workers are separate.** The command port talks to the cluster; you still run
  workers that poll the configured task queue — the route's `queue` must match
  what the worker polls.
- The cluster host is the lifecycle step's `host=`; `TemporalConfig` carries
  `namespace` and interceptors.
- Schedules need a real Temporal server (not the time-skipping test env). Pass
  the same `workflow_configs` to the lifecycle step to bootstrap them.
- Worker-side helpers (`ExecutionContextInterceptor`, `sandboxed_workflow_runner`,
  `TemporalSaga`) are exported for the worker process.

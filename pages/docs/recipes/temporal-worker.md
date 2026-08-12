---
title: Run a Temporal worker
icon: lucide/hard-hat
summary: A worker process that starts, drains, and restarts like every other Forze plane
---

Your workflows and activities need a process polling their task queue. That process
is not a Forze concept — it is a Temporal worker, written against the SDK — but
*starting and stopping it* is, and getting shutdown wrong costs you every activity
that happened to be running at deploy time.

`temporal_worker_lifecycle_step` runs the worker as a lifecycle citizen: supervised
crash restart, and a drain that lets in-flight activities land before teardown.

## The entrypoint

A worker process is the client step, the worker step, and a probe listener so
Kubernetes can see it:

```python
import asyncio

from forze.application.execution import build_runtime
from forze_temporal import (
    TemporalClient,
    TemporalConfig,
    TemporalDepsModule,
    temporal_lifecycle_step,
    temporal_worker_lifecycle_step,
)

from myapp.workflows import FulfilOrder, charge_card, reserve_stock

temporal = TemporalClient()

runtime = build_runtime(
    TemporalDepsModule(client=temporal, workflows=WORKFLOW_CONFIGS),
    lifecycle_steps=[
        temporal_lifecycle_step(host="temporal:7233", config=TemporalConfig()),
        temporal_worker_lifecycle_step(
            client=temporal,
            task_queue="orders-tq",
            workflows=[FulfilOrder],
            activities=[reserve_stock, charge_card],
        ),
    ],
)


async def main() -> None:
    async with runtime.scope():
        await asyncio.Event().wait()  # run until the process is signalled


if __name__ == "__main__":
    asyncio.run(main())
```

Order matters: the worker step reaches for the client's live connection at startup,
so it must come **after** the step that connects it. Wired the other way round, the
process fails at boot rather than restarting a worker that can never reach a server.

## What you get for free

The worker rides the framework client's connection, so it inherits everything
configured there:

- the data converter — including payload encryption, if you turned it on
- every client interceptor that is also a worker interceptor, which is how
  `ExecutionContextInterceptor` gives activities the caller's identity, tenant and
  correlation id without appearing in the wiring above

That is also why there is no `interceptors=` argument on the step. One interceptor
on the client covers both sides in the same process.

## Shutdown

On shutdown the step stops polling and gives in-flight activities
`graceful_shutdown` (10 s by default) to finish. The SDK's own default is **zero** —
activities are cancelled the instant the worker stops — so a hand-rolled worker pays
for every deploy in retried work.

That window is spent out of the runtime's own drain budget, `shutdown_step_timeout`
(also 10 s), which every background loop in the process shares. Raising
`graceful_shutdown` alone buys nothing: the shared deadline still fires first, and when
it does it cuts *every* loop's shutdown short, not just this one. Raise both together —
`shutdown_step_timeout` is a runtime field rather than a `build_runtime` argument, so
evolve it:

```python
from datetime import timedelta

import attrs

runtime = attrs.evolve(runtime, shutdown_step_timeout=timedelta(seconds=45))
```

Then keep the pair under your orchestrator's termination grace period, so the pod is not
killed mid-drain.

## When a worker dies

A crashed worker is rebuilt after a jittered backoff, on the same connection. Set
`max_consecutive_crashes` to give up instead of restarting forever:

```python
temporal_worker_lifecycle_step(
    client=temporal,
    task_queue="orders-tq",
    workflows=[FulfilOrder],
    activities=[reserve_stock, charge_card],
    max_concurrent_activities=50,
    restart_backoff=timedelta(seconds=5),
    max_consecutive_crashes=10,
)
```

Ten short-lived runs in a row means the fault is not transient — better a process
that exits loudly than one that hot-loops a critical log until someone notices.

## Notes

- **One step runs one worker on one queue.** How many workers, which queues exist,
  and how they are deployed stay yours; register a second step for a second queue.
- **Synchronous activities need an executor.** The SDK refuses to build a worker with a
  plain `def` activity unless one is given, so pass `activity_executor=ThreadPoolExecutor(…)`
  and size it alongside `max_concurrent_activities`.
- A tenant-routed client cannot back a worker — it resolves its connection from the
  calling scope's tenant, and a worker polls a queue. Run one worker process per cluster.
- Add `probe_listener_step` so the orchestrator can see the process — it serves
  `/livez` and `/readyz` from the same runtime state, and reports `draining` for the
  whole shutdown window. See
  [Shutdown & fleets](../running-in-prod/shutdown-and-fleets.md).
- Workflow and activity *authoring* is raw `temporalio` and always will be — see
  [Temporal](../integrations/temporal.md) for what the framework does and does not
  own.
- Under a `SERVERLESS` deployment profile this step fails runtime assembly by
  design: a function that freezes between invocations cannot host a poller.
- Long activities should heartbeat so a dead worker is detected in seconds rather
  than at the timeout — see
  [heartbeats](../integrations/temporal.md#activity-heartbeats).

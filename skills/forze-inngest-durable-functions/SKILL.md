---
name: forze-inngest-durable-functions
description: >-
  Wires Forze durable function contracts with DurableFunctionEventSpec,
  DurableFunctionSpec, InngestDepsModule, event emit, function registration,
  DurableFunctionStepDepKey, and FastAPI serve. Use for event-driven durable
  handlers with Inngest in applications using forze[inngest].
---

# Forze Inngest durable functions

Use when your application emits events from HTTP handlers and runs memoized steps in Inngest workers. Install `forze[inngest]` (and `forze[inngest,fastapi]` for `serve`). Contracts live in `forze.application.contracts.durable.function`; wire them with `forze_inngest`.

## Event spec

```python
from forze.application.contracts.durable.function import DurableFunctionEventSpec
from forze.base.serialization import PydanticModelCodec

invoice_paid = DurableFunctionEventSpec(
    name="app/invoice.paid",
    codec=PydanticModelCodec(model_type=InvoicePaidPayload),
)
```

## Function spec

```python
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)

on_invoice_paid = DurableFunctionSpec(
    name="on-invoice-paid",
    run=DurableFunctionInvokeSpec(args_type=InvoicePaidPayload, return_type=Result),
    triggers=(DurableFunctionEventTrigger(event="app/invoice.paid"),),
)
```

## Runtime wiring (API)

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_inngest import InngestClient, InngestDepsModule, InngestEventConfig, inngest_lifecycle_step

client = InngestClient(app_id="my-app")
module = InngestDepsModule(
    client=client,
    events={invoice_paid.name: InngestEventConfig()},
)

deps = DepsRegistry.from_modules(module)
lifecycle = LifecyclePlan.from_steps(inngest_lifecycle_step())
```

## Emit events

There is no `ctx.durable_function_event(...)`. Use `resolve_configurable` with
`route=spec.name`:

```python
port = ctx.deps.resolve_configurable(
    ctx,
    DurableFunctionEventCommandDepKey,
    invoice_paid,
    route=invoice_paid.name,
)
await port.send(InvoicePaidPayload(invoice_id="inv-1"))
```

## Worker: register and serve

**Canonical (frozen registry operation):** set `operation` on the spec and bind the
same `frozen_registry` as HTTP routes.

```python
from forze_inngest import InngestFunctionBinding, register_functions
from forze_inngest.fastapi import serve

scan_spec = DurableFunctionSpec(
    name="scan-inbox",
    operation="jobs.scan_inbox",
    run=DurableFunctionInvokeSpec(args_type=CronTickArgs, return_type=None),
    triggers=(DurableFunctionCronTrigger(expression="0 */3 * * *"),),
)

binding = InngestFunctionBinding.for_registry_operation(scan_spec, frozen_registry)

register_functions(client, [binding], ctx_factory=make_execution_context)
serve(app, client, [binding], ctx_factory=make_execution_context)
```

**Escape hatch:** `handler_factory` when `operation` is unset (custom handler, not a
registry op).

```python
binding = InngestFunctionBinding(
    spec=on_invoice_paid,
    handler_factory=lambda ctx: OnInvoicePaidHandler(deps=ctx.deps),
)
```

## Steps

Inside the function handler only:

```python
step = ctx.deps.provide(DurableFunctionStepDepKey)
await step.run("notify", lambda: notifier.send(...))
```

## Self-hosted alternative: durable functions on Postgres

When the deployment runs only Postgres, the same durable-function form (memoized steps + crash recovery + cron schedules) works with **no external engine**. Wire `PostgresDepsModule(durable_step=PostgresDurableStepConfig(relation=...), durable_run=PostgresDurableRunConfig(relation=...), durable_schedule=...)`, then drive it with the `forze_kits.integrations.durable` runner:

```python
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_recovery_background_lifecycle_step,
    resolve_durable_step,
)

registry = DurableFunctionRegistry()
registry.register("fulfil-order", fulfil_order)  # async (ctx, input) -> output
runner = DurableFunctionRunner(registry=registry)
await runner.enqueue(ctx, "fulfil-order", {"order_id": str(order_id)})
```

Inside a registered function, `step = resolve_durable_step(ctx)` gives the same memoized `step.run(...)` port. Add `durable_recovery_background_lifecycle_step(runner=runner)` so crashed runs are re-claimed (`FOR UPDATE SKIP LOCKED`, multi-worker-safe) and replayed from the journal; `DurableScheduler` + `durable_scheduler_background_lifecycle_step(scheduler=scheduler, specs=[...])` fires cron-triggered specs. Setting `admin=True` on `PostgresDurableRunConfig` also registers a read-only `DurableRunAdminPort` (`list_runs`) for run inspection. Step results journal as **JSON** — keep step inputs/outputs JSON-serializable and step bodies idempotent. See [Durable execution](https://morzecrew.github.io/forze/latest/data-events/durable-execution/).

## Anti-patterns

- Do not use `DurableWorkflow*` types or `forze_temporal` in Inngest-based apps for the same work — pick Temporal **or** Inngest per use case.
- Do not call `DurableFunctionStepDepKey` from ordinary HTTP handlers.
- Do not expect a workflow-style `start()` command port — Inngest runs are event- or cron-driven.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Inngest integration](https://morzecrew.github.io/forze/latest/integrations/inngest/)
- [Durable execution](https://morzecrew.github.io/forze/latest/data-events/durable-execution/)
- [Durable function contracts](https://morzecrew.github.io/forze/latest/reference/contracts/durable/)
- [`forze-wiring`](../forze-wiring/SKILL.md)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)

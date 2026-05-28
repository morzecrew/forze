# Inngest Integration

## Page opening

`forze_inngest` connects Forze [durable function](../core-package/contracts/durable-function.md)
contracts to [Inngest](https://www.inngest.com/). HTTP handlers emit events through
`DurableFunctionEventCommandPort`; a worker process serves registered functions with
memoized steps via `DurableFunctionStepPort`.

| Topic | Details |
|------|---------|
| What it provides | `InngestClient`, `InngestDepsModule`, event command adapters, function registration, FastAPI `serve`. |
| Supported Forze contracts | `DurableFunctionEventCommandDepKey`, `DurableFunctionStepDepKey`, plus `InngestClientDepKey`. |
| When to use it | Event-driven durable handlers, cron-triggered jobs, and step memoization without Temporal workflows. |

## Installation

```bash
uv add 'forze[inngest]'
```

For FastAPI serve helpers:

```bash
uv add 'forze[inngest,fastapi]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `inngest` installs the Inngest Python SDK. |
| Local development | [Inngest Dev Server](https://www.inngest.com/docs/local-development) (`inngest dev`) or Inngest Cloud. |
| Environment | `INNGEST_DEV`, `INNGEST_EVENT_KEY`, `INNGEST_SIGNING_KEY`, `INNGEST_BASE_URL`, `INNGEST_SERVE_ORIGIN` per Inngest docs. |
| Integration tests | Docker + `inngest/inngest` testcontainer (`tests/integration/test_forze_inngest_integration/`). The app binds to `0.0.0.0`; the dev server container uses `host.docker.internal:host-gateway` to invoke it. |

## Minimal setup

### Client

```python
from forze_inngest import InngestClient, InngestConfig

client = InngestClient(
    app_id="my-app",
    config=InngestConfig(is_production=False),
)
```

### Deps module (emit events from API)

```python
from forze.application.execution import DepsPlan
from forze_inngest import InngestDepsModule

inngest_module = InngestDepsModule(
    client=client,
    events={
        "app/invoice.paid": {},
    },
)

deps_plan = DepsPlan.from_modules(inngest_module)
```

Route keys must match `DurableFunctionEventSpec.name`.

### Emit from a handler

```python
events = ctx.deps.resolve_configurable(
    ctx,
    DurableFunctionEventCommandDepKey,
    invoice_paid_spec,
    route=invoice_paid_spec.name,
)
await events.send(InvoicePaidPayload(invoice_id="inv-1"))
```

When `include_execution_context` is enabled (default), invocation metadata and identity
are embedded under `_forze` in the event payload and restored in the worker.

### Register functions and serve (worker)

**Preferred (registry-backed):** set `operation` on `DurableFunctionSpec` and pass the
same frozen `OperationRegistry` used for HTTP. Cron/event runs resolve the full operation
plan (middleware, transactions, dispatch).

```python
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze_inngest import InngestFunctionBinding, register_functions
from forze_inngest.fastapi import serve

scan_spec = DurableFunctionSpec(
    name="scan-inbox",
    operation="jobs.scan_inbox",
    run=DurableFunctionInvokeSpec(args_type=CronTickArgs, return_type=None),
    triggers=(DurableFunctionCronTrigger(expression="0 */3 * * *"),),
)

binding = InngestFunctionBinding.for_registry_operation(scan_spec, frozen_registry)

functions = register_functions(
    client,
    [binding],
    ctx_factory=make_execution_context,
)
# Or pass registry once for many bindings:
# register_functions(client, bindings, ctx_factory=..., registry=frozen_registry)

serve(app, client, [binding], ctx_factory=make_execution_context)
```

**Escape hatch:** custom handler when the function is not a registry operation:

```python
binding = InngestFunctionBinding(
    spec=my_function_spec,
    handler_factory=lambda ctx: MyHandler(deps=ctx.deps),
)
```

Store bindings on the deps module when you want a single wiring site:

```python
InngestDepsModule(
    client=client,
    events={...},
    function_bindings=[binding],
)
```

Use `get_function_bindings(module)` in your worker bootstrap.

### Lifecycle

```python
from forze.application.execution import LifecyclePlan
from forze_inngest import inngest_lifecycle_step

lifecycle = LifecyclePlan.from_steps(inngest_lifecycle_step())
```

Startup verifies `InngestClientDepKey` is registered; shutdown is a no-op.

## Steps inside handlers

Resolve `DurableFunctionStepDepKey` only inside a registered function run:

```python
step = ctx.deps.provide(DurableFunctionStepDepKey)
await step.run("charge", lambda: payment_port.charge(...))
```

Outside a function run, the port raises a precondition error.

## Related pages

- [Durable function contracts](../core-package/contracts/durable-function.md)
- [Temporal integration](temporal.md) — long-running workflow orchestration
- Agent skill: `forze-inngest-durable-functions`

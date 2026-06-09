---
title: Inngest
icon: lucide/workflow
summary: Durable functions and events on Inngest
---

`forze[inngest]` implements the durable-function contracts on
[Inngest](https://inngest.com) — emit events that trigger functions, and run
memoized steps inside them, behind the durable ports.

## Install

```bash
uv add 'forze[inngest]'
```

Needs Inngest, and a served function endpoint. The FastAPI serve helper needs
`forze[inngest,fastapi]`.

## The client

```python
from forze_inngest import InngestClient, InngestConfig

inngest = InngestClient(app_id="orders", config=InngestConfig())
```

`RoutedInngestClient` resolves per-tenant credentials.

## Wire it

Register the events you emit and bind your functions to operations:

```python
from forze.application.execution import DepsRegistry
from forze_inngest import InngestDepsModule, InngestEventConfig, InngestFunctionBinding

bindings = [InngestFunctionBinding.for_registry_operation(fulfil_spec, registry)]

deps = DepsRegistry.from_modules(
    InngestDepsModule(client=inngest, events={"orders": InngestEventConfig()}, function_bindings=bindings),
)
```

Serve the functions from FastAPI (registers them with Inngest):

```python
from forze_inngest.fastapi import serve

serve(app, inngest, bindings, ctx_factory=lambda req: runtime.get_context(), registry=registry)
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Durable function event command (emit) | `DurableFunctionEventSpec.name` (`events`) |
| Durable function step (memoized steps) | resolved inside a function run |

## Notes

- A function binding maps a `DurableFunctionSpec` to either an operation
  (`for_registry_operation`) or a handler factory — set exactly one.
- The execution-context metadata travels in a `_forze` envelope and is restored
  in the worker, so functions run with the right identity/tenant.
- Steps resolve only inside a running function.

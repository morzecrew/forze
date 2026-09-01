---
title: Inngest
icon: lucide/workflow
summary: Durable functions and events on Inngest
---

`forze[inngest]` implements the durable-function contracts on
[Inngest](https://inngest.com) — emit events that trigger functions, and run
memoized steps inside them, behind the durable ports.

## What this package is

The same boundary discipline as [Temporal](temporal.md#what-this-package-is):
connection and credentials, what rides on the wire, what crosses into execution
context, registration and lifecycle. Function *authoring* is the Inngest SDK's, and
stays that way.

Here the SDK forces that boundary rather than merely permitting it — its step object is
handed to your function, so there is no equivalent of Temporal's escape hatch to add:
the SDK's own objects are already in your hands. Parity between the two packages means
the same discipline, not the same feature list.

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

### Settings

`InngestSettings` holds the two keys — one to send events, one to verify that an inbound
invocation really came from Inngest — as separate secrets. See
[connection settings](index.md#connection-settings).

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

## The serve process

Inngest's execution model inverts Temporal's: instead of a worker polling a queue, the
platform calls **you**. So the canonical process here is an ordinary Forze HTTP app —
runtime, FastAPI, `serve(...)` on the same router — and there is no worker step,
because there is no poller to supervise.

What that costs you is the drain story a poller gets for free: an in-flight function
call is an in-flight HTTP request, so it drains with your web server, and the platform
retries a step whose request the deploy cut short. Give the app a termination grace
period longer than your slowest step for the same reason a worker gets a graceful
shutdown window.

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

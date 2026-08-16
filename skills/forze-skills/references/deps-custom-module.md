# Custom deps modules

Writing your own `DepsModule` and `DepKey` for an integration Forze does not ship. The advanced case — start with [dependency resolution](deps-resolution.md), which covers the built-in modules that handle most applications.

## Container model

| Mode | Shape | Use when |
|------|-------|----------|
| `Deps.plain({DepKey: value})` | one provider per key | shared clients |
| `Deps.routed({DepKey: {route: value}})` | one provider per key + route | specs resolved by `spec.name` |
| `Deps.routed_group({...}, routes={...})` | same provider for many routes | one backend, many logical resources |

`Deps.merge(...)` raises `CoreException` on conflicts — fix duplicate keys or routes in your module composition.

## Module shape

Route keys are `str | StrEnum` (`StrKey`), so shared application enums stay type-safe as map keys.

```python
from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps, DepsModule


WidgetClientDepKey = DepKey[WidgetClientPort]("widget_client")
WidgetDepKey = DepKey[WidgetDepPort]("widget")


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class WidgetDepsModule(DepsModule):
    client: WidgetClientPort
    widgets: Mapping[str | StrEnum, WidgetConfig] | None = None

    def __call__(self) -> Deps:
        plain = Deps.plain({WidgetClientDepKey: self.client})

        if not self.widgets:
            return plain

        routed = Deps.routed(
            {
                WidgetDepKey: {
                    name: ConfigurableWidget(config=config)
                    for name, config in self.widgets.items()
                }
            }
        )
        return plain.merge(routed)
```

Register the module with `DepsRegistry.from_modules(WidgetDepsModule(...), ...)`.

## Dep factories

Spec-backed adapters usually register factories like:

```python
from forze.application.execution import ExecutionContext

def __call__(self, ctx: ExecutionContext, spec: WidgetSpec) -> WidgetPort:
    return WidgetAdapter(
        client=ctx.deps.provide(WidgetClientDepKey),
        spec=spec,
        config=self.config,
    )
```

Handlers resolve with `route=spec.name` via `ctx.deps.resolve_configurable(ctx, WidgetDepKey, spec, route=spec.name)` unless a convenience helper exists on `ExecutionContext`.

## Lifecycle stays separate

`DepsModule.__call__` builds providers only. Open connections in `LifecycleStep` functions and add them to `LifecyclePlan` alongside other integration steps.

## Tenant-aware clients

Register a shared client as a plain dep; routed factories pick tenant-specific connections from `ExecutionContext` at call time.

When you build on a shipped **routed** client (`RoutedPostgresClient`, `RoutedNeo4jClient`, …) and need a backend call the routed facade does not wrap, use its public `client_scope()` seam rather than reaching for internals:

```python
async with routed_client.client_scope() as client:
    await client.do_something_unwrapped()
```

It resolves the ambient tenant, refreshes the access fingerprint so credential rotation is detected, and yields the pooled client — in both registry modes. A `guarded=True` client also holds an eviction lease for the scope, so a concurrent rotation disposes the client only after you exit; that is what keeps a multi-statement transaction on one client instead of swapping it mid-scope.

For a **rotating credential**, keep the fingerprint a stable identity (a role or account name, never the short-lived secret) and fetch the live token through a client callback — a fingerprint that changes on every rotation churns the pool.

## Anti-patterns

- **Instantiating adapters directly in handlers** — register factories and resolve ports.
- **Using only raw strings for new routes** — prefer shared `StrEnum` values for route keys.
- **Opening connections in `DepsModule.__call__`** — use lifecycle steps.
- **Overlapping keys from multiple custom modules** — merge configs before constructing modules or use distinct routes.
- **Plain deps for multi-spec keys** — use routed deps keyed by `spec.name`.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)

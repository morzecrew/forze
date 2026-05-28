---
name: forze-custom-deps
description: >-
  Authors custom DepKey and DepsModule implementations in an application when
  a private integration is not covered by shipped forze_* packages. Use for
  Neo4j graph adapters, Redis pub/sub maps, or other app-specific backends.
---

# Forze custom dependency modules

Use when your **application** needs a private integration that no shipped `forze_*` package provides (for example graph databases, Redis pub/sub routes, or an internal HTTP client). For everyday wiring with `PostgresDepsModule`, `S3DepsModule`, and similar, use [`forze-deps-consumption`](../forze-deps-consumption/SKILL.md) and [`forze-wiring`](../forze-wiring/SKILL.md).

## Container model

| Mode | Shape | Use when |
|------|-------|----------|
| `Deps.plain({DepKey: value})` | one provider per key | shared clients |
| `Deps.routed({DepKey: {route: value}})` | one provider per key + route | specs resolved by `spec.name` |
| `Deps.routed_group({...}, routes={...})` | same provider for many routes | one backend, many logical resources |

`Deps.merge(...)` raises `CoreError` on conflicts — fix duplicate keys or routes in your module composition.

## Module shape

Keep modules generic over `K: str | StrEnum` so routes stay type-safe with your application enums.

```python
from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.base import DepKey
from forze.application.execution import Deps, DepsModule


WidgetClientDepKey = DepKey[WidgetClientPort]("widget_client")
WidgetDepKey = DepKey[WidgetDepPort]("widget")


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class WidgetDepsModule[K: str | StrEnum](DepsModule[K]):
    client: WidgetClientPort
    widgets: Mapping[K, WidgetConfig] | None = None

    def __call__(self) -> Deps[K]:
        plain = Deps[K].plain({WidgetClientDepKey: self.client})
        routed = Deps[K]()

        if self.widgets:
            routed = Deps[K].routed(
                {
                    WidgetDepKey: {
                        name: ConfigurableWidget(config=config)
                        for name, config in self.widgets.items()
                    }
                }
            )

        return plain.merge(routed)
```

Register the module with `DepsPlan.from_modules(WidgetDepsModule(...), ...)`.

## Dep factories

Spec-backed adapters usually register factories like:

```python
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

## Anti-patterns

1. **Instantiating adapters directly in handlers** — register factories and resolve ports.
2. **Using only raw strings for new routes** — prefer `K: str | StrEnum` on the module.
3. **Opening connections in `DepsModule.__call__`** — use lifecycle steps.
4. **Overlapping keys from multiple custom modules** — merge configs before constructing modules or use distinct routes.
5. **Plain deps for multi-spec keys** — use routed deps keyed by `spec.name`.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/docs/reference/execution/)
- [`forze-deps-consumption`](../forze-deps-consumption/SKILL.md)
- [`forze-graph-contracts`](../forze-graph-contracts/SKILL.md) (graph ports + custom module)
- [`forze-messaging-streaming`](../forze-messaging-streaming/SKILL.md) (Redis pub/sub/stream custom wiring)

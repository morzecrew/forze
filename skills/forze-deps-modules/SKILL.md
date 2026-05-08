---
name: forze-deps-modules
description: >-
  Designs and reviews Forze dependency keys, Deps containers, routed/plain
  registrations, lifecycle steps, and custom DepsModule implementations. Use
  when authoring adapters, adding integration modules, or debugging dependency
  resolution and StrEnum route wiring.
---

# Forze dependency modules

Use when authoring or reviewing infrastructure wiring. For application bootstrap, see [`forze-wiring`](forze-wiring/SKILL.md). For logical names and `StrEnum` route values, see [`forze-specs-infrastructure`](forze-specs-infrastructure/SKILL.md).

## Container model

`Deps` has two registration modes:

| Mode | Shape | Use when |
|------|-------|----------|
| `Deps.plain({DepKey: value})` | one provider per key | shared clients, secrets, idempotency defaults |
| `Deps.routed({DepKey: {route: value}})` | one provider per key + route | specs resolved by `spec.name` |
| `Deps.routed_group({...}, routes={...})` | same provider for many routes | one backend supports several logical resources |

`Deps.merge(...)` raises `CoreError` on duplicate plain keys, plain-vs-routed conflicts, or duplicate routed keys. Let it fail fast instead of silently overriding providers.

## Module shape

Built-in modules are generic over `K: str | StrEnum`. Follow that pattern so callers can use `StrEnum` names.

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

## Dep factories

Most spec-backed adapters register factories shaped like:

```python
def __call__(self, ctx: ExecutionContext, spec: WidgetSpec) -> WidgetPort:
    return WidgetAdapter(client=ctx.dep(WidgetClientDepKey), spec=spec, config=self.config)
```

Application code resolves the factory with `route=spec.name`, then calls it with `(ctx, spec)`. `ExecutionContext` convenience methods do this internally for documents, search, cache, counters, storage, locks, and embeddings.

## Lifecycle stays separate

Keep `DepsModule.__call__` pure and cheap: it builds providers, not network connections. Initialize pools and clients in `LifecycleStep` functions (`postgres_lifecycle_step`, `redis_lifecycle_step`, `s3_lifecycle_step`, etc.).

## Routed infrastructure

For tenant-aware clients, register a structural client port as a plain dependency and let routed clients choose the concrete connection at call time. For routed Postgres clients, pass `introspector_cache_partition_key` when catalog metadata differs by tenant/database.

## Anti-patterns

1. **Instantiating adapters directly in usecases** — use deps factories and ports.
2. **Using only strings in new modules** — keep route type as `K: str | StrEnum`.
3. **Opening connections in `__call__`** — lifecycle owns startup/shutdown.
4. **Returning overlapping keys from multiple modules** — compose maps before module construction or use distinct routes.
5. **Registering spec-backed providers as plain deps when multiple specs exist** — use routed deps keyed by `spec.name`.

## Reference

- [`src/forze/application/execution/deps.py`](../../src/forze/application/execution/deps.py)
- [`src/forze/application/contracts/base/deps.py`](../../src/forze/application/contracts/base/deps.py)
- [`src/forze_postgres/execution/deps/module.py`](../../src/forze_postgres/execution/deps/module.py)
- [`src/forze_redis/execution/deps/module.py`](../../src/forze_redis/execution/deps/module.py)

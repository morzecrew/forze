---
name: forze-deps-consumption
description: >-
  Explains how Forze resolves dependencies in applications: plain vs routed
  Deps, route=spec.name, built-in DepsModule composition, and merge conflicts.
  Use when debugging handler resolution or wiring Postgres, Redis, S3, and
  other shipped integration modules.
---

# Forze dependency consumption

Use when wiring or debugging how your application resolves ports from `ExecutionContext`. For bootstrap and `DepsPlan`, see [`forze-wiring`](../forze-wiring/SKILL.md). For mapping logical spec names to integration configs, see [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md). For a private integration not covered by shipped `forze_*` packages, see [`forze-custom-deps`](../forze-custom-deps/SKILL.md).

## Plain vs routed

`Deps` registers providers in two shapes:

| Mode | Shape | Use when |
|------|-------|----------|
| `Deps.plain({DepKey: value})` | one provider per key | shared clients, secrets, idempotency defaults |
| `Deps.routed({DepKey: {route: value}})` | one provider per key + route | specs resolved by `spec.name` |

Shipped modules such as `PostgresDepsModule`, `S3DepsModule`, and `InngestDepsModule` return merged `Deps` containers. Your app passes them to `DepsPlan.from_modules(...)`.

## How handlers resolve ports

1. You define a logical spec (`DocumentSpec`, `QueueSpec`, …) with a stable `name` (prefer a shared `StrEnum`).
2. The integration module maps that `name` to physical config (table, bucket, queue URL, …).
3. At runtime, `ExecutionContext` resolves the factory with `route=spec.name`, then builds the port.

Convenience helpers (`ctx.document.query`, `ctx.storage`, …) do this internally. For other dep keys:

```python
port = ctx.deps.resolve_configurable(
    ctx, QueueCommandDepKey, order_queue, route=order_queue.name
)
```

## Merge conflicts

`Deps.merge(...)` raises `CoreError` on duplicate plain keys, plain-vs-routed conflicts, or duplicate routed keys. Treat that as a wiring bug: two modules registered the same key/route, or you merged overlapping maps twice.

## Lifecycle vs deps

Built-in `DepsModule.__call__` should only register providers. Connection pools and clients start via `LifecycleModule` or `LifecycleStep` factories on your `LifecyclePlan` (`PostgresLifecycleModule`, `postgres_lifecycle_step`, `s3_lifecycle_step`, …). Keep deps and lifecycle as separate plans. See [`forze-wiring`](../forze-wiring/SKILL.md).

## Anti-patterns

1. **Instantiating integration adapters in handlers** — resolve ports from `ExecutionContext`.
2. **Mismatching spec `name` and deps-module route keys** — use one shared `StrEnum` for both.
3. **Calling `resolve_configurable` without `route=spec.name`** for spec-backed keys.
4. **Opening network connections inside handler code** — use lifecycle steps and registered clients.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/docs/reference/execution/)
- [Specs and wiring](https://morzecrew.github.io/forze/docs/concepts/specs-and-wiring/)
- [`forze-wiring`](../forze-wiring/SKILL.md)
- [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md)

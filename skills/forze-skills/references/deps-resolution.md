# Dependency resolution

How a handler gets a port: plain versus routed deps, why `route=spec.name` matters, and how to read a merge conflict between modules. The wiring that registers them is [runtime and lifecycle](runtime-lifecycle.md).

## Plain vs routed

`Deps` registers providers in two shapes:

| Mode | Shape | Use when |
|------|-------|----------|
| `Deps.plain({DepKey: value})` | one provider per key | shared clients, secrets, idempotency defaults |
| `Deps.routed({DepKey: {route: value}})` | one provider per key + route | specs resolved by `spec.name` |

Shipped modules such as `PostgresDepsModule`, `S3DepsModule`, and `InngestDepsModule` return merged `Deps` containers. Your app passes them to `DepsRegistry.from_modules(...)`.

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

`Deps.merge(...)` raises `CoreException` on duplicate plain keys, plain-vs-routed conflicts, or duplicate routed keys. Treat that as a wiring bug: two modules registered the same key/route, or you merged overlapping maps twice.

## Lifecycle vs deps

Built-in `DepsModule.__call__` should only register providers. Connection pools and clients start via `LifecycleModule` or `LifecycleStep` factories on your `LifecyclePlan` (`PostgresLifecycleModule`, `postgres_lifecycle_step`, `s3_lifecycle_step`, …). Keep deps and lifecycle as separate plans, **freeze both**, then pass the frozen registries to `ExecutionRuntime`. See [runtime and lifecycle](runtime-lifecycle.md).

## Anti-patterns

- **Instantiating integration adapters in handlers** — resolve ports from `ExecutionContext`.
- **Mismatching spec `name` and deps-module route keys** — use one shared `StrEnum` for both.
- **Calling `resolve_configurable` without `route=spec.name`** for spec-backed keys.
- **Opening network connections inside handler code** — use lifecycle steps and registered clients.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)

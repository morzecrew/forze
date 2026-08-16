# Spec naming and routes

## Prefer `StrEnum` names

Use a shared `StrEnum` for spec names and dependency routes. `BaseSpec.name` and built-in deps modules accept `str | StrEnum`, so enum values keep application specs, deps-module maps, and transaction routes aligned during refactors.

```python
from enum import StrEnum


class ResourceName(StrEnum):
    PROJECTS = "projects"
    PROJECT_ATTACHMENTS = "project-attachments"
    ORDERS = "orders"


class TxRoute(StrEnum):
    DEFAULT = "default"
```

Use enum members consistently for:

- `DocumentSpec(name=...)`, `SearchSpec(name=...)`, `CacheSpec(name=...)`, `CounterSpec(name=...)`, …
- keys in `PostgresDepsModule.rw_documents` / `ro_documents` / `searches`, `MongoDepsModule` maps, `RedisDepsModule.caches` / `counters` / `dlocks`, `S3DepsModule.storages`, `SQSDepsModule.queue_readers` / `queue_writers`, `RabbitMQDepsModule.queue_readers` / `queue_writers`, `TemporalDepsModule.workflows`, etc.
- transaction route sets such as `PostgresDepsModule(tx={TxRoute.DEFAULT})`

`ExecutionContext` resolves routed factories using `spec.name` as the route.

## Transaction routes

Register routes on the backend module (e.g. `PostgresDepsModule(tx={TxRoute.DEFAULT})`). Application code uses `async with ctx.tx_ctx.scope(TxRoute.DEFAULT):` and `registry.bind(...).bind_tx().set_route(TxRoute.DEFAULT).finish(deep=True).freeze()`.

## Gotchas

- Mismatch between `spec.name` and infra dict keys is a frequent wiring bug — check the spec enum and deps-module map when debugging “dependency not registered”.
- Do not mix plain strings and enum members casually in new code. Equality works by value, but shared enums make missing routes easier to catch in review.
- Enable `history_enabled` on the **spec** when you want history semantics; the **relation** still comes from infra (`history` on Postgres/Mongo config).
- `S3DepsModule(client=...)` / `GCSDepsModule(client=...)`, `SQSDepsModule(client=...)`, and `TemporalDepsModule(client=...)` register only client keys unless their routed maps are populated — for object storage the `storages={...}` map is required for **both** S3 and GCS.

## Anti-patterns

- **Scattering literal spec names** — put resource names in a shared `StrEnum` and reuse it in specs and deps modules.
- **Naming physical infrastructure in application code.** A handler that mentions a table, bucket, queue or workflow-queue name has bypassed the spec. The logical `StrEnum` member is the only name application code should know; every physical name belongs in the deps module's routed map, where changing it is a wiring edit rather than a search-and-replace through handlers.
- **Reusing one `StrEnum` member across planes that do not share a backend.** One name is one route. Pointing a `DocumentSpec` and an unrelated `StorageSpec` at the same member couples two migrations that have no reason to move together, and makes "which config does this name mean?" a question about the reader's memory.
- **Duplicating literal route strings** — use shared `StrEnum` values for spec names and transaction routes.

## Reference

- [Specs and wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)

# Contracts

Contracts are the protocol interfaces and lightweight specifications that define
what application code needs from infrastructure. Usecases resolve them through
`ExecutionContext`, while integration packages register implementations under
well-known dependency keys.

## Choose the right contract

| Need | Contract domain | Start with | Typical implementations | Related keys |
|------|-----------------|------------|-------------------------|--------------|
| Read or mutate aggregate-backed documents | [Document](contracts/document.md) | `DocumentSpec`, `DocumentQueryPort`, `DocumentCommandPort` | Mock, Postgres, Mongo | `DocumentQueryDepKey`, `DocumentCommandDepKey` |
| Cache JSON-like values or document projections | [Cache](contracts/cache.md) | `CacheSpec`, `CachePort` | Mock, Redis / Valkey | `CacheDepKey` |
| Send work to one consumer or worker pool | [Queue](contracts/queue.md) | `QueueSpec`, `QueueQueryPort`, `QueueCommandPort` | Mock, SQS, RabbitMQ | `QueueQueryDepKey`, `QueueCommandDepKey` |
| Broadcast messages to subscribers | [Pub/Sub](contracts/pubsub.md) | `PubSubSpec`, `PubSubCommandPort`, `PubSubQueryPort` | Mock, Redis / Valkey | `PubSubCommandDepKey`, `PubSubQueryDepKey` |
| Append ordered events and consume by stream or group | [Stream](contracts/stream.md) | `StreamSpec`, stream ports | Mock, Redis / Valkey | `StreamQueryDepKey`, `StreamCommandDepKey`, `StreamGroupQueryDepKey` |
| Store and retrieve binary objects | [Storage](contracts/storage.md) | `StorageSpec`, `StoragePort` | Mock, S3-compatible storage | `StorageDepKey` |
| Start and interact with long-running processes | [Workflow](contracts/workflow.md) | `WorkflowSpec`, workflow ports | Temporal | `WorkflowCommandDepKey`, `WorkflowQueryDepKey` |
| Replay duplicate HTTP-style requests safely | [Idempotency](contracts/idempotency.md) | `IdempotencySpec`, `IdempotencyPort` | Mock, Redis / Valkey | `IdempotencyDepKey` |

## Contract pattern

Each infrastructure concern usually follows the same shape:

| Component | Role | Example |
|-----------|------|---------|
| **Spec** | Declarative runtime configuration owned by the application layer | `DocumentSpec[R, D, C, U]` |
| **Port** | Protocol interface consumed by usecases and coordinators | `DocumentQueryPort[R]` |
| **Value type** | TypedDict or attrs object returned by a port | `QueueMessage[M]` |
| **DepPort** | Factory protocol that builds a port from `ExecutionContext` and a spec | `QueueCommandDepPort` |
| **DepKey** | Typed container key used when registering factories | `QueueCommandDepKey` |

Specs carry logical names. Integration modules map those names to physical
resources such as tables, collections, buckets, streams, queues, or workflow
task queues.

## Resolving ports

Prefer `ExecutionContext` convenience methods when available:

    :::python
    doc_q = ctx.doc_query(project_spec)
    doc_c = ctx.doc_command(project_spec)
    cache = ctx.cache(cache_spec)
    storage = ctx.storage(storage_spec)

For domains without a convenience helper, resolve the factory by dependency key
and call it with `(ctx, spec)`:

    :::python
    from forze.application.contracts.queue import QueueCommandDepKey

    queue = ctx.dep(QueueCommandDepKey)(ctx, order_queue_spec)
    message_id = await queue.enqueue("orders", payload)

## Common supporting contracts

- `DepKey[T]` (`forze.application.contracts.base`) identifies a dependency in a
  `Deps` container. Its `name` is used in diagnostics; the type parameter carries
  static type information.
- `DepsPort[K]` (`forze.application.contracts.base`) exposes `provide`, `exists`,
  `merge`, `without`, `without_route`, `empty`, and `count` for dependency
  containers. Routed registrations are selected with `provide(key, route=...)`.
- `BaseSpec` (`forze.application.contracts.base`) provides the shared `name` field
  used by resource specs.

## Related pages

- [Contracts and Adapters](../concepts/contracts-adapters.md)
- [Specs and Wiring](../concepts/specs-and-wiring.md)
- [Execution](execution.md)
- [Integrations](../integrations/mock.md)

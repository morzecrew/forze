# Contracts

Contracts are the protocol interfaces that define what the application needs from infrastructure. Each contract is a Python `Protocol` class — adapters implement them, and usecases consume them through `ExecutionContext`. For the architectural rationale, see [Contracts and Adapters](../core-concepts/contracts-adapters.md). This page is the complete API reference.

## Structure of a contract

Every infrastructure concern follows the same pattern:

| Component | Role | Example |
|-----------|------|---------|
| **Port** | Protocol interface defining operations | `DocumentQueryPort[R]` |
| **Spec** | Declarative configuration for the concern | `DocumentSpec[R, D, C, U]` |
| **DepKey** | Typed key for dependency registration | `DocumentQueryDepKey` |
| **DepPort** | Factory protocol that builds a port from context | `DocumentQueryDepPort` |
| **Routed deps** | Same dep key, multiple providers keyed by `spec.name` | `Deps.routed({...})` |

Ports are resolved at runtime via `ExecutionContext`, never imported directly from adapter packages.

## Dependencies and keys

### DepKey

A typed key that identifies a dependency in the container:

    :::python
    from forze.application.contracts.base import DepKey

    MyServiceKey = DepKey[MyService]("my_service")

The `name` parameter is used in error messages and diagnostics. The type parameter `T` carries static type information for safe resolution.

### DepsPort

Protocol for a dependency container:

| Method | Purpose |
|--------|---------|
| `provide(key)` | Return the dependency registered under `key` |
| `exists(key)` | Check if a dependency is registered |
| `merge(*deps)` | Combine multiple containers (raises on key conflicts) |
| `without(key)` | Return a new container without the given key |
| `empty()` | Check if the container has no dependencies |

Routed dependency selection is built into `Deps` itself and resolved via `provide(key, route=...)`.

## Document storage

Documents are the primary data abstraction. Ports are split into read and write for CQRS flexibility.

### DocumentQueryPort[R]

Read-only operations for document aggregates:

| Method | Signature | Returns |
|--------|-----------|---------|
| `get` | `(pk, *, for_update?, return_fields?)` | `R` or `JsonDict` |
| `get_many` | `(pks, *, return_fields?)` | `Sequence[R]` or `Sequence[JsonDict]` |
| `find` | `(filters, *, for_update?, return_fields?)` | `R \| None` or `JsonDict \| None` |
| `find_many` | `(filters?, limit?, offset?, sorts?, *, return_fields?)` | `(list[R], int)` or `(list[JsonDict], int)` |
| `count` | `(filters?)` | `int` |

When `return_fields` is provided, methods return `JsonDict` projections instead of typed models. `for_update` locks the row when the backend supports it.

### DocumentCommandPort[R, D, C, U]

Mutation operations for document aggregates:

| Method | Signature | Returns |
|--------|-----------|---------|
| `create` | `(dto)` | `R` |
| `create_many` | `(dtos)` | `Sequence[R]` |
| `update` | `(pk, dto, *, rev?)` | `R` |
| `update_many` | `(pks, dtos, *, revs?)` | `Sequence[R]` |
| `touch` / `touch_many` | `(pk)` / `(pks)` | `R` / `Sequence[R]` |
| `kill` / `kill_many` | `(pk)` / `(pks)` | `None` |
| `delete` / `delete_many` | `(pk, *, rev?)` / `(pks, *, revs?)` | `R` / `Sequence[R]` |
| `restore` / `restore_many` | `(pk, *, rev?)` / `(pks, *, revs?)` | `R` / `Sequence[R]` |

The optional `rev` parameter enables optimistic concurrency control. When provided, the adapter checks that the current revision matches before applying the change.

### DocumentSpec

Kernel specification: model types, logical `name`, optional `history_enabled`, optional `CacheSpec`. **Physical tables and collections** are configured in `PostgresDepsModule` / `MongoDepsModule` (see [Specs and infrastructure wiring](../core-concepts/specs-and-wiring.md)).

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec

    spec = DocumentSpec(
        name="projects",
        read=ProjectRead,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        history_enabled=True,
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `str` | Logical route — matches infra config keys |
| `read` | `type[R]` | Read model (`ReadDocument`) |
| `write` | `DocumentWriteTypes \| None` | Domain + commands, or `None` for read-only |
| `history_enabled` | `bool` | Whether history is active when infra provides it |
| `cache` | `CacheSpec \| None` | Enables read-through cache on query/command ports |

Helper methods:

- `supports_soft_delete()` — `True` when the domain model inherits from `SoftDeletionMixin`
- `supports_update()` — `True` when the update command has writable fields

### Dependency keys

| Key | Resolved via |
|-----|--------------|
| `DocumentQueryDepKey` | `ctx.doc_query(spec)` |
| `DocumentCommandDepKey` | `ctx.doc_command(spec)` |

## Transaction management

### TxManagerPort

Manages transaction boundaries:

| Method | Purpose |
|--------|---------|
| `transaction()` | Return an async context manager for a transaction scope |
| `scope_key()` | Return the `TxScopeKey` identifying this tx manager kind |

### TxScopedPort

Marker protocol for ports that are bound to a specific transaction scope. The execution context validates that scoped ports match the active transaction.

### TxHandle

Value object holding the active transaction's scope key. Used internally to detect scope mismatches.

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `TxManagerDepKey` | `ctx.txmanager()` |

## Cache

### CacheSpec

    :::python
    from forze.application.contracts.cache import CacheSpec

    cache_spec = CacheSpec(name="projects", ttl=timedelta(minutes=10))

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `str` | Route to `RedisDepsModule.caches[name]` (or other cache backend) |
| `ttl` | `timedelta` | Default time-to-live for entries |
| `ttl_pointer` | `timedelta` | TTL for version pointer keys when using versioned cache |

### CachePort

Combines read and write operations:

| Method | Purpose |
|--------|---------|
| `get(pk)` | Retrieve a cached document |
| `get_many(pks)` | Retrieve multiple cached documents |
| `set(pk, data)` | Store a document in cache |
| `invalidate(pk)` | Remove a document from cache |
| `invalidate_many(pks)` | Remove multiple documents |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `CacheDepKey` | `ctx.cache(spec)` |

## Counter

### CounterPort

Namespace-scoped atomic counters:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `incr` | `(suffix?, by?)` | Increment and return new value |
| `incr_batch` | `(count, suffix?)` | Increment by count, return final value |
| `decr` | `(suffix?, by?)` | Decrement and return new value |
| `reset` | `(suffix?, value?)` | Reset to a specific value |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `CounterDepKey` | `ctx.counter(CounterSpec(...))` |

## Search

### SearchSpec

    :::python
    from forze.application.contracts.search import SearchSpec

    search_spec = SearchSpec(
        name="projects",
        model_type=ProjectReadModel,
        fields=("title", "description"),
        default_weights={"title": 0.6, "description": 0.4},
    )

Postgres index and heap names belong in `PostgresDepsModule.searches[name]` (`PostgresSearchConfig`), not on the kernel spec.

| Field | Purpose |
|-------|---------|
| `name` | Logical route — matches `PostgresSearchConfig` registration |
| `model_type` | Pydantic model for typed hits |
| `fields` | Indexed field names (unique) |
| `default_weights` | Optional per-field weights (must cover all `fields` if set) |
| `fuzzy` | Optional `SearchFuzzySpec` |

### SearchQueryPort[R]

| Method | Purpose |
|--------|---------|
| `search(query, filters?, limit?, offset?, sorts?, *, options?, return_model?, return_fields?)` | Full-text search with optional filters and pagination |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `SearchQueryDepKey` | `ctx.search_query(spec)` |

## Object storage

### StoragePort

S3-style blob storage:

| Method | Signature | Returns |
|--------|-----------|---------|
| `upload` | `(filename, data, description?, *, prefix?)` | `StoredObject` |
| `download` | `(key)` | `DownloadedObject` |
| `delete` | `(key)` | `None` |
| `list` | `(limit, offset, *, prefix?)` | `(list[ObjectMetadata], int)` |

### Storage types

| Type | Fields |
|------|--------|
| `StoredObject` | `key`, `filename`, `content_type`, `size` |
| `ObjectMetadata` | `key`, `filename`, `content_type`, `size`, `last_modified` |
| `DownloadedObject` | `key`, `filename`, `content_type`, `size`, `data` |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `StorageDepKey` | `ctx.storage(StorageSpec(name=...))` |

## Queue

### QueueSpec

    :::python
    from forze.application.contracts.queue import QueueSpec

    order_queue = QueueSpec(name="orders", model=OrderPayload)

### QueueReadPort[M]

| Method | Purpose |
|--------|---------|
| `receive(queue, *, limit?, timeout?)` | Receive a batch of messages |
| `consume(queue, *, timeout?)` | Async iterator over messages |
| `ack(queue, ids)` | Acknowledge processed messages |
| `nack(queue, ids, *, requeue?)` | Reject messages, optionally re-queue |

### QueueWritePort[M]

| Method | Purpose |
|--------|---------|
| `enqueue(queue, payload, *, type?, key?, enqueued_at?)` | Send a single message |
| `enqueue_many(queue, payloads, *, type?, key?, enqueued_at?)` | Send a batch |

### QueueMessage[M]

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `str` | Message identifier |
| `payload` | `M` | Deserialized message body |
| `type` | `str \| None` | Optional message type |
| `key` | `str \| None` | Optional routing key |
| `enqueued_at` | `datetime \| None` | Enqueue timestamp |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `QueueReadDepKey` | Read port |
| `QueueWriteDepKey` | Write port |

## Pub/Sub

### PubSubSpec

    :::python
    from forze.application.contracts.pubsub import PubSubSpec

    events_spec = PubSubSpec(name="events", model=EventPayload)

### PubSubPublishPort[M]

| Method | Purpose |
|--------|---------|
| `publish(topic, payload, *, type?, key?, published_at?)` | Publish a message |

### PubSubSubscribePort[M]

| Method | Purpose |
|--------|---------|
| `subscribe(topics, *, timeout?)` | Async iterator over messages |

### PubSubMessage[M]

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `str` | Message identifier |
| `topic` | `str` | Topic the message was published to |
| `payload` | `M` | Deserialized message body |
| `type` | `str \| None` | Optional message type |
| `key` | `str \| None` | Optional routing key |
| `published_at` | `datetime \| None` | Publish timestamp |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `PubSubPublishDepKey` | Publish port |
| `PubSubSubscribeDepKey` | Subscribe port |

## Stream

### StreamSpec

    :::python
    from forze.application.contracts.stream import StreamSpec

    audit_stream = StreamSpec(name="audit", model=AuditEntry)

### StreamReadPort[M]

| Method | Purpose |
|--------|---------|
| `read(stream_mapping, *, limit?, timeout?)` | Read entries from streams |
| `tail(stream_mapping, *, timeout?)` | Async iterator following new entries |

### StreamGroupPort[M]

| Method | Purpose |
|--------|---------|
| `read(group, consumer, stream_mapping, *, limit?, timeout?)` | Consumer group read |
| `tail(group, consumer, stream_mapping, *, timeout?)` | Consumer group tail |
| `ack(group, stream, ids)` | Acknowledge entries |

### StreamWritePort[M]

| Method | Purpose |
|--------|---------|
| `append(stream, payload, *, type?, key?, timestamp?)` | Append an entry |

### StreamMessage[M]

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `str` | Entry identifier |
| `stream` | `str` | Stream name |
| `payload` | `M` | Deserialized entry body |
| `type` | `str \| None` | Optional entry type |
| `key` | `str \| None` | Optional routing key |
| `timestamp` | `datetime \| None` | Entry timestamp |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `StreamReadDepKey` | Read port |
| `StreamWriteDepKey` | Write port |
| `StreamGroupDepKey` | Group port |

## Idempotency

### IdempotencyPort

Deduplicate operations by caching responses keyed by operation name, idempotency key, and payload hash:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `begin` | `(op, key, payload_hash)` | Check for a cached response; returns `IdempotencySnapshot \| None` |
| `commit` | `(op, key, payload_hash, snapshot)` | Store the response for future dedup |

### IdempotencySnapshot

| Field | Type | Purpose |
|-------|------|---------|
| `status_code` | `int` | HTTP status code |
| `body` | `bytes` | Serialized response body |
| `headers` | `dict[str, str]` | Response headers |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `IdempotencyDepKey` | Idempotency port |

## Workflow

Workflows are typed with **`WorkflowSpec`** (logical **`name`**, **`run`** invocation, optional **`signals`**, **`queries`**, **`updates`**). Ports are split between commands and queries.

### WorkflowCommandPort

| Method | Purpose |
|--------|---------|
| `start(args, *, workflow_id?, raise_on_already_started?)` | Start a run; returns **`WorkflowHandle`** |
| `signal(handle, *, signal, args)` | Send a signal |
| `update(handle, *, update, args)` | Run a workflow update |
| `cancel(handle)` | Request cancellation |
| `terminate(handle, *, reason?)` | Terminate the run |

### WorkflowQueryPort

| Method | Purpose |
|--------|---------|
| `query(handle, *, query, args)` | Run a query |
| `result(handle)` | Await the workflow result |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `WorkflowCommandDepKey` | Routed factory → **`WorkflowCommandPort`** (route = **`WorkflowSpec.name`**) |
| `WorkflowQueryDepKey` | Routed factory → **`WorkflowQueryPort`** (route = **`WorkflowSpec.name`**) |

## Context handling

Execution identity is represented by `CallContext` and `PrincipalContext` on `ExecutionContext`.
`PrincipalContext` contains optional `tenant_id` and `actor_id`, bound at the boundary via `ctx.bind_call(...)`.

## Resolving ports

All ports are resolved through `ExecutionContext`. Contracts with convenience methods:

    :::python
    from forze.application.contracts.counter import CounterSpec
    from forze.application.contracts.storage import StorageSpec

    doc_q = ctx.doc_query(project_spec)
    doc_c = ctx.doc_command(project_spec)
    cache = ctx.cache(cache_spec)
    counter = ctx.counter(CounterSpec(name="tickets"))
    storage = ctx.storage(StorageSpec(name="attachments"))
    search = ctx.search_query(search_spec)
    tx = ctx.txmanager("default")

For contracts without a convenience method, use `dep()` with the dep key:

    :::python
    from forze.application.contracts.pubsub import PubSubPublishDepKey

    publisher = ctx.dep(PubSubPublishDepKey)(ctx, events_spec)
    await publisher.publish("events.created", payload)

# Contracts

Contracts are the protocol interfaces that define what the application needs from infrastructure. Each contract is a Python `Protocol` class — adapters implement them, and usecases consume them through `ExecutionContext`. For the architectural rationale, see [Contracts and Adapters](../core-concepts/contracts-adapters.md). This page is the complete API reference.

## Structure of a contract

Every infrastructure concern follows the same pattern:

| Component | Role | Example |
|-----------|------|---------|
| **Port** | Protocol interface defining operations | `DocumentReadPort[R]` |
| **Spec** | Declarative configuration for the concern | `DocumentSpec[R, D, C, U]` |
| **DepKey** | Typed key for dependency registration | `DocumentReadDepKey` |
| **DepPort** | Factory protocol that builds a port from context | `DocumentReadDepPort` |
| **DepRouter** | Router that selects a provider by spec | `DocumentReadDepRouter` |

Ports are resolved at runtime via `ExecutionContext`, never imported directly from adapter packages.

## Dependencies and keys

### DepKey

A typed key that identifies a dependency in the container:

    :::python
    from forze.application.contracts.deps import DepKey

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

### DepRouter

Generic router that selects a dependency provider based on a spec. Used by integration modules to map multiple adapters (e.g. Postgres for some specs, Mongo for others) under a single dep key.

## Document storage

Documents are the primary data abstraction. Ports are split into read and write for CQRS flexibility.

### DocumentReadPort[R]

Read-only operations for document aggregates:

| Method | Signature | Returns |
|--------|-----------|---------|
| `get` | `(pk, *, for_update?, return_fields?)` | `R` or `JsonDict` |
| `get_many` | `(pks, *, return_fields?)` | `Sequence[R]` or `Sequence[JsonDict]` |
| `find` | `(filters, *, for_update?, return_fields?)` | `R \| None` or `JsonDict \| None` |
| `find_many` | `(filters?, limit?, offset?, sorts?, *, return_fields?)` | `(list[R], int)` or `(list[JsonDict], int)` |
| `count` | `(filters?)` | `int` |

When `return_fields` is provided, methods return `JsonDict` projections instead of typed models. `for_update` locks the row when the backend supports it.

### DocumentWritePort[R, D, C, U]

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

Declarative specification that binds a document aggregate to its storage, cache, and history:

    :::python
    from forze.application.contracts.document import DocumentSpec

    spec = DocumentSpec(
        namespace="projects",
        read={"source": "public.projects", "model": ProjectRead},
        write={
            "source": "public.projects",
            "models": {
                "domain": Project,
                "create_cmd": CreateProjectCmd,
                "update_cmd": UpdateProjectCmd,
            },
        },
        history={"source": "public.projects_history"},
        cache={"enabled": True},
    )

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `namespace` | `str` | Yes | Logical name and cache key prefix |
| `read` | `DocumentReadSpec[R]` | Yes | Source relation and read model type |
| `write` | `DocumentWriteSpec[D, C, U]` | No | Source relation and write model types |
| `history` | `DocumentHistorySpec` | No | Source relation for revision audit trail |
| `cache` | `DocumentCacheSpec` | No | Cache configuration (enable flag, TTL) |

Helper methods:

- `supports_soft_delete()` — `True` when the domain model inherits from `SoftDeletionMixin`
- `supports_update()` — `True` when the update command has writable fields

### Dependency keys

| Key | Type | Resolved via |
|-----|------|-------------|
| `DocumentReadDepKey` | `DepKey[DocumentReadDepPort]` | `ctx.doc_read(spec)` |
| `DocumentWriteDepKey` | `DepKey[DocumentWriteDepPort]` | `ctx.doc_write(spec)` |

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

    cache_spec = CacheSpec(namespace="projects", ttl=timedelta(minutes=10))

| Field | Type | Purpose |
|-------|------|---------|
| `namespace` | `str` | Cache key namespace |
| `ttl` | `timedelta` | Default time-to-live for entries |

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
| `CounterDepKey` | `ctx.counter(namespace)` |

## Search

### SearchSpec

    :::python
    from forze.application.contracts.search import SearchSpec

    search_spec = SearchSpec(
        namespace="projects",
        model=ProjectRead,
        indexes={
            "idx_title": {
                "source": "public.projects",
                "fields": [{"path": "title"}],
            },
        },
        default_index="idx_title",
    )

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `namespace` | `str` | Yes | Logical search domain name |
| `model` | `type[BaseModel]` | Yes | Result model for typed search |
| `indexes` | `dict[str, SearchIndexSpec]` | Yes | Index name to configuration |
| `default_index` | `str` | No | Default index when not specified |

### SearchIndexSpec

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `fields` | `list[SearchFieldSpec]` | Yes | Fields included in the index |
| `source` | `str` | No | Source relation |
| `groups` | `list[SearchGroupSpec]` | No | Weight groups for ranking |
| `default_group` | `str` | No | Default weight group |
| `fuzzy` | `SearchFuzzySpec` | No | Fuzzy search parameters |

### SearchReadPort[R]

| Method | Purpose |
|--------|---------|
| `search(query, filters?, limit?, offset?, sorts?, *, options?, return_model?, return_fields?)` | Full-text search with optional filters and pagination |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `SearchReadDepKey` | `ctx.search(spec)` |

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
| `StorageDepKey` | `ctx.storage(bucket)` |

## Queue

### QueueSpec

    :::python
    from forze.application.contracts.queue import QueueSpec

    order_queue = QueueSpec(namespace="orders", model=OrderPayload)

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

    events_spec = PubSubSpec(namespace="events", model=EventPayload)

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

    audit_stream = StreamSpec(namespace="audit", model=AuditEntry)

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

### WorkflowPort

Orchestrate long-running processes:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `start` | `(name, id, args, queue?)` | Start a workflow instance |
| `signal` | `(id, signal, data)` | Send a signal to a running workflow |

## Context ports

### ActorContextPort

Ambient actor identity for audit trails:

| Method | Purpose |
|--------|---------|
| `get()` | Return current actor UUID |
| `set(actor_id)` | Bind actor for the current context |

### TenantContextPort

Ambient tenant identity for multi-tenant routing:

| Method | Purpose |
|--------|---------|
| `get()` | Return current tenant UUID |
| `set(tenant_id)` | Bind tenant for the current context |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `TenantContextDepKey` | Tenant context port |

## Resolving ports

All ports are resolved through `ExecutionContext`. Contracts with convenience methods:

    :::python
    doc_read  = ctx.doc_read(project_spec)
    doc_write = ctx.doc_write(project_spec)
    cache     = ctx.cache(cache_spec)
    counter   = ctx.counter("tickets")
    storage   = ctx.storage("attachments")
    search    = ctx.search(search_spec)
    tx        = ctx.txmanager()

For contracts without a convenience method, use `dep()` with the dep key:

    :::python
    from forze.application.contracts.pubsub import PubSubPublishDepKey

    publisher = ctx.dep(PubSubPublishDepKey)(ctx, events_spec)
    await publisher.publish("events.created", payload)

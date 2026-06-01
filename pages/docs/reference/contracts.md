# Contracts

Contracts are the protocol interfaces that define what the application needs from infrastructure. Each contract is a Python `Protocol` class — adapters implement them, and handlers consume them through `ExecutionContext`. For the architectural rationale, see [Contracts and Adapters](../concepts/contracts-adapters.md). This page is the complete API reference.

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

Read-only operations for document aggregates. Result shape is selected by the method name (for example `find_page` includes a total count; `find_many` does not). Projections use `project*`; arbitrary row models use `select*` with an explicit `return_type`.

| Method group | Typical signature | Returns |
|--------------|-------------------|---------|
| `get` | `(pk, *, for_update?, skip_cache?)` | `R` |
| `get_many` | `(pks, *, skip_cache?)` | `Sequence[R]` |
| `find` | `(filters, *, for_update?)` | `R \| None` |
| `project` | `(filters, fields, *, for_update?)` | `JsonDict \| None` |
| `select` | `(filters, return_type, *, for_update?)` | `T \| None` |
| `find_many` / `find_page` | `(filters?, pagination?, sorts?)` | `CountlessPage[R]` / `Page[R]` |
| `project_many` / `project_page` | `(fields, filters?, pagination?, sorts?)` | `CountlessPage[JsonDict]` / `Page[JsonDict]` |
| `select_many` / `select_page` | `(return_type, filters?, pagination?, sorts?)` | `CountlessPage[T]` / `Page[T]` |
| `find_cursor` / `project_cursor` / `select_cursor` | `(filters?, cursor?, sorts?)` / `(fields, ...)` / `(return_type, ...)` | `CursorPage[R]` / `CursorPage[JsonDict]` / `CursorPage[T]` |
| `find_stream` / `project_stream` / `select_stream` | `(filters?, *, sorts?, chunk_size=500)` / `(fields, ...)` / `(return_type, ...)` | `AsyncIterator[Sequence[R]]` / `AsyncIterator[Sequence[JsonDict]]` / `AsyncIterator[Sequence[T]]` |
| `aggregate_many` / `aggregate_page` | `(aggregates, filters?, pagination?, sorts?)` | `CountlessPage[JsonDict]` / `Page[JsonDict]` |
| `select_many_aggregated` / `select_page_aggregated` | `(return_type, aggregates, ...)` | `CountlessPage[T]` / `Page[T]` |
| `count` | `(filters?)` | `int` |

`for_update` uses :data:`~forze.application.contracts.document.RowLockMode` (`False`, `True`, `"nowait"`, or `"skip_locked"`) to lock rows when the backend supports it. Postgres applies full SQL lock semantics; Mongo, Firestore, and Mock backends treat any non-`False` mode as a transactional read and **degrade** `"nowait"` / `"skip_locked"` to `True` (with a debug log).

Stream methods export rows in keyset chunks via repeated internal cursor pages (default `chunk_size` 500, clamped 10–20 000). They do not support `for_update` or aggregates.

`DocumentAdapter` (integrations layer) adds safety limits on internal scan/stream loops: `max_scan_pages`, `max_stream_pages`, and `max_chunked_command_pages` default to **100 000**; set any of them to `None` for unlimited export. Non-advancing cursor tokens raise an internal error instead of looping forever. Routed tenant pools (`TenantClientRegistry` with `guarded=True`) must not call `use()` for the same tenant from that tenant's `create` callback.

### DocumentCommandPort[R, D, C, U]

Mutation operations for document aggregates:

| Method | Signature | Returns |
|--------|-----------|---------|
| `create` | `(dto)` | `R` |
| `create_many` | `(dtos)` | `Sequence[R]` |
| `update` | `(pk, rev, dto)` | `R` |
| `update_many` | `(updates)` | `Sequence[R]` |
| `touch` / `touch_many` | `(pk)` / `(pks)` | `R` / `Sequence[R]` |
| `kill` / `kill_many` | `(pk)` / `(pks)` | `None` |
| `delete` / `delete_many` | `(pk, rev)` / `(deletes)` | `R` / `Sequence[R]` |
| `restore` / `restore_many` | `(pk, rev)` / `(restores)` | `R` / `Sequence[R]` |

Revision-bearing commands check that the current revision matches before applying the change. Batch `updates`, `deletes`, and `restores` are sequences of tuples that include the document id and expected revision.

#### Bulk updates

| Method | Mechanism | Domain `apply` | Per-row `rev` | Typical use |
|--------|-----------|----------------|---------------|-------------|
| `update` / `update_many` | per-row optimistic patch | yes | yes | business mutations |
| `update_matching_strict` | chunked `project_many` + `update_many` | yes | yes | bulk with same rules as `update` |
| `update_matching` | single/bulk store patch | **no** | **no** | admin flags, uniform SQL/`$set` |

`ensure` is insert-only on primary-key conflict (never mutates existing rows). `upsert` inserts or runs optimistic `update` on conflict (not a full document replace).

On Postgres, `ensure` / `upsert` use `ON CONFLICT` on `PostgresDocumentConfig.conflict_target` or the inferred write-table primary key (see [Postgres integration](../integrations/postgres.md#conflict_target-ensure--upsert)). On MongoDB, the same operations upsert on `_id` only (see [Mongo integration](../integrations/mongo.md#ensure--upsert-and-unique-indexes)).

Postgres `update_matching` uses `UPDATE … RETURNING`; the coordinator hydrates read models from returned domain rows when read and write share the same source. See [Postgres integration](../integrations/postgres.md) for `bookkeeping_strategy` (`"application"` vs `"database"` triggers).

### DocumentSpec

Kernel specification: model types, logical `name`, optional `history_enabled`, optional `CacheSpec`. **Physical tables and collections** are configured in `PostgresDepsModule` / `MongoDepsModule` (see [Specs and infrastructure wiring](../concepts/specs-and-wiring.md)).

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
| `cache` | `CacheSpec \| None` | When set, document dep factories resolve `ctx.cache(...)` while building query/command ports |

Helper methods:

- `supports_soft_delete()` — `True` when the domain model inherits from `forze_kits.domain.soft_deletion.SoftDeletionMixin`
- `supports_update()` — `True` when the update command has writable fields

### Dependency keys

| Key | Resolved via |
|-----|--------------|
| `DocumentQueryDepKey` | `ctx.document.query(spec)` (alias `ctx.doc.query`) |
| `DocumentCommandDepKey` | `ctx.document.command(spec)` (alias `ctx.doc.command`) |

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
| `TxManagerDepKey` | `ctx.tx_ctx.resolver()` |

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

## Analytics

### AnalyticsSpec

    :::python
    from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec

    metrics_spec = AnalyticsSpec(
        name="events",
        read=MetricRow,
        queries={"daily": AnalyticsQueryDefinition(params=DailyParams)},
        ingest=EventRow,
    )

Warehouse dataset/table names belong in future integration modules (e.g. `BigQueryDepsModule`), not on the kernel spec.

| Field | Purpose |
|-------|---------|
| `name` | Logical route for the analytics surface |
| `read` | Default Pydantic model for query rows |
| `queries` | Named queries (`query_key` → parameter model) |
| `ingest` | Optional append row model |

### AnalyticsQueryPort[R]

| Method | Purpose |
|--------|---------|
| `run` | Named query; `CountlessPage[R]` |
| `run_page` | Named query with total count when supported |
| `run_chunked` | Batch iterator for large scans |
| `project_run` / `project_run_page` / `project_run_chunked` | Projected `JsonDict` rows |
| `select_run` / … | Rows validated as `return_type` |
| `run_cursor` / `project_run_cursor` / `select_run_cursor` | Opaque cursor pagination |

### AnalyticsIngestPort[I]

| Method | Purpose |
|--------|---------|
| `append` | Append a batch of rows; returns `AnalyticsAppendResult` |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `AnalyticsQueryDepKey` | `ctx.analytics.query(spec)` |
| `AnalyticsIngestDepKey` | `ctx.analytics.ingest(spec)` |

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

Result shape and pagination mode are encoded in the method name:

| Method | Purpose |
|--------|---------|
| `search` | Typed `R` hits; offset pagination; no total count query |
| `search_page` | Typed `R` hits with total matching count |
| `project_search` / `project_search_page` | Projected `JsonDict` rows (`fields` required) |
| `select_search` / `select_search_page` | Hits validated as `return_type` |
| `search_cursor` / `project_search_cursor` / `select_search_cursor` | Keyset cursor pagination |

### Dependency keys

| Key | Resolved via |
|-----|-------------|
| `SearchQueryDepKey` | `ctx.search.query(spec)` |

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
    from forze.base.serialization import PydanticRecordMappingCodec

    order_queue = QueueSpec(
        name="orders",
        codec=PydanticRecordMappingCodec(OrderPayload),
    )

### QueueReadPort[M]

| Method | Purpose |
|--------|---------|
| `receive(queue, *, limit?, timeout?)` | Receive a batch of messages |
| `consume(queue, *, timeout?)` | Async iterator over messages |
| `ack(queue, ids)` | Acknowledge processed messages |
| `nack(queue, ids, *, requeue?)` | Reject messages, optionally re-queue |

### QueueCommandPort[M]

| Method | Purpose |
|--------|---------|
| `enqueue(queue, payload, *, type?, key?, enqueued_at?, delay?, not_before?)` | Send a single message |
| `enqueue_many(queue, payloads, *, type?, key?, enqueued_at?, delay?, not_before?)` | Send a batch (same delay applies to all) |

`enqueued_at` is message metadata. `delay` / `not_before` control visibility (mutually exclusive). See `resolve_delivery_delay` and `SQS_MAX_DELAY` in `forze.application.contracts.queue`.

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
| `QueueQueryDepKey` | Query port |
| `QueueCommandDepKey` | Command port |

## Pub/Sub

### PubSubSpec

    :::python
    from forze.application.contracts.pubsub import PubSubSpec
    from forze.base.serialization import PydanticRecordMappingCodec

    events_spec = PubSubSpec(
        name="events",
        codec=PydanticRecordMappingCodec(EventPayload),
    )

### PubSubCommandPort[M]

| Method | Purpose |
|--------|---------|
| `publish(topic, payload, *, type?, key?, published_at?)` | Publish a message |

### PubSubQueryPort[M]

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
| `PubSubCommandDepKey` | Command port |
| `PubSubQueryDepKey` | Query port |

## Stream

### StreamSpec

    :::python
    from forze.application.contracts.stream import StreamSpec
    from forze.base.serialization import PydanticRecordMappingCodec

    audit_stream = StreamSpec(
        name="audit",
        codec=PydanticRecordMappingCodec(AuditEntry),
    )

### StreamQueryPort[M]

| Method | Purpose |
|--------|---------|
| `read(stream_mapping, *, limit?, timeout?)` | Read entries from streams |
| `tail(stream_mapping, *, timeout?)` | Async iterator following new entries |

### StreamGroupQueryPort[M]

| Method | Purpose |
|--------|---------|
| `read(group, consumer, stream_mapping, *, limit?, timeout?)` | Consumer group read |
| `tail(group, consumer, stream_mapping, *, timeout?)` | Consumer group tail |
| `ack(group, stream, ids)` | Acknowledge entries |

### StreamCommandPort[M]

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
| `StreamQueryDepKey` | Query port |
| `StreamCommandDepKey` | Command port |
| `StreamGroupQueryDepKey` | Group query port |

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

## Durable workflow

Workflows are typed with **`DurableWorkflowSpec`** (logical **`name`**, **`run`** invocation, optional **`signals`**, **`queries`**, **`updates`**). Ports are split between commands and queries. See [Durable workflow contracts](../core-package/contracts/durable-workflow.md).

### DurableWorkflowCommandPort

| Method | Purpose |
|--------|---------|
| `start(args, *, workflow_id?, raise_on_already_started?)` | Start a run; returns **`DurableWorkflowHandle`** |
| `signal(handle, *, signal, args)` | Send a signal |
| `update(handle, *, update, args)` | Run a workflow update |
| `cancel(handle)` | Request cancellation |
| `terminate(handle, *, reason?)` | Terminate the run |

### DurableWorkflowQueryPort

| Method | Purpose |
|--------|---------|
| `describe(handle)` | Return **`DurableWorkflowRunDescription`** (coarse run status) |
| `query(handle, *, query, args)` | Run an app-defined workflow query |
| `result(handle)` | Await the workflow result |

### DurableWorkflowRunDescription

| Field | Purpose |
|-------|---------|
| `workflow_id`, `run_id`, `workflow_name` | Run identifiers |
| `status` | **`DurableWorkflowRunStatus`** (`running`, `completed`, `failed`, …) |
| `started_at`, `closed_at` | Timestamps when the provider supplies them |
| `failure_message`, `failure_type` | Optional failure detail |
| `is_terminal` | Property: true when the coarse status is terminal |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `DurableWorkflowCommandDepKey` | Routed factory → **`DurableWorkflowCommandPort`** (route = **`DurableWorkflowSpec.name`**) |
| `DurableWorkflowQueryDepKey` | Routed factory → **`DurableWorkflowQueryPort`** (route = **`DurableWorkflowSpec.name`**) |

## Durable workflow schedule

Schedule resources are typed with **`DurableWorkflowScheduleTiming`** and managed through schedule command/query ports (separate from run handles).

### DurableWorkflowScheduleCommandPort

| Method | Purpose |
|--------|---------|
| `create(schedule_id, args, timing, ...)` | Create a schedule |
| `upsert(...)` | Create or update |
| `update(handle, *, timing?, args?, ...)` | Partial update |
| `delete(handle)` | Delete the schedule |
| `pause(handle, *, note?)` / `unpause(handle, *, note?)` | Pause or resume |
| `trigger(handle)` | Fire immediately |

### DurableWorkflowScheduleQueryPort

| Method | Purpose |
|--------|---------|
| `describe(handle)` | Return **`DurableWorkflowScheduleDescription`** |
| `list(*, limit?, next_page_token?)` | Paginated schedules for this workflow |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `DurableWorkflowScheduleCommandDepKey` | Routed factory → **`DurableWorkflowScheduleCommandPort`** |
| `DurableWorkflowScheduleQueryDepKey` | Routed factory → **`DurableWorkflowScheduleQueryPort`** |

Declarative **`DurableWorkflowScheduleBootstrap`** entries on **`TemporalDepsModule`** are upserted on lifecycle startup when **`workflow_configs`** is passed to **`temporal_lifecycle_step`**.

## Durable function

Event emit and step execution for platform-managed functions. See [Durable function contracts](../core-package/contracts/durable-function.md).

### DurableFunctionEventCommandPort

| Method | Purpose |
|--------|---------|
| `send(payload, *, event_id?, occurred_at?)` | Emit an event; returns event id |

### DurableFunctionStepPort

| Method | Purpose |
|--------|---------|
| `run(step_id, fn)` | Run a memoized, retriable step |

### Dependency keys

| Key | Purpose |
|-----|---------|
| `DurableFunctionEventCommandDepKey` | Routed factory → **`DurableFunctionEventCommandPort`** |
| `DurableFunctionStepDepKey` | Simple factory → **`DurableFunctionStepPort`** |

## Context handling

Execution identity is represented by `InvocationMetadata`, optional `AuthnIdentity`, and optional `TenantIdentity` on `ExecutionContext`.
`AuthnPort` returns `AuthnResult` at the boundary, but only its principal-only `AuthnIdentity` is bound onto `ExecutionContext`; `TenantIdentity` carries the current `tenant_id`. Bind them via `ctx.inv_ctx.bind(metadata=..., authn=..., tenant=...)`.

The full authentication contract surface — `AuthnPort`, the verifier and resolver ports, `AuthnSpec`, `VerifiedAssertion`, all dep keys, the `forze_identity.authn` first-party stack, and `forze_identity.oidc` — is documented on the dedicated [Authentication contracts](authentication.md) page. `forze_identity.authz` provides catalog-backed RBAC helpers (`AuthzDepsModule`, policy principal and binding specs) on top of the same identity model. Both providers use regular document ports, so storage is selected by the existing document adapter wiring.

## Resolving ports

All ports are resolved through `ExecutionContext`. Contracts with convenience methods:

    :::python
    from forze.application.contracts.counter import CounterSpec
    from forze.application.contracts.storage import StorageSpec

    doc_q = ctx.document.query(project_spec)
    doc_c = ctx.document.command(project_spec)
    cache = ctx.cache(cache_spec)
    counter = ctx.counter(CounterSpec(name="tickets"))
    storage = ctx.storage(StorageSpec(name="attachments"))
    search = ctx.search.query(search_spec)
    tx = ctx.tx_ctx.resolver("default")

For contracts without a convenience method, use `dep()` with the dep key:

    :::python
    from forze.application.contracts.pubsub import PubSubCommandDepKey

    publisher = ctx.deps.resolve_configurable(ctx, PubSubCommandDepKey, events_spec, route=events_spec.name)
    await publisher.publish("events.created", payload)

# Contracts and Adapters

Forze follows **hexagonal architecture** (ports and adapters). The core idea is simple: the application layer declares *what* capabilities it needs through protocol interfaces (contracts), and infrastructure packages provide *how* those capabilities are implemented (adapters). The application never depends on a specific adapter.

## How it works

1. The application layer defines **contracts**: Python `Protocol` classes describing required capabilities
2. Infrastructure packages provide **adapters**: concrete implementations of those protocols
3. A **dependency plan** wires adapters to contracts at startup
4. Usecases resolve contracts from `ExecutionContext`; they never import adapter classes

Switching from Postgres to Mongo means changing the dependency plan, not the usecase code.

<div class="d2-diagram">
  <img class="d2-light" src="../../assets/diagrams/light/contracts-adapters.svg" alt="Contracts and adapters">
  <img class="d2-dark" src="../../assets/diagrams/dark/contracts-adapters.svg" alt="Contracts and adapters">
</div>

## Contract catalog

### Document storage

Split into read and write ports for CQRS flexibility:

**`DocumentReadPort[R]`**: read-only operations:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `get` | `(pk, *, for_update?, return_fields?) -> R \| JsonDict` | Fetch one document by ID |
| `get_many` | `(pks, *, return_fields?) -> Sequence[R] \| Sequence[JsonDict]` | Fetch multiple by IDs |
| `find` | `(filters, *, for_update?, return_fields?) -> R \| None` | Find one by filter |
| `find_many` | `(filters?, limit?, offset?, sorts?, *, return_fields?) -> (list[R], int)` | Paginated query |
| `count` | `(filters?) -> int` | Count matching documents |

**`DocumentWritePort[R, D, C, U]`**: mutation operations:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `create` | `(dto) -> R` | Create a document |
| `create_many` | `(dtos) -> Sequence[R]` | Batch create |
| `update` | `(pk, dto, *, rev?) -> R` | Partial update |
| `update_many` | `(pks, dtos, *, revs?) -> Sequence[R]` | Batch update |
| `touch` | `(pk) -> R` | Bump `last_update_at` only |
| `kill` | `(pk) -> None` | Hard delete |
| `delete` | `(pk, *, rev?) -> R` | Soft delete |
| `restore` | `(pk, *, rev?) -> R` | Restore from soft delete |

Both ports also have `*_many` batch variants for all applicable operations.

### Transaction management

**`TxManagerPort`**: manages transaction boundaries:

| Method | Purpose |
|--------|---------|
| `transaction()` | Returns an async context manager for a transaction scope |
| `scope_key()` | Returns the scope key identifying this tx manager kind |

**`TxScopedPort`**: marks a port as transaction-aware. The execution context validates that scoped ports match the active transaction kind.

### Cache

**`CachePort`**: document-level caching with read and write sub-protocols:

| Method | Purpose |
|--------|---------|
| `get(pk)` | Retrieve a cached document |
| `get_many(pks)` | Retrieve multiple cached documents |
| `set(pk, data)` | Store a document in cache |
| `invalidate(pk)` | Remove a document from cache |
| `invalidate_many(pks)` | Remove multiple documents from cache |

### Counter

**`CounterPort`**: namespace-scoped atomic counters:

| Method | Purpose |
|--------|---------|
| `incr(suffix?, by?)` | Increment and return new value |
| `incr_batch(count, suffix?)` | Increment by count and return final value |
| `decr(suffix?, by?)` | Decrement and return new value |
| `reset(suffix?, value?)` | Reset to a specific value |

### Search

**`SearchReadPort[R]`**: full-text search:

| Method | Purpose |
|--------|---------|
| `search(query, filters?, limit?, offset?, sorts?, *, options?, return_model?, return_fields?)` | Search with optional filters and pagination |

### Object storage

**`StoragePort`**: S3-style blob storage:

| Method | Purpose |
|--------|---------|
| `upload(filename, data, description?, *, prefix?)` | Upload an object |
| `download(key)` | Download an object |
| `delete(key)` | Delete an object |
| `list(limit, offset, *, prefix?)` | List objects with pagination |

### Queue (message queue)

**`QueueReadPort[M]`**: consume messages from a queue:

| Method | Purpose |
|--------|---------|
| `receive(queue, *, limit?, timeout?)` | Receive a batch of messages |
| `consume(queue, *, timeout?)` | Async iterator over messages |
| `ack(queue, ids)` | Acknowledge processed messages |
| `nack(queue, ids, *, requeue?)` | Reject messages, optionally re-queue |

**`QueueWritePort[M]`**: produce messages to a queue:

| Method | Purpose |
|--------|---------|
| `enqueue(queue, payload, *, type?, key?, enqueued_at?)` | Send a single message |
| `enqueue_many(queue, payloads, *, type?, key?, enqueued_at?)` | Send a batch |

### Pub/Sub

**`PubSubPublishPort[M]`**: publish to a topic:

| Method | Purpose |
|--------|---------|
| `publish(topic, payload, *, type?, key?, published_at?)` | Publish a message |

**`PubSubSubscribePort[M]`**: subscribe to topics:

| Method | Purpose |
|--------|---------|
| `subscribe(topics, *, timeout?)` | Async iterator over messages |

### Stream

**`StreamReadPort[M]`**: read from an append-only log:

| Method | Purpose |
|--------|---------|
| `read(stream_mapping, *, limit?, timeout?)` | Read entries from streams |
| `tail(stream_mapping, *, timeout?)` | Async iterator that follows new entries |

**`StreamGroupPort[M]`**: consumer group reads:

| Method | Purpose |
|--------|---------|
| `read(group, consumer, stream_mapping, *, limit?, timeout?)` | Group read |
| `tail(group, consumer, stream_mapping, *, timeout?)` | Group tail |
| `ack(group, stream, ids)` | Acknowledge entries |

**`StreamWritePort[M]`**: append to a stream:

| Method | Purpose |
|--------|---------|
| `append(stream, payload, *, type?, key?, timestamp?)` | Append an entry |

### Idempotency

**`IdempotencyPort`**: deduplicate HTTP requests:

| Method | Purpose |
|--------|---------|
| `begin(op, key, payload_hash)` | Check for cached response |
| `commit(op, key, payload_hash, snapshot)` | Store response for future dedup |

### Workflow

**`WorkflowPort`**: orchestrate long-running processes:

| Method | Purpose |
|--------|---------|
| `start(name, id, args, queue?)` | Start a workflow instance |
| `signal(id, signal, data)` | Send a signal to a running workflow |

### Context ports

**`TenantContextPort`**: ambient tenant identity for multi-tenant routing:

| Method | Purpose |
|--------|---------|
| `get()` | Return current tenant ID |
| `set(tenant_id)` | Bind tenant for the current context |

**`ActorContextPort`**: ambient actor identity for audit trails:

| Method | Purpose |
|--------|---------|
| `get()` | Return current actor ID |
| `set(actor_id)` | Bind actor for the current context |

## Dependency keys

Each contract has a corresponding `DepKey` for registration and resolution. Integration modules register adapters under these keys; the execution context resolves them.

    :::python
    doc = self.ctx.doc_read(project_spec)     # resolves DocumentReadDepKey
    cache = self.ctx.cache(cache_spec)         # resolves CacheDepKey
    counter = self.ctx.counter("tickets")      # resolves CounterDepKey
    storage = self.ctx.storage("attachments")  # resolves StorageDepKey

For contracts without convenience methods on `ExecutionContext`, use `dep()` directly:

    :::python
    from forze.application.contracts.pubsub import PubSubPublishDepKey

    publish = ctx.dep(PubSubPublishDepKey)(ctx, spec)

## Wiring adapters

Integration modules register their adapters at dependency plan build time:

    :::python
    from forze.application.execution import Deps, DepsPlan

    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            PostgresDepsModule(client=pg_client, ...)(),
            RedisDepsModule(client=redis_client)(),
            S3DepsModule(client=s3_client)(),
        ),
    )

`Deps.merge()` combines containers and raises `CoreError` if any key is registered twice. This catches misconfigured plans early.

## Testing

Tests stub contracts with in-memory or fake implementations. Build a `Deps` container with only the ports your test needs:

    :::python
    from forze.application.execution import Deps, ExecutionContext

    deps = Deps(deps={
        DocumentReadDepKey: lambda ctx, spec, cache=None: FakeDocumentReadAdapter(),
    })
    ctx = ExecutionContext(deps=deps)

    doc = ctx.doc_read(project_spec)
    result = await doc.get(some_uuid)

No real databases or external services are needed for unit testing business logic.

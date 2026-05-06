# Aggregate Specification

Specifications are the bridge between your domain models and infrastructure adapters. A **kernel spec** declares model types and logical `name`; **integration configs** (on `PostgresDepsModule`, `RedisDepsModule`, etc.) map that `name` to tables, Redis namespaces, and other physical details. See [Specs and infrastructure wiring](specs-and-wiring.md) for the full picture.

## DocumentSpec

`DocumentSpec` binds read/write model types to a logical resource. It inherits `name` from `BaseSpec`.

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec


    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        history_enabled=True,
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `str` | Logical id — must match keys in `PostgresDepsModule` / `MongoDepsModule` / `RedisDepsModule` maps |
| `read` | `type[R]` | `ReadDocument` subclass for query results |
| `write` | `DocumentWriteTypes \| None` | Domain + command types; `None` for read-only aggregates |
| `history_enabled` | `bool` | When `True`, infra may persist revision history if configured |
| `cache` | `CacheSpec \| None` | When set, the document adapter factory resolves `ctx.cache(...)` for read-through caching |

### Write types

`write` is a dict (see `DocumentWriteTypes`) with:

- `domain`: `Document` subclass
- `create_cmd`: `CreateDocumentCmd` subclass
- `update_cmd`: `BaseDTO` subclass for patches (empty model means no update support)

### Helper methods

- `supports_soft_delete()` — `True` when `write["domain"]` subclasses `SoftDeletionMixin`
- `supports_update()` — `True` when `update_cmd` has writable fields

Physical storage (Postgres tables, Mongo collections) is **not** on the spec — configure `PostgresDocumentConfig` / `MongoDocumentConfig` under the same `name`.

## SearchSpec

Search is separate from `DocumentSpec`. It describes indexed fields and result shape; Postgres index and heap relations go in `PostgresSearchConfig` under `SearchSpec.name`.

    :::python
    from forze.application.contracts.search import SearchSpec


    project_search_spec = SearchSpec(
        name="projects",
        model_type=ProjectReadModel,
        fields=("title", "description"),
        default_weights={"title": 0.6, "description": 0.4},
    )

### Fields

| Field | Purpose |
|-------|---------|
| `name` | Logical id — matches `PostgresDepsModule.searches` / other search wiring |
| `model_type` | Pydantic model for typed hits |
| `fields` | Non-empty sequence of searchable field names |
| `default_weights` | Optional per-field weights (0.0–1.0), must cover all `fields` if provided |
| `fuzzy` | Optional fuzzy matching (`SearchFuzzySpec`) |

## Other specs

### CacheSpec

    :::python
    from forze.application.contracts.cache import CacheSpec

    cache_spec = CacheSpec(name="projects", ttl=timedelta(minutes=10))

Used by `ctx.cache(spec)` and embedded in `DocumentSpec.cache`. The `name` must match a Redis cache route when using `RedisDepsModule.caches`.

### CounterSpec

    :::python
    from forze.application.contracts.counter import CounterSpec

    tickets = CounterSpec(name="tickets")

Resolved with `ctx.counter(tickets)`.

### QueueSpec, PubSubSpec, StreamSpec

Each subclasses `BaseSpec` with a `name` and a `model` type (Pydantic) for payloads.

    :::python
    from forze.application.contracts.queue import QueueSpec
    from forze.application.contracts.pubsub import PubSubSpec
    from forze.application.contracts.stream import StreamSpec

    order_queue = QueueSpec(name="orders", model=OrderPayload)
    events_pubsub = PubSubSpec(name="events", model=EventPayload)
    audit_stream = StreamSpec(name="audit", model=AuditEntry)

### StorageSpec

    :::python
    from forze.application.contracts.storage import StorageSpec

    attachments = StorageSpec(name="attachments")

## Resolving ports

Use `ExecutionContext` helpers — names on specs must match routed infra config:

    :::python
    doc_q = ctx.doc_query(project_spec)
    doc_c = ctx.doc_command(project_spec)
    search = ctx.search_query(project_search_spec)
    cache = ctx.cache(cache_spec)
    counter = ctx.counter(counter_spec)
    storage = ctx.storage(storage_spec)

For contracts without a helper, use `ctx.dep(SomeDepKey)(ctx, spec, ...)`.

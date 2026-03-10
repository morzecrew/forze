# Aggregate Specification

Specifications are the bridge between your domain models and infrastructure adapters. A spec is a declarative description of an aggregate's storage, caching, and search needs. You declare a spec once; adapters read it to configure themselves. Switching backends means changing the adapter, not the spec.

## DocumentSpec

`DocumentSpec` binds together everything an adapter needs to store and retrieve a document aggregate:

    :::python
    from datetime import timedelta

    from forze.application.contracts.document import DocumentSpec


    project_spec = DocumentSpec(
        namespace="projects",
        read={"source": "public.projects", "model": ProjectReadModel},
        write={
            "source": "public.projects",
            "models": {
                "domain": Project,
                "create_cmd": CreateProjectCmd,
                "update_cmd": UpdateProjectCmd,
            },
        },
        history={"source": "public.projects_history"},
        cache={"enabled": True, "ttl": timedelta(minutes=5)},
    )

### Spec fields

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `namespace` | `str` | Yes | Logical name and cache key prefix |
| `read` | `DocumentReadSpec` | Yes | Source relation and read model type |
| `write` | `DocumentWriteSpec` | No | Source relation and write model types<br>(domain, create cmd, update cmd) |
| `history` | `DocumentHistorySpec` | No | Source relation for revision audit trail |
| `cache` | `DocumentCacheSpec` | No | Enable flag and TTL for document caching |

### Read specification

    :::python
    read={"source": "public.projects", "model": ProjectReadModel}

- `source`: the table, view, or collection used for read queries (e.g. `"public.projects"` for Postgres, `"projects"` for Mongo)
- `model`: the `ReadDocument` subclass used to deserialize query results

### Write specification

    :::python
    write={
        "source": "public.projects",
        "models": {
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
    }

- `source`: the table or collection for write operations
- `models.domain`: the `Document` subclass holding business logic
- `models.create_cmd`: the `CreateDocumentCmd` subclass for creation
- `models.update_cmd`: the `BaseDTO` subclass for partial updates

When `write` is `None`, the spec is read-only. Adapters skip mutation operations.

### History specification

    :::python
    history={"source": "public.projects_history"}

Stores previous document revisions for audit trails and historical consistency checks. The source can be a dedicated table (Postgres) or collection (Mongo).

### Cache specification

    :::python
    cache={"enabled": True, "ttl": timedelta(minutes=5)}

When enabled, `ExecutionContext.doc_read()` and `doc_write()` automatically resolve a `CachePort` and inject it into the adapter. The TTL defaults to 300 seconds if not specified.

### Helper methods

`DocumentSpec` provides two convenience methods:

- `supports_soft_delete()`: returns `True` when the domain model inherits from `SoftDeletionMixin`
- `supports_update()`: returns `True` when the update command has writable fields

## SearchSpec

Search is configured separately from document storage. A `SearchSpec` describes the full-text search indexes for an aggregate:

    :::python
    from forze.application.contracts.search import SearchSpec


    project_search_spec = SearchSpec(
        namespace="projects",
        model=ProjectReadModel,
        indexes={
            "public.idx_projects_title": {
                "fields": [{"path": "title"}],
                "source": "public.projects",
            },
            "public.idx_projects_content": {
                "fields": [
                    {"path": "title", "weight": 2.0},
                    {"path": "description", "weight": 1.0},
                ],
                "source": "public.projects",
            },
        },
        default_index="public.idx_projects_title",
    )

### Spec fields

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `namespace` | `str` | Yes | Logical name for the search domain |
| `model` | `type[BaseModel]` | Yes | Result model for typed search |
| `indexes` | `dict[str, SearchIndexSpec]` | Yes | Index name to index configuration |
| `default_index` | `str` | No | Which index to use when<br>not specified |

### Index specification

Each index entry is a `SearchIndexSpec` dict:

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `fields` | `list[SearchFieldSpec]` | Yes | Fields included in the index |
| `source` | `str` | No | Source relation containing<br>the indexed data |
| `groups` | `list[SearchGroupSpec]` | No | Weight groups for FTS ranking |
| `default_group` | `str` | No | Default weight group |
| `mode` | `SearchIndexMode` | No | Override the auto-detected search mode |
| `fuzzy` | `SearchFuzzySpec` | No | Fuzzy search parameters |

### Field specification

Each field in an index:

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `path` | `str` | Yes | Column or field path |
| `group` | `str` | No | Weight group assignment |
| `weight` | `float` | No | Relevance weight |

## Other specifications

### QueueSpec

    :::python
    from forze.application.contracts.queue import QueueSpec

    order_queue = QueueSpec(namespace="orders", model=OrderPayload)

Binds a queue namespace to a Pydantic model type. Used when resolving queue read/write ports.

### PubSubSpec

    :::python
    from forze.application.contracts.pubsub import PubSubSpec

    events_pubsub = PubSubSpec(namespace="events", model=EventPayload)

Binds a pub/sub namespace to a message model type.

### StreamSpec

    :::python
    from forze.application.contracts.stream import StreamSpec

    audit_stream = StreamSpec(namespace="audit", model=AuditEntry)

Binds a stream namespace to an entry model type.

### CacheSpec

    :::python
    from forze.application.contracts.cache import CacheSpec

    cache_spec = CacheSpec(namespace="projects", ttl=timedelta(minutes=10))

Used when resolving cache ports directly (outside of automatic document cache).

## Resolving ports from specs

All specs are consumed by `ExecutionContext` methods:

    :::python
    doc_read  = ctx.doc_read(project_spec)
    doc_write = ctx.doc_write(project_spec)
    search    = ctx.search(project_search_spec)
    cache     = ctx.cache(cache_spec)
    counter   = ctx.counter("tickets")
    storage   = ctx.storage("attachments")

For contracts without a dedicated context method, resolve via dependency key:

    :::python
    from forze.application.contracts.queue import QueueWriteDepKey

    queue = ctx.dep(QueueWriteDepKey)(ctx, order_queue)
    await queue.enqueue("orders", OrderPayload(order_id="123"))

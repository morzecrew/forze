---
name: forze-ports-and-adapters
description: Work with Forze's ports (contracts), specs, dependency injection, and adapter patterns. Use when implementing new adapters, defining ports, configuring specs, or wiring dependencies.
---

# Forze Ports and Adapters

## Ports (Contracts)

Ports are `Protocol`-based interfaces in `forze.application.contracts`. They define what the application layer needs, without specifying how.

### Document Ports

```python
from forze.application.contracts.document import DocumentReadPort, DocumentWritePort

# DocumentReadPort[R] — read operations
port.get(pk)                                       # single document
port.get_many(pks)                                 # multiple documents
port.find(filters)                                 # find one by filter
port.find_many(filters, limit, offset, sorts)     # paginated query
port.count(filters)                                # count documents

# DocumentWritePort[R, D, C, U] — write operations
port.create(dto)                                   # create document
port.update(pk, dto, rev=...)                      # update document
port.delete(pk, rev=...)                           # soft-delete
port.restore(pk, rev=...)                          # restore soft-deleted
port.kill(pk)                                      # hard-delete
port.touch(pk)                                     # bump last_update_at
```

Type parameters:
- `R` — read model type (extends `ReadDocument`)
- `D` — domain model type (extends `Document`)
- `C` — create command type (extends `CreateDocumentCmd`)
- `U` — update command type (extends `BaseDTO`)

### Cache Ports

```python
from forze.application.contracts.cache import CachePort, CacheReadPort, CacheWritePort

# CacheReadPort
port.get(key)                              # get cached value
port.get_many(keys)                        # get multiple values

# CacheWritePort
port.set(key, value)                       # set cached value
port.set_versioned(key, version, value)    # set with version
port.delete(key, hard=False)               # delete cached value

# CachePort combines both
```

### Search Port

```python
from forze.application.contracts.search import SearchReadPort

port.search(
    query,
    filters=...,
    limit=...,
    offset=...,
    sorts=...,
    options=...,
    return_model=...,
    return_fields=...,
)
```

### Other Ports

| Port | Module | Purpose |
|------|--------|---------|
| `CounterPort` | `contracts.counter` | Atomic counters (`incr`, `decr`, `get`) |
| `StoragePort` | `contracts.storage` | Object storage (`upload`, `download`, `list`, `delete`) |
| `TxManagerPort` | `contracts.tx` | Transaction management |
| `QueueReadPort` / `QueueWritePort` | `contracts.queue` | Message queues |
| `PubSubPublishPort` / `PubSubSubscribePort` | `contracts.pubsub` | Publish/subscribe |
| `StreamReadPort` / `StreamWritePort` / `StreamGroupPort` | `contracts.stream` | Event streams |
| `IdempotencyPort` | `contracts.idempotency` | Idempotency snapshots |
| `WorkflowPort` | `contracts.workflow` | Workflow orchestration |

## Specs

Specs describe aggregates and their storage requirements. They are used by `ExecutionContext` to resolve the correct port implementation.

### DocumentSpec

```python
from forze.application.contracts.document import (
    DocumentSpec,
    DocumentReadSpec,
    DocumentWriteSpec,
    DocumentHistorySpec,
    DocumentCacheSpec,
)

spec = DocumentSpec(
    namespace="my_entity",
    read=DocumentReadSpec(model=MyReadDocument),
    write=DocumentWriteSpec(
        model=MyDocument,
        create=CreateMyDocumentCmd,
        update=UpdateMyDocumentCmd,
    ),
    history=DocumentHistorySpec(source="my_entity"),
    cache=DocumentCacheSpec(ttl=timedelta(seconds=300)),
)
```

### CacheSpec

```python
from forze.application.contracts.cache import CacheSpec

cache_spec = CacheSpec(namespace="my_cache", ttl=timedelta(seconds=300))
```

### SearchSpec

```python
from forze.application.contracts.search import SearchSpec, SearchIndexSpec

search_spec = SearchSpec(
    namespace="my_entity",
    model=MyReadDocument,
    indexes={"default": SearchIndexSpec(...)},
    default_index="default",
)
```

## Dependency Injection

### DepKey

Typed dependency key for the DI container:

```python
from forze.application.contracts.deps import DepKey

MY_CLIENT_KEY = DepKey[MyClient]("my_client")
```

### Deps and DepsPort

```python
from forze.application.execution import Deps

deps = Deps(deps={
    MY_CLIENT_KEY: my_client_instance,
})

# DepsPort protocol
deps.provide(MY_CLIENT_KEY)   # resolve dependency
deps.exists(MY_CLIENT_KEY)    # check existence
```

### DepsModule

A factory function that returns `Deps`:

```python
from forze.application.execution import Deps

def my_deps_module() -> Deps:
    client = MyClient(config)
    return Deps(deps={MY_CLIENT_KEY: client})
```

### DepsPlan

Compose multiple modules:

```python
from forze.application.execution import DepsPlan

plan = DepsPlan(modules=(
    postgres_module,
    redis_module,
    my_deps_module,
))
```

### DepRouter

Route dependency resolution by spec namespace (multi-tenant or multi-backend):

```python
from forze.application.contracts.deps import DepRouter

class MyDocumentDepRouter(DepRouter):
    dep_key = MY_DOC_ADAPTER_KEY

    def selector(self, spec):
        return spec.namespace

    routes = {
        "entity_a": adapter_a,
        "entity_b": adapter_b,
    }
```

## Implementing Adapters

Adapters implement ports and live in integration packages.

### Pattern

```python
import attrs
from forze.application.contracts.document import DocumentReadPort, DocumentWritePort

@attrs.define(slots=True, kw_only=True, frozen=True)
class MyDocumentAdapter(DocumentReadPort[R], DocumentWritePort[R, D, C, U]):
    client: MyClient
    namespace: str

    async def get(self, pk, *, for_update=False, return_fields=None):
        ...

    async def create(self, dto):
        ...
```

### Integration Package Structure

```text
src/forze_mydb/
├── __init__.py          # public re-exports
├── _compat.py           # optional dependency checks
├── adapters/            # port implementations
│   ├── __init__.py
│   └── document.py
├── client.py            # database client wrapper
├── execution/           # DI module and lifecycle
│   ├── __init__.py
│   ├── deps.py          # DepsModule class
│   └── lifecycle.py     # LifecycleStep
└── kernel/              # internal utilities
    └── query.py         # query rendering
```

### DepsModule for Integration

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class MyDbDepsModule:
    client: MyDbClient

    def __call__(self) -> Deps:
        return Deps(deps={
            MY_DB_CLIENT_KEY: self.client,
            MY_DOC_ADAPTER_KEY: MyDocumentAdapter(client=self.client),
        })
```

### LifecycleStep for Integration

```python
from forze.application.execution import LifecycleStep

my_db_lifecycle = LifecycleStep(
    name="mydb",
    startup=lambda ctx: ctx.dep(MY_DB_CLIENT_KEY).connect(),
    shutdown=lambda ctx: ctx.dep(MY_DB_CLIENT_KEY).disconnect(),
)
```

## Existing Adapters

### Mock (in-memory)

```python
from forze_mock.adapters import MockDocumentAdapter, MockState

state = MockState()
adapter = MockDocumentAdapter(
    state=state,
    namespace="my_entity",
    read_model=MyReadDocument,
)
```

### Postgres

```python
from forze_postgres.adapters.document import PostgresDocumentAdapter
from forze_postgres.execution import PostgresDepsModule
```

### Redis

```python
from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.execution import RedisDepsModule
```

### S3

```python
from forze_s3.adapters import S3StorageAdapter
from forze_s3.execution import S3DepsModule
```

## Error Handling in Adapters

Use `@handled` decorator to convert infrastructure exceptions to `CoreError` subtypes:

```python
from forze.base.errors import handled, CoreError

@handled(my_error_handler, "create")
async def create(self, dto):
    ...
```

Each integration package provides its own error handler (e.g., `postgres_handled`, `mongo_handled`, `redis_handled`).

## Checklist

When implementing a new adapter:

1. Identify which port(s) to implement from `forze.application.contracts`
2. Create the adapter class with `@attrs.define(slots=True, kw_only=True, frozen=True)`
3. Create a `DepsModule` class to wire the adapter into the DI container
4. Create a `LifecycleStep` for startup/shutdown
5. Add an error handler mapping infrastructure exceptions to `CoreError` subtypes
6. Place code in `src/forze_<provider>/`
7. Add the package to `pyproject.toml` import-linter `protected_modules`

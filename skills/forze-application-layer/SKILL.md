---
name: forze-application-layer
description: Work with Forze's application layer including usecases, middleware, CQRS, composition, execution context, and facades. Use when creating usecases, configuring middleware, or setting up document/storage/search operations.
---

# Forze Application Layer

## Usecases

Usecases are immutable `attrs` classes that extend `Usecase[Args, R]`:

```python
import attrs
from forze.application.execution import Usecase, ExecutionContext

@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[Out: ReadDocument](Usecase[UUID, Out]):
    doc: DocumentReadPort[Out]

    async def main(self, args: UUID) -> Out:
        return await self.doc.get(args)
```

Key properties:
- `ctx: ExecutionContext` — injected automatically
- `middlewares: tuple[Middleware, ...]` — composable middleware chain
- `main(args)` — override with business logic
- `__call__(args)` — builds and executes the middleware chain

### Built-in Usecases

| Usecase | Args | Returns | Purpose |
|---------|------|---------|---------|
| `GetDocument` | `UUID` | `ReadDocument` | Fetch a single document |
| `CreateDocument` | `BaseDTO` | `ReadDocument` | Create a document via mapper |
| `UpdateDocument` | `UpdateArgs[BaseDTO]` | `ReadDocument` | Update a document via mapper |
| `DeleteDocument` | `DeleteArgs` | `ReadDocument` | Soft-delete a document |
| `RestoreDocument` | `RestoreArgs` | `ReadDocument` | Restore a soft-deleted document |
| `KillDocument` | `UUID` | `None` | Hard-delete a document |
| `ListDocuments` | `ListRequestDTO` | `Paginated[R]` | Paginated list |
| `UploadObject` | `UploadObjectRequestDTO` | `StoredObject` | Upload to storage |
| `ListObjects` | `ListObjectsRequestDTO` | `list[StoredObject]` | List storage objects |

### Custom Usecases

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class ArchiveDocument(Usecase[UUID, MyReadDocument]):
    doc_write: DocumentWritePort[MyReadDocument, MyDocument, Any, Any]

    async def main(self, args: UUID) -> MyReadDocument:
        return await self.doc_write.update(
            args, MyUpdateDTO(status="archived"),
        )
```

## Middleware

Middleware wraps usecase execution. Three types:

### Guard (before)

Runs before the usecase. Raises on failure:

```python
from forze.application.execution.middleware import Guard

class OwnershipGuard(Guard[UUID]):
    async def __call__(self, args: UUID) -> None:
        # raise if caller is not the owner
        ...
```

### Effect (after)

Runs after the usecase. Can transform the result:

```python
from forze.application.execution.middleware import Effect

class AuditEffect(Effect[UUID, MyReadDocument]):
    async def __call__(self, args: UUID, res: MyReadDocument) -> MyReadDocument:
        # log audit event
        return res
```

### Middleware (wrap)

Full control over the call chain:

```python
from forze.application.execution.middleware import Middleware, NextCall

class TimingMiddleware(Middleware[Any, Any]):
    async def __call__(self, next: NextCall, args: Any) -> Any:
        start = time.time()
        result = await next(args)
        duration = time.time() - start
        return result
```

### TxMiddleware

Built-in middleware that wraps execution in a transaction with optional after-commit effects:

```python
from forze.application.execution.middleware import TxMiddleware

tx_mw = TxMiddleware(ctx=ctx, after_commit=(notify_effect,))
```

## Execution Context

`ExecutionContext` is the central dependency resolution hub:

```python
ctx.dep(my_dep_key)                    # resolve a typed dependency
ctx.doc_read(my_spec)                  # resolve DocumentReadPort
ctx.doc_write(my_spec)                 # resolve DocumentWritePort
ctx.cache(my_cache_spec)              # resolve CachePort
ctx.counter(namespace)                 # resolve CounterPort
ctx.txmanager()                        # resolve TxManagerPort
ctx.storage(bucket)                    # resolve StoragePort
ctx.search(my_search_spec)            # resolve SearchReadPort

async with ctx.transaction():          # scoped transaction (supports nesting)
    ...
```

## Composition

### UsecasePlan

Configure middleware for each operation:

```python
from forze.application.execution import UsecasePlan

plan = (
    UsecasePlan()
    .tx(DocumentOperation.CREATE)                          # wrap in transaction
    .before(DocumentOperation.CREATE, my_guard_factory)    # add guard
    .after(DocumentOperation.CREATE, my_effect_factory)    # add effect
    .after_commit(DocumentOperation.CREATE, notify_factory) # after commit
)
```

Middleware buckets (execution order):
1. `outer_before` — guards before everything
2. `outer_wrap` — wrapping middleware
3. `outer_after` — effects after outer
4. Transaction boundary (if `tx=True`)
5. `in_tx_before` — guards inside transaction
6. `in_tx_wrap` — wrapping inside transaction
7. `in_tx_after` — effects inside transaction
8. `after_commit` — effects after transaction commits

### UsecaseRegistry

Maps operation keys to usecase factories:

```python
from forze.application.execution import UsecaseRegistry

registry = UsecaseRegistry(defaults={
    DocumentOperation.GET: lambda ctx: GetDocument(ctx=ctx, doc=ctx.doc_read(spec)),
    DocumentOperation.CREATE: lambda ctx: CreateDocument(ctx=ctx, doc=ctx.doc_write(spec), mapper=mapper),
})
```

Use `build_document_registry` for standard document CRUD:

```python
from forze.application.composition.document import build_document_registry

registry = build_document_registry(spec=my_spec, dtos=my_dtos)
```

### DocumentUsecasesFacade

Typed facade for document operations:

```python
from forze.application.composition.document import DocumentUsecasesFacade

facade = DocumentUsecasesFacade(ctx=ctx, reg=registry)

doc = await facade.get(doc_id)
created = await facade.create(create_dto)
updated = await facade.update(UpdateArgs(pk=doc_id, dto=update_dto))
deleted = await facade.delete(DeleteArgs(pk=doc_id))
```

### facade_op Descriptor

`facade_op` is a descriptor that resolves usecases lazily from the registry:

```python
from forze.application.composition.document import facade_op

class MyFacade(UsecasesFacade):
    get = facade_op(DocumentOperation.GET, uc=GetDocument)
    create = facade_op(DocumentOperation.CREATE, uc=CreateDocument)
```

## DocumentOperation Enum

```python
from forze.application.composition.document import DocumentOperation

DocumentOperation.GET        # "document.get"
DocumentOperation.CREATE     # "document.create"
DocumentOperation.UPDATE     # "document.update"
DocumentOperation.DELETE     # "document.delete"
DocumentOperation.RESTORE    # "document.restore"
DocumentOperation.KILL       # "document.kill"
DocumentOperation.LIST       # "document.list"
DocumentOperation.RAW_LIST   # "document.raw_list"
```

## DTOs

### Pagination

```python
from forze.application.dto import Pagination, Paginated

class MyListRequest(Pagination):
    status: str | None = None
```

### ListRequestDTO / RawListRequestDTO

```python
from forze.application.dto import ListRequestDTO, RawListRequestDTO

# ListRequestDTO includes: page, size, filters, sorts
# RawListRequestDTO adds: return_fields (partial projections)
```

### DocumentDTOs

Typed container for document DTO types:

```python
from forze.application.composition.document import DocumentDTOs

dtos = DocumentDTOs(
    read=MyReadDocument,
    create=CreateMyDocumentDTO,
    update=UpdateMyDocumentDTO,
    list=ListRequestDTO,
    raw_list=RawListRequestDTO,
)
```

## DTO Mapping

### DTOMapper

Composable async mapper from input DTO to command:

```python
from forze.application.mapping import DTOMapper

mapper = DTOMapper(in_=CreateMyDTO, out=CreateMyDocumentCmd)
cmd = await mapper(ctx, input_dto)
```

### MappingStep

Custom mapping steps inject additional data:

```python
from forze.application.mapping import MappingStep

@attrs.define(slots=True, kw_only=True, frozen=True)
class MyStep(MappingStep[CreateMyDTO]):
    def produces(self) -> frozenset[str]:
        return frozenset({"custom_field"})

    async def __call__(self, ctx, source, payload, params=None):
        return {"custom_field": "computed_value"}
```

### Built-in Steps

- `NumberIdStep(namespace=...)` — auto-increment numeric ID via `CounterPort`
- `CreatorIdStep` — inject `created_by` from context

## Execution Runtime

### DepKey

Typed dependency key:

```python
from forze.application.contracts.deps import DepKey

MY_SERVICE_KEY = DepKey[MyService]("my_service")
```

### DepsModule and DepsPlan

```python
from forze.application.execution import Deps, DepsPlan

def my_module() -> Deps:
    return Deps(deps={MY_SERVICE_KEY: MyServiceImpl()})

plan = DepsPlan(modules=(my_module,))
```

### ExecutionRuntime

Combines deps, lifecycle, and context scope:

```python
from forze.application.execution import ExecutionRuntime, LifecyclePlan, LifecycleStep

runtime = ExecutionRuntime(
    deps=deps_plan,
    lifecycle=LifecyclePlan.from_steps(
        LifecycleStep(name="db", startup=db_startup, shutdown=db_shutdown),
    ),
)

async with runtime.scope():
    ctx = runtime.get_context()
    # use ctx for operations
```

## Checklist

When creating a new usecase:

1. Extend `Usecase[Args, R]` with `@attrs.define(slots=True, kw_only=True, frozen=True)`
2. Declare port dependencies as attrs fields
3. Implement `async def main(self, args: Args) -> R`
4. Register the factory in a `UsecaseRegistry`
5. Add to a facade if needed
6. Configure middleware via `UsecasePlan` for transactions, guards, and effects
7. Place in `src/forze/application/usecases/`

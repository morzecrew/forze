# Operation composition

## Document composition

The registries below are what `AggregateKit` composes under the hood — reach for them directly for plain CRUD with no governed concerns, or for a fully bespoke operation surface.

### Registry and transaction plan

`build_document_registry` registers standard CRUD handlers. **Transactions are not implicit** — bind a transaction route on write operations, then **freeze** before HTTP attach:

```python
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)

write_ops = [
    project_spec.default_namespace.key(op)
    for op in (
        DocumentKernelOp.CREATE,
        DocumentKernelOp.UPDATE,
        DocumentKernelOp.KILL,
    )
]
# soft delete/restore are a separate registry — bind SoftDeletionKernelOp ops
# from build_soft_deletion_registry the same way

registry = (
    build_document_registry(project_spec, project_dtos)
    .bind(*write_ops)
    .bind_tx()
    .set_route("default")
    .finish(deep=True)
    .freeze()
)
```

### Custom handlers and stage hooks

```python
from forze.application.contracts.execution import BeforeStep
from forze_kits.aggregates.document import DocumentKernelOp

create_op = project_spec.default_namespace.key(DocumentKernelOp.CREATE)


def auth_before_factory(ctx):
    async def _before(args):
        if not is_authorized(ctx):
            raise PermissionError("Not authorized")
    return _before


registry = (
    build_document_registry(project_spec, project_dtos)
    .bind(create_op)
    .bind_tx()
    .set_route("default")
    .finish(deep=False)
    .bind_outer()
    .before(BeforeStep(id="auth", factory=auth_before_factory, priority=100))
    .finish(deep=True)
    .freeze()
)
```

Custom operations use explicit operation keys on the same registry:

```python
archive_op = project_spec.default_namespace.key("archive")

registry = build_document_registry(project_spec, project_dtos)
registry = registry.set_handler(
    archive_op,
    lambda ctx: ArchiveProject(doc=ctx.document.command(project_spec)),
)
registry = (
    registry.bind(archive_op)
    .bind_tx()
    .set_route("default")
    .finish(deep=True)
    .freeze()
)
```

### Stage order

Outer `before` / `wrap` / `on_success` / `on_failure` / `finally_`, then optional transaction scope (`tx_before`, handler, transactional `on_success`, `after_commit`, `dispatch_after_commit`). Higher `priority` runs first within the same stage. See [Middleware and plans](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/).

### Cross-cutting patches

`registry.patch(selector)` applies a plan default (route, deadline, hook) to every operation a selector matches. Patches are **late-bound** — resolved at `freeze()` against the full key set. Across `OperationRegistry.merge(...)` the cross-registry reach is **fail-closed**: if a patch from one part matches another part's operations, `merge` raises naming the selectors and ops. Resolve it by scoping the patch (`patch(selector, namespace=ns)` — matches only ops under `ns`), folding it into per-operation plans first (`registry.materialize_patches()`), or allowing it explicitly (`merge(..., cross_registry=True)`). A policy patch applied *after* the merge never travels through `merge`. A **live** patch is "apply wherever this lands"; a **materialized** one is "settled here." See [Middleware and plans](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/).

## Mapping steps

Inject computed fields (e.g. `number_id`, `creator_id`) before the handler runs:

```python
from forze_kits.aggregates.document import DocumentMappers, build_document_registry
from forze_kits.domain.creator_id import CreatorIdMappingStepFactory
from forze_kits.domain.number_id import NumberIdMappingStepFactory
from forze_kits.mapping import PydanticPipelineMapperFactory

create_mapper = PydanticPipelineMapperFactory(
    in_=CreateProjectRequest,
    out=CreateProjectCmd,
    step_factories=(
        NumberIdMappingStepFactory(spec=project_counter_spec),
        CreatorIdMappingStepFactory(),  # configured per your identity resolver
    ),
)

registry = build_document_registry(
    project_spec,
    project_dtos,
    DocumentMappers(create=create_mapper),
).freeze()
```

See [document and search specs](document-spec.md) and the mapping reference for step configuration.

## Anti-patterns

- **Attaching HTTP routes without `.freeze()`** — call `.freeze()` after `bind_tx().set_route(...).finish(...)` on operation registries, and on deps/lifecycle plans before `ExecutionRuntime`.

## Reference

- [Operation composition](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/)

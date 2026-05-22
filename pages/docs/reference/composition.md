# Composition & Mapping

Forze provides pre-built composition layers for document, search, storage, and authn aggregates. These helpers build an `OperationRegistry`, expose typed facades via `facade_op(...)`, and keep operation keys explicit through `StrKeyNamespace` on each spec.

## Document composition

### `build_document_registry`

Creates an `OperationRegistry` pre-populated with standard document handler factories:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
    )

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)

Use `spec.default_namespace` with `DocumentKernelOp` when you need fully qualified operation keys. Bind transaction routes and freeze before FastAPI attach:

    :::python
    from forze.application.composition.document import DocumentKernelOp

    write_ops = [
        project_spec.default_namespace.key(op)
        for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE, DocumentKernelOp.KILL)
    ]
    registry = (
        registry.bind(*write_ops)
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

### `DocumentFacade`

Typed facade exposing document operations as attributes (requires a frozen registry):

    :::python
    from forze.application.composition.document import DocumentFacade

    facade = DocumentFacade(
        ctx=ctx,
        registry=registry,
        namespace=project_spec.default_namespace,
    )

    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

Facade attributes are namespace-aware `facade_op(...)` descriptors. HTTP endpoint specs carry `operation: StrKey` with the fully qualified key (see [FastAPI integration](../integrations/fastapi.md)).

### Custom operations

Register custom handlers on the same registry:

    :::python
    registry = registry.set_handler(
        project_spec.default_namespace.key("archive"),
        lambda ctx: ArchiveProject(doc=ctx.document.command(project_spec)),
        override=True,
    )

Add stages with `.bind(...).bind_outer().before(...)` as needed, then `.freeze()`.

## Search composition

### `build_search_registry`

Creates a registry with search handler factories:

    :::python
    from forze.application.composition.search import build_search_registry

    search_registry = build_search_registry(search_spec).freeze()

### `SearchFacade`

    :::python
    from forze.application.composition.search import SearchFacade

    facade = SearchFacade(
        ctx=ctx,
        registry=search_registry,
        namespace=search_spec.default_namespace,
    )
    result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))

Hub and federated search use `build_hub_search_registry` and `build_federated_search_registry` with the same freeze pattern.

## Storage composition

`build_storage_registry(storage_spec)` registers `upload`, `list`, `download`, and `delete` handlers. `StorageFacade` resolves them through `registry` + `namespace`. Bind tx routes for write operations, then `.freeze()` before `attach_storage_endpoints`.

## Authn composition

`build_authn_registry(authn_spec)` registers `password_login`, `refresh_tokens`, `logout`, and `change_password`. `AuthnFacade` uses the same namespace-aware facade contract. Freeze the registry before `attach_authn_endpoints`.

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the handler. `PydanticPipelineMapperFactory` maps a Pydantic source model to an output DTO; optional `MappingStep`s inject computed fields such as `number_id` or `creator_id` from `forze_contrib`.

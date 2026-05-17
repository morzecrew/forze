# Composition & Mapping

Forze provides pre-built composition layers for document, search, storage, and authn aggregates. These helpers build a `UsecaseRegistry`, expose typed facades via `facade_op(...)`, and keep operation keys explicit through `OperationNamespace`.

## Document composition

### `build_document_registry`

Creates a `UsecaseRegistry` pre-populated with standard CRUD usecase factories:

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

Use `operation_namespace_for(project_spec)` with `DocumentKernelOp` when you need the fully qualified operation keys. Transactions are explicit:

    :::python
    from forze.application.composition.document import DocumentKernelOp
    from forze.application.execution import operation_namespace_for

    ops = operation_namespace_for(project_spec)

    registry.tx(ops.op(DocumentKernelOp.CREATE), route="default")
    registry.tx(ops.op(DocumentKernelOp.UPDATE), route="default")
    registry.finalize("projects")

### `DocumentUsecasesFacade`

Typed facade exposing document operations as attributes:

    :::python
    from forze.application.composition.document import DocumentUsecasesFacade
    from forze.application.execution import operation_namespace_for

    facade = DocumentUsecasesFacade(
        ctx=ctx,
        registry=registry,
        namespace=operation_namespace_for(project_spec),
    )

    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

The facade attributes are namespace-aware `facade_op(...)` descriptors. For endpoint metadata and other non-facade call sites, use `OperationRef.absolute(...)` with a fully qualified operation key.

### Custom operations

Register custom operations on the same registry and author stages directly on it:

    :::python
    registry.register("archive", lambda ctx: ArchiveProject(ctx=ctx))
    registry.tx("archive", route="default")
    registry.before("archive", auth_guard, priority=100)

## Search composition

### `build_search_registry`

Creates a registry with typed and raw search usecase factories:

    :::python
    from forze.application.composition.search import build_search_registry

    search_registry = build_search_registry(search_spec)

### `SearchUsecasesFacade`

    :::python
    from forze.application.composition.search import SearchUsecasesFacade
    from forze.application.execution import operation_namespace_for

    facade = SearchUsecasesFacade(
        ctx=ctx,
        registry=search_registry,
        namespace=operation_namespace_for(search_spec),
    )
    result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))

## Storage composition

`build_storage_registry(storage_spec)` registers `upload`, `list`, `download`, and `delete` operations. `StorageUsecasesFacade` resolves the same operations through `registry` + `namespace`.

## Authn composition

`build_authn_registry(authn_spec)` registers `password_login`, `refresh_tokens`, `logout`, and `change_password`. `AuthnUsecasesFacade` uses the same namespace-aware facade contract as the other built-in composition packages.

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the usecase. `DTOMapper` maps a Pydantic source model to an output DTO and optional `MappingStep`s inject computed fields such as `number_id` or `creator_id`.

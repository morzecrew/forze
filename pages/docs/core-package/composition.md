# Composition & Mapping

Forze provides pre-built composition layers for document and search aggregates, a DTO mapping pipeline for field injection, and paginated response types. These reduce boilerplate when wiring standard CRUD and search operations.

## Document composition

### build_document_registry

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

The registry includes factories for all `DocumentOperation` variants:

| Operation | Usecase | Args | Returns |
|-----------|---------|------|---------|
| `GET` | `GetDocument` | `UUID` | `R` |
| `CREATE` | `CreateDocument` | `C` | `R` |
| `UPDATE` | `UpdateDocument` | `UpdateArgs[U]` | `R` |
| `KILL` | `KillDocument` | `UUID` | `None` |
| `DELETE` | `DeleteDocument` | `SoftDeleteArgs` | `R` |
| `RESTORE` | `RestoreDocument` | `SoftDeleteArgs` | `R` |
| `LIST` | `TypedListDocuments` | `tL` | `Paginated[R]` |
| `RAW_LIST` | `RawListDocuments` | `rL` | `RawPaginated` |

`DELETE` and `RESTORE` are only registered when the domain model supports soft deletion. `UPDATE` is only registered when the spec supports update.

### tx_document_plan

A pre-built `UsecasePlan` with transaction wrapping for all operations:

    :::python
    from forze.application.composition.document import tx_document_plan

    registry.extend_plan(tx_document_plan, inplace=True)

### DocumentUsecasesFacade

Typed facade exposing document operations as attributes:

    :::python
    from forze.application.composition.document import DocumentUsecasesFacade

    facade = DocumentUsecasesFacade(ctx=ctx, reg=registry)

    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(project.id)
    updated = await facade.update(UpdateArgs(pk=project.id, dto=UpdateProjectCmd(title="Updated")))
    await facade.kill(project.id)

| Attribute | Resolved usecase |
|-----------|-----------------|
| `get` | `GetDocument[R]` |
| `create` | `CreateDocument[C, D, R]` |
| `update` | `UpdateDocument[U, D, R]` |
| `kill` | `KillDocument` |
| `delete` | `DeleteDocument[R]` |
| `restore` | `RestoreDocument[R]` |
| `list` | `TypedListDocuments[tL, R]` |
| `raw_list` | `RawListDocuments[rL]` |

Each attribute resolves the usecase from the registry with the plan's middleware chain.

### DocumentDTOs

An attrs class mapping DTO types for a document aggregate:

    :::python
    from forze.application.composition.document import DocumentDTOs

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

| Field | Type | Required |
|-------|------|----------|
| `read` | `type[ReadDocument]` | Yes |
| `create` | `type[BaseDTO]` | No |
| `update` | `type[BaseDTO]` | No |
| `list` | `type[ListRequestDTO]` | No |
| `raw_list` | `type[RawListRequestDTO]` | No |

### Extending document composition

Add custom middleware to the default plan or register custom operations:

    :::python
    from forze.application.composition.document import (
        DocumentOperation,
        build_document_registry,
        tx_document_plan,
    )

    # Custom middleware
    plan = (
        tx_document_plan
        .before(DocumentOperation.CREATE, auth_guard, priority=100)
        .after_commit(DocumentOperation.CREATE, notify_effect)
    )

    # Custom operation
    registry = build_document_registry(project_spec, project_dtos)
    registry = registry.register(
        "archive",
        lambda ctx: ArchiveProject(ctx=ctx),
    )

    plan = plan.tx("archive").before("archive", auth_guard, priority=100)

### UpdateArgs and SoftDeleteArgs

Typed argument containers for update and soft-delete usecases:

    :::python
    from forze.application.usecases.document import UpdateArgs, SoftDeleteArgs

    # Update
    await facade.update()(UpdateArgs(pk=project_id, dto=update_cmd))

    # Soft delete with optimistic concurrency
    await facade.delete()(SoftDeleteArgs(pk=project_id, rev=current_rev))

`UpdateArgs[U]` carries `pk` (UUID) and `dto` (update command). `SoftDeleteArgs` carries `pk` (UUID) and optional `rev` (int).

## Search composition

### build_search_registry

Creates a registry with typed and raw search usecase factories:

    :::python
    from forze.application.composition.search import (
        SearchDTOs,
        build_search_registry,
    )

    search_dtos = SearchDTOs(read=ProjectRead)
    search_registry = build_search_registry(search_spec, search_dtos)

### SearchUsecasesFacade

    :::python
    from forze.application.composition.search import SearchUsecasesFacade

    facade = SearchUsecasesFacade(ctx=ctx, reg=search_registry)
    result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))

### SearchOperation

| Key | Purpose |
|-----|---------|
| `TYPED_SEARCH` | Search returning typed read models |
| `RAW_SEARCH` | Search returning raw JSON dicts |

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the create usecase. It injects computed fields like `number_id` or `creator_id`.

### DTOMapper

Pipeline that maps a Pydantic source model to an output DTO:

    :::python
    from forze.application.mapping import DTOMapper

    mapper = DTOMapper(
        in_=CreateProjectDTO,
        out=CreateProjectCmd,
        steps=(NumberIdStep(namespace="projects"),),
    )

    result = await mapper(ctx, incoming_dto)

The mapper:

1. Dumps the source model to a dict (excluding unset fields)
2. Runs each `MappingStep` in order
3. Merges each step's patch into the payload
4. Validates the final payload into the target DTO

Steps must not produce overlapping fields. If a step would overwrite an existing field, the `MappingPolicy` controls whether it is allowed.

### MappingStep

Protocol for a single step in the mapping pipeline:

    :::python
    from forze.application.mapping import MappingStep

    class MyStep(MappingStep):
        def produces(self) -> frozenset[str]:
            return frozenset({"my_field"})

        async def __call__(self, ctx, source, payload) -> JsonDict:
            return {"my_field": compute_value(ctx, source)}

| Method | Purpose |
|--------|---------|
| `produces()` | Return the set of field names this step writes |
| `__call__(ctx, source, payload)` | Compute a patch dict to merge into the payload |

### MappingPolicy

Controls field overwrite behavior:

    :::python
    from forze.application.mapping.mapper import MappingPolicy

    policy = MappingPolicy(allow_overwrite=frozenset({"updated_at"}))

By default, no overwrites are allowed.

### Built-in steps

| Step | Produces | Purpose |
|------|----------|---------|
| `NumberIdStep(namespace)` | `number_id` | Resolves a counter port and increments to get the next ID |
| `CreatorIdStep` | `creator_id` | Placeholder for actor-based injection (not yet implemented) |

### build_document_create_mapper

Factory that creates a mapper pre-configured for document creation:

    :::python
    from forze.application.composition.document import build_document_create_mapper

    mapper = build_document_create_mapper(project_spec, project_dtos)
    mapper = mapper.with_steps(NumberIdStep(namespace="projects"))

## DTOs

### Paginated

Generic paginated response for typed results:

    :::python
    from forze.application.dto import Paginated

    response: Paginated[ProjectRead] = Paginated(
        hits=[project1, project2],
        page=1,
        size=20,
        count=42,
    )

| Field | Type | Purpose |
|-------|------|---------|
| `hits` | `list[T]` | Records for the current page |
| `page` | `int` | One-based page number |
| `size` | `int` | Page size |
| `count` | `int` | Total matching records across all pages |

### RawPaginated

Same as `Paginated` but with `list[JsonDict]` hits for field-projected results.

### SearchRequestDTO

Search request payload:

    :::python
    from forze.application.dto import SearchRequestDTO

    request = SearchRequestDTO(
        query="roadmap",
        filters={"$fields": {"is_deleted": False}},
        sorts={"created_at": "desc"},
    )

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `query` | `str` | `""` | Full-text search query; empty for filter-only mode |
| `filters` | `QueryFilterExpression \| None` | `None` | Filter expression |
| `sorts` | `QuerySortExpression \| None` | `None` | Sort expression |
| `options` | `SearchOptions \| None` | `None` | Backend-specific search options |

### RawSearchRequestDTO

Extends `SearchRequestDTO` with a required `return_fields` set for raw result projections:

    :::python
    from forze.application.dto import RawSearchRequestDTO

    request = RawSearchRequestDTO(
        query="roadmap",
        return_fields={"id", "title", "score"},
    )

## Putting it together

A complete example wiring document and search composition:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        DocumentUsecasesFacade,
        build_document_registry,
        tx_document_plan,
    )
    from forze.application.composition.search import (
        SearchDTOs,
        SearchUsecasesFacade,
        build_search_registry,
    )

    # Document registry
    project_dtos = DocumentDTOs(
        read=ProjectRead,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )
    doc_registry = build_document_registry(project_spec, project_dtos)
    doc_registry.extend_plan(tx_document_plan, inplace=True)

    # Search registry
    search_dtos = SearchDTOs(read=ProjectRead)
    search_registry = build_search_registry(project_search_spec, search_dtos)

    # At request time
    ctx = runtime.get_context()
    docs = DocumentUsecasesFacade(ctx=ctx, reg=doc_registry)
    search = SearchUsecasesFacade(ctx=ctx, reg=search_registry)

    # CRUD
    created = await docs.create(CreateProjectCmd(title="Roadmap"))
    fetched = await docs.get(created.id)

    # Search
    results = await search.search(
        SearchRequestDTO(query="roadmap", limit=20)
    )

# Usecase Composition

## What problem this solves

Repeated guards, effects, transactions, and operation wiring become noisy when every usecase assembles them by hand.

## When you need this

Use this when you want reusable middleware plans, operation registries, or document/search facades.


Forze provides a declarative system for composing usecases with middleware. Instead of manually wiring guards and effects into every operation, you declare them in **plans** and **registries** that are resolved at runtime.

## How composition works

The composition model has three parts:

1. **Registry**: maps operation names to usecase factories
2. **Plan**: describes which middleware (guards, effects, transactions) wraps each operation
3. **Facade**: ties registry, plan, and spec together into a single entry point

When a request arrives, the facade resolves the usecase factory from the registry, wraps it with the middleware chain from the plan, and returns a callable ready to execute.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-registry.svg" alt="Operation registry and plan resolution">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-registry.svg" alt="Operation registry and plan resolution">
</div>

## Operation registry

The `UsecaseRegistry` maps operation keys to usecase factories. Each factory receives an `ExecutionContext` and returns a `Usecase` instance:

    :::python
    from forze.application.execution import UsecaseRegistry


    registry = UsecaseRegistry()
    registry = registry.register("get", lambda ctx: GetProject(ctx=ctx))
    registry = registry.register("create", lambda ctx: CreateProject(ctx=ctx))

For document aggregates, `build_document_registry(spec, dtos)` creates a registry pre-populated with standard CRUD operations (GET, CREATE, UPDATE, KILL, DELETE, RESTORE).

## Usecase plan

The `UsecasePlan` describes how each operation is composed with middleware. It maps operation keys to middleware buckets that run at specific stages.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-composition.svg" alt="Operation composition flow">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-composition.svg" alt="Operation composition flow">
</div>

### Plan buckets

Each operation has multiple middleware buckets (a :class:`~forze.application.execution.bucket.Phase` × :class:`~forze.application.execution.bucket.Slot` placement, represented by :class:`~forze.application.execution.bucket.BucketKey`), executed in this order:

| Bucket | When it runs | Use case |
|--------|-------------|----------|
| `outer_before` | Before everything | Authorization, input validation |
| `outer_wrap` | Wraps the entire chain | Metrics, retries, error handling |
| Transaction boundary | (automatic when tx=True) | |
| `in_tx_before` | Inside tx, before usecase | Lock acquisition, pre-checks |
| `in_tx_wrap` | Inside tx, wraps usecase | In-transaction cross-cutting |
| `in_tx_after` | Inside tx, after usecase | Audit logging inside tx |
| `outer_after` | After everything | Response transformation |
| `after_commit` | After successful commit | Notifications, event publishing |

The `in_tx_*` and `after_commit` buckets only activate when `tx=True` for the operation.

### Building a plan

    :::python
    from forze.application.execution import UsecasePlan


    plan = (
        UsecasePlan()
        .tx("create", route="default")
        .tx("update", route="default")
        .before("create", auth_guard, priority=100)
        .after("create", log_effect, priority=0)
        .after_commit("create", notify_effect)
        .before("*", rate_limit_guard, priority=200)
    )

The wildcard `"*"` applies to all operations as a base plan. Per-operation plans extend the base. When resolved, the base and operation-specific plans are merged.

### Priority ordering

Middlewares within a bucket are sorted by priority (descending). Higher priority runs first (outermost). Priority values must be unique within a bucket to avoid ambiguity.

### Merging plans

Multiple plans can be merged for modular composition:

    :::python
    auth_plan = build_auth_plan()
    audit_plan = build_audit_plan()

    final_plan = UsecasePlan.merge(base_plan, auth_plan, audit_plan)

### Inspecting a plan

Use `explain()` to see the resolved middleware chain for an operation:

    :::python
    explanation = plan.explain("create")
    print(explanation.pretty_format())

This outputs the full chain with bucket names, priorities, and factory references, useful for debugging composition issues.

## Document composition

For document aggregates, Forze provides a pre-built composition layer:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        DocumentUsecasesFacade,
        build_document_registry,
    )

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)

`build_document_registry(spec, dtos)` registers standard usecase factories for all `DocumentOperation` variants.

Transaction middleware is composed explicitly with `UsecasePlan` and merged into the registry when needed.


Create a facade from an execution context and the registry:

    :::python
    from forze.application.dto import DocumentIdDTO

    facade = DocumentUsecasesFacade(ctx=ctx, reg=registry)
    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

### Document operations

The `DocumentOperation` enum defines the standard operation keys:

| Key | Operation |
|-----|-----------|
| `GET` | Fetch a document by ID |
| `CREATE` | Create a new document |
| `UPDATE` | Apply a partial update |
| `KILL` | Hard-delete a document |
| `DELETE` | Soft-delete a document |
| `RESTORE` | Restore a soft-deleted document |
| `LIST` | List documents with typed results |
| `RAW_LIST` | List documents with raw results |

### Extending document composition

Add custom middleware to the default plan:

    :::python
    from forze.application.composition.document import (
        DocumentOperation,
    )
    from forze.application.execution import UsecasePlan


    def my_auth_guard(ctx):
        async def guard(args):
            if not is_authorized(ctx):
                raise PermissionError("Not authorized")
        return guard


    plan = (
        UsecasePlan()
        .before(DocumentOperation.CREATE, my_auth_guard, priority=100)
        .before(DocumentOperation.UPDATE, my_auth_guard, priority=100)
        .after_commit(DocumentOperation.CREATE, my_notification_effect)
    )

## Search composition

Search follows the same pattern:

    :::python
    from forze.application.composition.search import (
        SearchDTOs,
        SearchUsecasesFacade,
        build_search_registry,
    )

    search_dtos = SearchDTOs(read=ProjectReadModel)
    search_registry = build_search_registry(project_search_spec, search_dtos)

    facade = SearchUsecasesFacade(ctx=ctx, reg=search_registry)
    result = await facade.search(
        SearchRequestDTO(query="roadmap", limit=20)
    )

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the usecase:

    :::python
    from forze.application.mapping import DTOMapper, NumberIdStep, CreatorIdStep

    mapper = (
        build_document_create_mapper(project_spec, project_dtos)
        .with_steps(NumberIdStep(), CreatorIdStep())
    )

Each `MappingStep` can inject computed fields (like `number_id` from a counter or `creator_id` from the actor context) into the DTO before it reaches the create usecase.

## Custom usecases

You can register entirely custom usecases alongside the standard ones:

    :::python
    from forze.application.execution import Usecase, UsecasePlan, UsecaseRegistry
    from forze.domain.models import BaseDTO


    class ArchiveProjectArgs(BaseDTO):
        id: UUID
        rev: int


    class ArchiveProject(Usecase[ArchiveProjectArgs, ProjectReadModel]):
        async def main(self, args: ArchiveProjectArgs) -> ProjectReadModel:
            doc = self.ctx.doc_command(project_spec)
            return await doc.update(args.id, args.rev, UpdateProjectCmd(status="archived"))


    registry = build_document_registry(project_spec, project_dtos)
    registry = registry.register("archive", lambda ctx: ArchiveProject(ctx=ctx))

    plan = (
        UsecasePlan()
        .tx("archive", route="default")
        .before("archive", auth_guard, priority=100)
    )

The custom operation integrates into the same composition system as built-in operations and benefits from the same middleware infrastructure.

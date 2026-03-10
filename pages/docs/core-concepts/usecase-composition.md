# Usecase Composition

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

For document aggregates, `build_document_registry(spec)` creates a registry pre-populated with standard CRUD operations (GET, CREATE, UPDATE, KILL, DELETE, RESTORE).

## Usecase plan

The `UsecasePlan` describes how each operation is composed with middleware. It maps operation keys to middleware buckets that run at specific stages.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-composition.svg" alt="Operation composition flow">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-composition.svg" alt="Operation composition flow">
</div>

### Plan buckets

Each operation has seven middleware buckets, executed in this order:

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
        .tx("create")
        .tx("update")
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
    base_plan = build_document_plan()
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
        DocumentUsecasesFacadeProvider,
        build_document_plan,
        build_document_registry,
    )

    provider = DocumentUsecasesFacadeProvider(
        spec=project_spec,
        reg=build_document_registry(project_spec),
        plan=build_document_plan(),
        dtos={
            "read": ProjectReadModel,
            "create": CreateProjectCmd,
            "update": UpdateProjectCmd,
        },
    )

`build_document_registry(spec)` registers standard usecase factories for all `DocumentOperation` variants.

`build_document_plan()` returns a default plan with transaction wrapping for write operations.

The provider is a factory: call it with an `ExecutionContext` to get a typed facade:

    :::python
    facade = provider(ctx)
    project = await facade.create()(CreateProjectCmd(title="New"))
    fetched = await facade.get()(project.id)

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

### Extending document composition

Add custom middleware to the default plan:

    :::python
    from forze.application.composition.document import (
        DocumentOperation,
        build_document_plan,
    )


    def my_auth_guard(ctx):
        async def guard(args):
            if not is_authorized(ctx):
                raise PermissionError("Not authorized")
        return guard


    plan = (
        build_document_plan()
        .before(DocumentOperation.CREATE, my_auth_guard, priority=100)
        .before(DocumentOperation.UPDATE, my_auth_guard, priority=100)
        .after_commit(DocumentOperation.CREATE, my_notification_effect)
    )

## Search composition

Search follows the same pattern:

    :::python
    from forze.application.composition.search import (
        SearchUsecasesFacadeProvider,
        build_search_plan,
        build_search_registry,
    )

    search_provider = SearchUsecasesFacadeProvider(
        spec=project_search_spec,
        reg=build_search_registry(project_search_spec),
        plan=build_search_plan(),
        read_dto=ProjectReadModel,
    )

    facade = search_provider(ctx)
    hits, total = await facade.typed_search()(
        TypedSearchArgs(query="roadmap", limit=20)
    )

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the usecase:

    :::python
    from forze.application.mapping import DTOMapper, NumberIdStep, CreatorIdStep

    mapper = (
        build_document_create_mapper(project_spec)
        .with_steps(NumberIdStep(), CreatorIdStep())
    )

Each `MappingStep` can inject computed fields (like `number_id` from a counter or `creator_id` from the actor context) into the DTO before it reaches the create usecase.

## Custom usecases

You can register entirely custom usecases alongside the standard ones:

    :::python
    from forze.application.execution import Usecase, UsecaseRegistry


    class ArchiveProject(Usecase[UUID, ProjectReadModel]):
        async def main(self, args: UUID) -> ProjectReadModel:
            doc = self.ctx.doc_write(project_spec)
            return await doc.update(args, UpdateProjectCmd(status="archived"))


    registry = build_document_registry(project_spec)
    registry = registry.register("archive", lambda ctx: ArchiveProject(ctx=ctx))

    plan = (
        build_document_plan()
        .tx("archive")
        .before("archive", auth_guard, priority=100)
    )

The custom operation integrates into the same composition system as built-in operations and benefits from the same middleware infrastructure.

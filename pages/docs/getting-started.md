---
title: Getting Started
summary: Build a working CRUD service with Forze from scratch
---

This guide walks through building a working CRUD service with Forze from scratch. By the end you will have domain models, a document specification, a runtime with Postgres and Redis, and a FastAPI app serving HTTP endpoints.

## What you will build

- A `Project` document aggregate with create, update, and read models
- A `DocumentSpec` that tells adapters how to persist and cache projects
- An `ExecutionRuntime` with Postgres for storage and Redis for caching
- A FastAPI application exposing standard document operations

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/getting-started-flow.svg" alt="Getting started flow">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/getting-started-flow.svg" alt="Getting started flow">
</div>

## Step 1: Define domain models

Every aggregate starts with four model types: a **domain model**, a **read model**, a **create command**, and an **update command**.

    :::python
    from forze.domain.mixins import SoftDeletionMixin
    from forze.domain.models import (
        BaseDTO,
        CreateDocumentCmd,
        Document,
        ReadDocument,
    )


    class Project(SoftDeletionMixin, Document):
        title: str
        description: str


    class CreateProjectCmd(CreateDocumentCmd):
        title: str
        description: str


    class UpdateProjectCmd(BaseDTO):
        title: str | None = None
        description: str | None = None


    class ProjectReadModel(ReadDocument):
        title: str
        description: str
        is_deleted: bool = False

`Document` provides `id`, `rev`, `created_at`, and `last_update_at` out of the box.
`SoftDeletionMixin` adds the `is_deleted` flag and prevents updates on deleted documents.
`ReadDocument` mirrors the core fields as a frozen DTO for query results.

## Step 2: Declare a document specification

The specification is the single source of truth that binds your models to storage relations. Adapters read it to configure themselves.

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

| Field | Purpose |
|-------|---------|
| `namespace` | Cache key prefix and logical name for the aggregate |
| `read` | Source relation and model type used for queries |
| `write` | Source relation and model types used for mutations |
| `history` | Optional source relation for revision audit trail |
| `cache` | Optional cache settings (enable flag, TTL) |

## Step 3: Wire the runtime

The runtime has two parts: a **dependency plan** that assembles the container and a **lifecycle plan** that manages startup and shutdown of infrastructure clients.

    :::python
    from forze.application.execution import (
        Deps,
        DepsPlan,
        ExecutionRuntime,
        LifecyclePlan,
    )
    from forze_postgres import (
        PostgresClient,
        PostgresConfig,
        PostgresDepsModule,
        postgres_lifecycle_step,
    )
    from forze_redis import (
        RedisClient,
        RedisConfig,
        RedisDepsModule,
        redis_lifecycle_step,
    )


    postgres_client = PostgresClient()
    redis_client = RedisClient()

    postgres_module = PostgresDepsModule(
        client=postgres_client,
        rev_bump_strategy="database",
        history_write_strategy="database",
    )
    redis_module = RedisDepsModule(client=redis_client)

    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(postgres_module(), redis_module()),
    )

    lifecycle_plan = LifecyclePlan.from_steps(
        postgres_lifecycle_step(
            dsn="postgresql://app:app@localhost:5432/app",
            config=PostgresConfig(min_size=2, max_size=15),
        ),
        redis_lifecycle_step(
            dsn="redis://localhost:6379/0",
            config=RedisConfig(max_size=20),
        ),
    )

    runtime = ExecutionRuntime(deps=deps_plan, lifecycle=lifecycle_plan)

`DepsPlan.from_modules` accepts callables that return a `Deps` container. `Deps.merge` combines the containers from both modules, raising if any dependency key collides.

## Step 4: Create the FastAPI application

Build a registry that wires usecase factories for the spec, attach document routes to an `APIRouter`, then include that router on the app.

    :::python
    from fastapi import APIRouter, FastAPI

    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
        tx_document_plan,
    )
    from forze_fastapi.endpoints.document import attach_document_endpoints


    app = FastAPI(title="Projects API")
    projects_router = APIRouter(prefix="/projects", tags=["projects"])

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)
    registry.extend_plan(tx_document_plan, inplace=True)


    def context_dependency():
        return runtime.get_context()


    attach_document_endpoints(
        projects_router,
        document=project_spec,
        dtos=project_dtos,
        registry=registry,
        ctx_dep=context_dependency,
    )

    app.include_router(projects_router)

`attach_document_endpoints` registers standard document operations (get, list, create, update, and others depending on the spec). All routes resolve ports through `ExecutionContext` so business logic never imports adapter classes directly.

## Step 5: Run with the runtime scope

`ExecutionRuntime.scope()` creates the execution context, runs startup hooks (opening connection pools), yields, and then runs shutdown hooks on exit.

    :::python
    import uvicorn


    async def main() -> None:
        async with runtime.scope():
            config = uvicorn.Config(app, host="0.0.0.0", port=8000)
            server = uvicorn.Server(config)
            await server.serve()

## Step 6: Create the database schema

The document table must include core fields expected by the domain model. Column names match Pydantic field names.

    :::sql
    CREATE TABLE public.projects (
        id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
        rev         integer     NOT NULL DEFAULT 1,
        created_at  timestamptz NOT NULL DEFAULT now(),
        last_update_at timestamptz NOT NULL DEFAULT now(),
        is_deleted  boolean     NOT NULL DEFAULT false,
        title       text        NOT NULL,
        description text        NOT NULL
    );

When using `rev_bump_strategy="database"`, add a trigger that increments `rev` on every update:

    :::sql
    CREATE OR REPLACE FUNCTION bump_rev()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.rev := OLD.rev + 1;
        NEW.last_update_at := now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER projects_bump_rev
    BEFORE UPDATE ON public.projects
    FOR EACH ROW EXECUTE FUNCTION bump_rev();

Optionally add a history table for audit trails:

    :::sql
    CREATE TABLE public.projects_history (
        source text    NOT NULL,
        id     uuid    NOT NULL,
        rev    integer NOT NULL,
        data   jsonb   NOT NULL,
        PRIMARY KEY (source, id, rev)
    );

/// details | Full working example
    type: note

    :::python
    import asyncio
    from datetime import timedelta

    import uvicorn
    from fastapi import APIRouter, FastAPI

    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
        tx_document_plan,
    )
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution import (
        Deps,
        DepsPlan,
        ExecutionRuntime,
        LifecyclePlan,
    )
    from forze.domain.mixins import SoftDeletionMixin
    from forze.domain.models import (
        BaseDTO,
        CreateDocumentCmd,
        Document,
        ReadDocument,
    )
    from forze_fastapi.endpoints.document import attach_document_endpoints
    from forze_postgres import (
        PostgresClient,
        PostgresConfig,
        PostgresDepsModule,
        postgres_lifecycle_step,
    )
    from forze_redis import (
        RedisClient,
        RedisConfig,
        RedisDepsModule,
        redis_lifecycle_step,
    )


    class Project(SoftDeletionMixin, Document):
        title: str
        description: str


    class CreateProjectCmd(CreateDocumentCmd):
        title: str
        description: str


    class UpdateProjectCmd(BaseDTO):
        title: str | None = None
        description: str | None = None


    class ProjectReadModel(ReadDocument):
        title: str
        description: str
        is_deleted: bool = False


    project_spec = DocumentSpec(
        namespace="projects",
        read={
            "source": "public.projects",
            "model": ProjectReadModel,
        },
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

    pg = PostgresClient()
    redis = RedisClient()

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(
            lambda: Deps.merge(
                PostgresDepsModule(
                    client=pg,
                    rev_bump_strategy="database",
                    history_write_strategy="database",
                )(),
                RedisDepsModule(client=redis)(),
            ),
        ),
        lifecycle=LifecyclePlan.from_steps(
            postgres_lifecycle_step(
                dsn="postgresql://app:app@localhost:5432/app",
                config=PostgresConfig(min_size=2, max_size=15),
            ),
            redis_lifecycle_step(
                dsn="redis://localhost:6379/0",
                config=RedisConfig(max_size=20),
            ),
        ),
    )

    app = FastAPI(title="Projects API")
    projects_router = APIRouter(prefix="/projects", tags=["projects"])

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)
    registry.extend_plan(tx_document_plan, inplace=True)

    attach_document_endpoints(
        projects_router,
        document=project_spec,
        dtos=project_dtos,
        registry=registry,
        ctx_dep=lambda: runtime.get_context(),
    )

    app.include_router(projects_router)


    async def main() -> None:
        async with runtime.scope():
            server = uvicorn.Server(
                uvicorn.Config(app, host="0.0.0.0", port=8000)
            )
            await server.serve()


    if __name__ == "__main__":
        asyncio.run(main())
///

## Next steps

- [Core Concepts](core-concepts/index.md): understand the architecture, layers, and execution model
- [Core Package](core-package/index.md): query syntax, specifications, and advanced composition
- Integration guides: [FastAPI](integrations/fastapi.md) | [PostgreSQL](integrations/postgres.md) | [Redis](integrations/redis.md) | [S3](integrations/s3.md) | [MongoDB](integrations/mongo.md) | [Socket.IO](integrations/socketio.md) | [Temporal](integrations/temporal.md) | [Mock](integrations/mock.md)

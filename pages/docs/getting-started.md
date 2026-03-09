# Getting Started

This guide gives you a practical first path through Forze: model your document, compose dependencies, wire runtime, and expose HTTP routes.

## What you build

- A document aggregate with create/update/read DTOs
- A `DocumentSpec` that binds model types and storage sources
- An execution runtime with Postgres + Redis modules
- A FastAPI router powered by Forze usecases

<div class="d2-diagram">
  <img class="d2-light" src="assets/diagrams/light/getting-started-flow.svg" alt="Getting started flow">
  <img class="d2-dark" src="assets/diagrams/dark/getting-started-flow.svg" alt="Getting started flow">
</div>

## 1) Define models

Start with domain/read/command models. Keep domain logic in `Document`, and keep transport/payload details in DTOs.

    :::python
    from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument


    class Project(Document):
        title: str
        description: str
        is_deleted: bool = False


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

## 2) Declare `DocumentSpec`

The spec is the contract between your application layer and adapters. Keep it centralized.

    :::python
    from forze.application.contracts.document import DocumentSpec


    project_spec = DocumentSpec(
        namespace="projects",
        sources={
            "read": "public.projects",
            "write": "public.projects",
            "history": "public.projects_history",
        },
        models={
            "read": ProjectReadModel,
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        cache={"enabled": True},
    )

## 3) Compose dependencies and lifecycle

Use `DepsModule` implementations from integration packages, then merge them into one runtime plan.

    :::python
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_postgres import (
        PostgresClient,
        PostgresConfig,
        PostgresDepsModule,
        postgres_lifecycle_step,
    )
    from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step


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

## 4) Build document facade + FastAPI router

Register standard document operations, wrap them in a plan, then expose with `build_document_router`.

    :::python
    from fastapi import FastAPI

    from forze.application.composition.document import (
        DocumentUsecasesFacadeProvider,
        build_document_plan,
        build_document_registry,
    )
    from forze_fastapi.routers import build_document_router


    app = FastAPI(title="Projects API")

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

    def context_dependency():
        return runtime.get_context()

    app.include_router(
        build_document_router(
            prefix="/projects",
            tags=["projects"],
            provider=provider,
            context=context_dependency,
        )
    )

## 5) Run with runtime scope

`ExecutionRuntime` owns startup/shutdown hooks for clients. Keep app serving inside `runtime.scope()`.

    :::python
    import uvicorn


    async def main() -> None:
        async with runtime.scope():
            config = uvicorn.Config(app, host="0.0.0.0", port=8000)
            server = uvicorn.Server(config)
            await server.serve()

## 6) Verify schema assumptions (Postgres)

For document adapters, table fields should match core domain fields (`id`, `rev`, `created_at`, `last_update_at`) plus your custom fields.

    :::sql
    CREATE TABLE public.projects (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        rev integer NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        last_update_at timestamptz NOT NULL DEFAULT now(),
        is_deleted boolean NOT NULL DEFAULT false,
        title text NOT NULL,
        description text NOT NULL
    );

## Next steps

- Read [Core Concepts](core-concepts/index.md) for architecture and execution details
- Pick an integration guide:
  - [FastAPI](integrations/fastapi.md)
  - [Socket.IO](integrations/socketio.md)
  - [PostgreSQL](integrations/postgres.md)
  - [Redis](integrations/redis.md)
  - [S3](integrations/s3.md)
  - [MongoDB](integrations/mongo.md)
  - [Temporal](integrations/temporal.md)

# CRUD with FastAPI, Postgres, and Redis

Use this recipe when you want a standard HTTP CRUD API backed by Postgres with Redis caching.

## Ingredients

- Domain models and commands from [Domain Layer](../concepts/domain-layer.md)
- A `DocumentSpec` from [Aggregate Specification](../concepts/aggregate-specification.md)
- Postgres document adapters from [PostgreSQL Integration](../integrations/postgres.md)
- Redis cache adapters from [Redis / Valkey Integration](../integrations/redis.md)
- FastAPI endpoint helpers from [FastAPI Integration](../integrations/fastapi.md)

## Steps

1. Define the aggregate, create command, update DTO, and read model.
2. Create a `DocumentSpec` with `write` models and an optional `CacheSpec`.
3. Register the same logical name in `PostgresDepsModule.rw_documents`.
4. Register the cache name in `RedisDepsModule.caches`.
5. Build an `ExecutionRuntime` from the Postgres and Redis modules.
6. Attach document endpoints to a FastAPI `APIRouter`.

## Minimal shape

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec


    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

## Next details

For a complete code-first walkthrough, see [First project walkthrough](../first-project-walkthrough.md). For exact adapter configuration fields, use the [PostgreSQL](../integrations/postgres.md), [Redis](../integrations/redis.md), and [FastAPI](../integrations/fastapi.md) integration pages.

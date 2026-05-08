# Read-only document API

Use this recipe when another system owns writes and your service only exposes typed reads.

## Ingredients

- A read model from [Domain Layer](../concepts/domain-layer.md)
- A `DocumentSpec` with `write=None`
- A read-capable storage integration such as [PostgreSQL](../integrations/postgres.md) or [MongoDB](../integrations/mongo.md)
- Optional [FastAPI](../integrations/fastapi.md) read endpoints

## Steps

1. Define a `ReadDocument` subclass for the response shape.
2. Create a `DocumentSpec` with the logical name and read model only.
3. Register a read-only document route in the storage dependency module.
4. Expose `get`, `get_many`, or `find_many` operations from a usecase or FastAPI endpoint.

## Minimal shape

    :::python
    from forze.application.contracts.document import DocumentSpec


    project_read_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write=None,
    )

## Notes

Read-only specs are useful for projections, reporting APIs, and integration boundaries where writes flow through another service.

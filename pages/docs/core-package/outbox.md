# Outbox

The outbox feature implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for reliable event publishing. Events are collected during a usecase execution and persisted in the same transaction as the business data. A separate process then publishes them to the message broker.

## How it works

1. During a usecase, code pushes event drafts into a `ContextualBuffer`
2. The `OutboxBufferMiddleware` scopes the buffer to the usecase execution
3. The `FlushOutboxEffect` runs after commit and persists buffered events via `OutboxService`
4. A background poller reads unpublished events and publishes them to the broker

Steps 1–3 happen within the Forze middleware chain. Step 4 is implemented by the infrastructure layer.

## Event model

Outbox events are standard `Document` instances with topic, payload, and publishing metadata:

    :::python
    from forze.application.features.outbox import (
        OutboxEvent,
        CreateOutboxEventCmd,
        ReadOutboxEvent,
    )

### OutboxEvent fields

| Field | Type | Source | Purpose |
|-------|------|--------|---------|
| `id` | `UUID` | `Document` | Event identifier |
| `rev` | `int` | `Document` | Revision number |
| `created_at` | `datetime` | `Document` | When the event was created |
| `topic` | `str` | Immutable | Destination topic or channel |
| `payload` | `JsonDict` | Immutable | Event data |
| `key` | `str \| None` | Immutable | Optional routing key |
| `headers` | `Mapping[str, str]` | Immutable | Optional message headers |
| `published_at` | `datetime \| None` | Mutable | Set when the event is published |

### Command types

| Class | Purpose |
|-------|---------|
| `CreateOutboxEventCmd` | Create command with `topic`, `payload`, `key`, `headers` |
| `UpdateOutboxEventCmd` | Update command with `published_at` |
| `ReadOutboxEvent` | Read projection with all fields |

## OutboxService

Thin service wrapping a `DocumentWritePort` for outbox events:

    :::python
    from forze.application.features.outbox import OutboxService

| Method | Signature | Purpose |
|--------|-----------|---------|
| `append(draft)` | `(CreateOutboxEventCmd) -> ReadOutboxEvent` | Create a single event |
| `append_many(drafts)` | `(Sequence[CreateOutboxEventCmd]) -> Sequence[ReadOutboxEvent]` | Batch create |
| `mark_as_published(pk, *, rev)` | `(UUID, rev=int) -> ReadOutboxEvent` | Mark event as published |
| `mark_many_as_published(pks, *, revs)` | `(Sequence[UUID], revs=Sequence[int]) -> Sequence[ReadOutboxEvent]` | Batch mark |

## Middleware components

### OutboxBufferMiddleware

Scopes a `ContextualBuffer` to the usecase execution. Events pushed during the usecase are isolated from outer scopes:

    :::python
    from forze.application.features.outbox import OutboxBufferMiddleware

The middleware enters `buffer.scope()` before calling `next(args)`. On exit, the scope restores the previous buffer state.

### FlushOutboxEffect

After-commit effect that pops all events from the buffer and persists them via `OutboxService`:

    :::python
    from forze.application.features.outbox import FlushOutboxEffect

This effect runs after the transaction commits successfully, ensuring events are only persisted when business data is committed.

## Dependency keys and factories

| Symbol | Type | Purpose |
|--------|------|---------|
| `OutboxServiceDepKey` | `DepKey[OutboxService]` | Key for registering the outbox service |
| `OutboxBufferDepKey` | `DepKey[ContextualBuffer]` | Key for registering the outbox buffer |
| `OutboxBuffer` | `ContextualBuffer[CreateOutboxEventCmd]` | Singleton buffer instance |

### Factory functions

| Function | Purpose |
|----------|---------|
| `build_outbox_service(ctx, spec)` | Build an `OutboxService` from context and outbox spec |
| `build_outbox_buffer_middleware(ctx)` | Build the buffer middleware from context |
| `build_flush_outbox_effect(ctx)` | Build the flush effect from context |

## OutboxSpec

Pre-configured `DocumentSpec` for outbox events:

    :::python
    from forze.application.features.outbox import OutboxSpec

    # OutboxSpec is DocumentSpec[ReadOutboxEvent, OutboxEvent, CreateOutboxEventCmd, UpdateOutboxEventCmd]

Use this when declaring the outbox table/collection in your adapter configuration.

## Wiring the outbox

Complete example integrating the outbox into a document aggregate:

    :::python
    from forze.application.contracts.deps import DepKey
    from forze.application.execution import Deps, DepsPlan, UsecasePlan
    from forze.application.composition.document import (
        DocumentOperation,
        build_document_plan,
    )
    from forze.application.features.outbox import (
        OutboxBuffer,
        OutboxBufferDepKey,
        OutboxServiceDepKey,
        build_flush_outbox_effect,
        build_outbox_buffer_middleware,
        build_outbox_service,
    )

    # 1. Register outbox dependencies
    def outbox_module(outbox_spec) -> Deps:
        return Deps(deps={
            OutboxBufferDepKey: OutboxBuffer,
            OutboxServiceDepKey: lambda ctx: build_outbox_service(ctx, outbox_spec),
        })

    # 2. Add outbox middleware to the plan
    plan = (
        build_document_plan()
        .wrap(DocumentOperation.CREATE, build_outbox_buffer_middleware, priority=50)
        .after_commit(DocumentOperation.CREATE, build_flush_outbox_effect)
    )

    # 3. Push events during usecase execution
    class CreateProject(Usecase[CreateProjectCmd, ProjectRead]):
        async def main(self, args: CreateProjectCmd) -> ProjectRead:
            doc = self.ctx.doc_write(project_spec)
            result = await doc.create(args)

            # Push an event to the outbox buffer
            buf = self.ctx.dep(OutboxBufferDepKey)
            buf.push([
                CreateOutboxEventCmd(
                    topic="projects.created",
                    payload={"id": str(result.id), "title": result.title},
                )
            ])

            return result

The flow at runtime:

1. `OutboxBufferMiddleware` enters a buffer scope
2. The usecase runs inside a transaction, pushing events to the buffer
3. Transaction commits
4. `FlushOutboxEffect` pops events from the buffer and persists them via `OutboxService`
5. A background process picks up unpublished events and publishes them

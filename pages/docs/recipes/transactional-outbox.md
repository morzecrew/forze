# Transactional outbox

Publish integration events reliably with the outbox contracts and Postgres or Mongo adapters.

## Checklist

1. Create the outbox store (Postgres table or Mongo collection + indexes; see [Outbox contracts](../core-package/contracts/outbox.md)).
2. Register `OutboxSpec` and `PostgresOutboxConfig` or `MongoOutboxConfig` on the deps module.
3. Register a `QueueSpec` for relay targets.
4. Patch mutating operations with `bind_tx()` and `outbox_flush_tx_on_success_factory` (`.set_route("postgres")` or `"mongo")`).
5. Run `relay_outbox_to_queue` from a background worker—or opt into `outbox_relay_background_lifecycle_step`.

## Postgres example

```python
from pydantic import BaseModel

from forze_kits.integrations.outbox import (
    outbox_flush_tx_on_success_factory,
    relay_outbox_to_queue,
)
from forze.application.contracts.execution import OnSuccessStep
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.base.serialization import PydanticRecordMappingCodec
from forze_postgres import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresOutboxConfig

class ProjectCreated(BaseModel):
    project_id: str

events_spec = OutboxSpec(
    name="events",
    codec=PydanticRecordMappingCodec(ProjectCreated),
    destination=OutboxDestination(queue_route="jobs", queue="jobs"),
)
jobs_spec = QueueSpec(name="jobs", codec=events_spec.codec)

pg_module = PostgresDepsModule(
    client=pg_client,
    tx={"default"},
    outboxes={
        "events": PostgresOutboxConfig(relation=("app", "outbox")),
    },
)
```

## Mongo example

Mongo outbox flush participates in the same **replica-set transaction** as document writes.

```python
from forze_mongo import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoOutboxConfig

mongo_module = MongoDepsModule(
    client=mongo_client,
    tx={"default"},
    outboxes={
        "events": MongoOutboxConfig(collection=("app", "outbox")),
    },
)
```

Wire flush with `.set_route("mongo")` on patched operations.

## Handler

```python
async def create_project(self, cmd: CreateProjectCmd) -> ProjectRead:
    project = await self.doc.create(cmd)
    await self.ctx.outbox.command(events_spec).stage(
        "project.created",
        ProjectCreated(project_id=str(project.id)),
    )
    return project
```

## Worker loop

Run relay on a schedule (cron, Temporal activity, asyncio task, or optional lifecycle step). Tune `reclaim_stale_after` to exceed your worst-case relay duration.

```python
from datetime import timedelta

from forze_kits.integrations.outbox import relay_outbox_to_queue

async def relay_pending(ctx: ExecutionContext) -> None:
    result = await relay_outbox_to_queue(
        ctx,
        outbox_spec=events_spec,
        queue_spec=jobs_spec,
        reclaim_stale_after=timedelta(minutes=5),
    )
    # Log result.claimed, result.published, result.failed, result.reclaimed
```

### Optional background lifecycle step

For long-running processes (not serverless):

```python
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step
from forze.application.execution import LifecyclePlan

lifecycle = LifecyclePlan.from_steps(
    mongo_lifecycle_step(...),
    outbox_relay_background_lifecycle_step(
        outbox_spec=events_spec,
        queue_spec=jobs_spec,
        interval=timedelta(seconds=30),
    ),
)
```

Production deployments often prefer external schedulers over in-process polling.

## Consumer idempotency

Relay delivers **at-least-once** and passes `key=str(event_id)` to the queue when supported. Handlers should deduplicate on `IntegrationEvent.event_id`.

## Failed rows

Query `status = 'failed'` and `last_error`, fix the cause, then:

```python
await ctx.outbox.query(events_spec).requeue_failed([row_id])
```

Or stage with a new `event_id` if the unique key must change.

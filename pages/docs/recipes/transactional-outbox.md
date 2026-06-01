# Transactional outbox

Publish integration events reliably with the outbox contracts and Postgres adapter.

## Checklist

1. Create the outbox table (see [Outbox contracts](../core-package/contracts/outbox.md#postgres-ddl-application-owned)), including `processing_at` for relay reclaim.
2. Register `OutboxSpec` and `PostgresOutboxConfig` on `PostgresDepsModule`.
3. Register a `QueueSpec` for relay targets.
4. Patch mutating operations with `bind_tx()` and `outbox_flush_tx_on_success_factory`.
5. Run `relay_outbox_to_queue` from a background worker.

## Example

```python
from pydantic import BaseModel

from forze.application.composition.outbox import (
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

In the handler:

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

Run relay on a schedule (cron, Temporal activity, or asyncio task). Tune `reclaim_stale_after` to exceed your worst-case relay duration (see `PostgresOutboxConfig.default_processing_lease`).

```python
from datetime import timedelta

async def relay_pending(ctx: ExecutionContext) -> None:
    result = await relay_outbox_to_queue(
        ctx,
        outbox_spec=events_spec,
        queue_spec=jobs_spec,
        reclaim_stale_after=timedelta(minutes=5),
    )
    # Log result.claimed, result.published, result.failed, result.reclaimed
```

Pass `reclaim_stale_after=None` if you reclaim stale rows in a separate maintenance job.

## Consumer idempotency

Relay delivers **at-least-once**. Handlers should treat `IntegrationEvent.event_id` (or an envelope field you copy into the queue message) as the deduplication key.

## DDL migration

If the outbox table predates reclaim support:

```sql
ALTER TABLE app.outbox ADD COLUMN IF NOT EXISTS processing_at TIMESTAMPTZ;
```

## Failed rows

Query `status = 'failed'` and `last_error` for operational review. Automatic retry is not built in—use a new `event_id`, fix the payload, or update/delete the row before re-staging.

# Durable workflow schedule contracts

Durable workflow schedule contracts manage **recurring or timed workflow starts** as
first-class resources, separate from individual workflow runs (`DurableWorkflowHandle`).
They apply to the [durable workflow](durable-workflow.md) kind (Temporal Schedules today).

## `DurableWorkflowScheduleTiming`

| Field | Purpose |
|-------|---------|
| `cron_expressions` | Cron strings (provider interprets timezone; Temporal defaults to UTC). |
| `interval` | Fixed delay between runs. |
| `start_at` / `end_at` | Optional window when the schedule may fire. |
| `jitter` | Optional random delay bound per fire. |
| `timezone` | Reserved for providers with named timezone support. |

At least one of `cron_expressions` or `interval` is required.

Import path:

    :::python
    from forze.application.contracts.durable.workflow import DurableWorkflowScheduleTiming

## `DurableWorkflowScheduleHandle`

Identifies a schedule resource (`schedule_id`). Tenant-aware adapters may
prefix ids with `tenant:{tenant_id}:`.

## `DurableWorkflowScheduleBootstrap`

Declarative schedule registered on application startup (see
[Temporal integration](../../integrations/temporal.md#schedule-bootstrap)).

| Field | Purpose |
|-------|---------|
| `workflow_name` | Route key matching `DurableWorkflowSpec.name`. |
| `schedule_id` | Stable schedule id. |
| `default_args` | Pydantic model instance for each fired run. |
| `timing` | When the schedule fires. |
| `workflow_id_template` | Optional workflow id for each run (see Temporal adapter docs). |
| `trigger_immediately` | Fire once right after create/upsert. |
| `note` | Optional operator note. |

## `DurableWorkflowScheduleCommandPort[In]`

| Method | Purpose |
|--------|---------|
| `create` | Create a schedule (conflict if it exists). |
| `upsert` | Create or update. |
| `update` | Partial update on an existing handle. |
| `delete` | Remove the schedule. |
| `pause` / `unpause` | Pause or resume firing. |
| `trigger` | Fire immediately. |

## `DurableWorkflowScheduleQueryPort[In]`

| Method | Purpose |
|--------|---------|
| `describe` | Return `DurableWorkflowScheduleDescription` (paused, timing, next runs). |
| `list` | Paginated schedules for this workflow (`limit`, `next_page_token`). |

## Dependency keys

| Key | Purpose |
|-----|---------|
| `DurableWorkflowScheduleCommandDepKey` | Routed factory → command port (`route=spec.name`). |
| `DurableWorkflowScheduleQueryDepKey` | Routed factory → query port (`route=spec.name`). |

## Handler resolution

    :::python
    from forze.application.contracts.durable.workflow import (
        DurableWorkflowScheduleCommandDepKey,
        DurableWorkflowScheduleQueryDepKey,
    )

    schedules = ctx.deps.resolve_configurable(
        ctx,
        DurableWorkflowScheduleCommandDepKey,
        workflow_spec,
        route=workflow_spec.name,
    )
    handle = await schedules.upsert(
        "nightly-sync",
        StartArgs(...),
        DurableWorkflowScheduleTiming(cron_expressions=("0 2 * * *",)),
    )

## Related pages

- [Durable workflow](durable-workflow.md)
- [Durable](durable.md)
- [Temporal](../../integrations/temporal.md)
- [Background Workflow](../../recipes/background-workflow.md)

---
name: forze-temporal-workflows
description: >-
  Wires and uses Forze durable workflow contracts with DurableWorkflowSpec,
  DurableWorkflowCommandDepKey, DurableWorkflowQueryDepKey, TemporalDepsModule,
  lifecycle, context propagation, tenant-aware workflow IDs, and Temporal tests.
  Use when orchestrating long-running workflows.
---

# Forze Temporal workflows

Use when starting, signaling, updating, querying, or testing workflow-backed handlers. The core application depends on durable workflow contracts; `forze_temporal` supplies the Temporal adapter.

## Workflow spec

```python
from enum import StrEnum

from forze.application.contracts.durable.workflow import (
    DurableWorkflowInvokeSpec,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
)


class WorkflowName(StrEnum):
    PROJECT_ONBOARDING = "project-onboarding"


project_onboarding = DurableWorkflowSpec(
    name=WorkflowName.PROJECT_ONBOARDING,
    run=DurableWorkflowInvokeSpec(args_type=StartOnboarding, return_type=OnboardingResult),
    signals={
        "step_completed": DurableWorkflowSignalSpec(
            name="step_completed",
            args_type=StepCompleted,
        )
    },
)
```

The spec name is the dependency route and should match `TemporalDepsModule.workflows`.

## Runtime wiring

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_temporal import (
    TemporalClient,
    TemporalConfig,
    TemporalDepsModule,
    temporal_lifecycle_step,
)

temporal_client = TemporalClient()
temporal_module = TemporalDepsModule(
    client=temporal_client,
    workflows={
        WorkflowName.PROJECT_ONBOARDING: {
            "queue": "project-tasks",
            "tenant_aware": True,
        }
    },
)

deps = DepsRegistry.from_modules(temporal_module)
lifecycle = LifecyclePlan.from_steps(
    temporal_lifecycle_step(
        host="localhost:7233",
        config=TemporalConfig(namespace="default"),
    )
)
```

If `workflows` is empty, only `TemporalClientDepKey` is registered.

## Handler resolution

There is no `ctx.workflow_command(...)` helper. Resolve workflow ports with `ctx.deps.resolve_configurable`.

```python
import attrs

from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepKey,
    DurableWorkflowCommandPort,
    DurableWorkflowHandle,
    DurableWorkflowQueryDepKey,
    DurableWorkflowQueryPort,
    DurableWorkflowRunDescription,
)
from forze.application.contracts.execution import Handler


@attrs.define(slots=True, kw_only=True, frozen=True)
class StartProjectOnboarding(Handler[StartOnboarding, DurableWorkflowHandle]):
    commands: DurableWorkflowCommandPort

    async def __call__(self, args: StartOnboarding) -> DurableWorkflowHandle:
        return await self.commands.start(
            args,
            workflow_id=f"project:{args.project_id}",
        )


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetOnboardingStatus(Handler[DurableWorkflowHandle, DurableWorkflowRunDescription]):
    queries: DurableWorkflowQueryPort

    async def __call__(self, args: DurableWorkflowHandle) -> DurableWorkflowRunDescription:
        return await self.queries.describe(args)


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetOnboardingResult(Handler[DurableWorkflowHandle, OnboardingResult]):
    queries: DurableWorkflowQueryPort

    async def __call__(self, args: DurableWorkflowHandle) -> OnboardingResult:
        return await self.queries.result(args)


# Register on OperationRegistry:
# lambda ctx: StartProjectOnboarding(
#     commands=ctx.deps.resolve_configurable(
#         ctx, DurableWorkflowCommandDepKey, project_onboarding, route=project_onboarding.name
#     ),
# )
```

## Context and tenancy

Use `ExecutionContextInterceptor` and `TemporalContextCodec` when `InvocationMetadata`, `AuthnIdentity`, or `TenantIdentity` must cross Temporal client/worker boundaries. For tenant-aware workflow config, bind `TenantIdentity` before starting workflows so generated IDs can include tenant scope.

## Workflow schedules (Temporal Schedules API)

Resolve schedule ports the same way as workflow command/query ports:

```python
from datetime import timedelta

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleCommandDepKey,
    DurableWorkflowScheduleTiming,
)

schedules = ctx.deps.resolve_configurable(
    ctx,
    DurableWorkflowScheduleCommandDepKey,
    project_onboarding,
    route=project_onboarding.name,
)
await schedules.upsert(
    "nightly-onboarding",
    StartOnboarding(project_id="p1"),
    DurableWorkflowScheduleTiming(cron_expressions=("0 2 * * *",)),
)
```

Declarative bootstrap on deploy:

```python
from forze.application.contracts.durable.workflow import DurableWorkflowScheduleBootstrap

TemporalDepsModule(
    client=client,
    workflows={WorkflowName.PROJECT_ONBOARDING: {"queue": "project-tasks"}},
    schedule_bootstraps=[
        DurableWorkflowScheduleBootstrap(
            workflow_name=WorkflowName.PROJECT_ONBOARDING,
            schedule_id="nightly",
            default_args=StartOnboarding(project_id="default"),
            timing=DurableWorkflowScheduleTiming(interval=timedelta(hours=24)),
        ),
    ],
)

# Pass the same workflow config map to lifecycle:
temporal_lifecycle_step(host="...", workflow_configs={...})
```

Schedules require a Temporal server with the Schedules API (not the time-skipping test environment).

## Testing

For handlers that only need to verify a workflow command was issued, register fake `DurableWorkflowCommandDepKey` / `DurableWorkflowQueryDepKey` factories in `Deps` (for example via `MockDepsModule` or a test `DepsRegistry`). Schedule handlers can use fake `DurableWorkflowScheduleCommandDepKey` / `DurableWorkflowScheduleQueryDepKey` factories similarly. For workflow definition tests, follow the [Temporal integration](https://morzecrew.github.io/forze/latest/integrations/temporal/) testing section.

## Anti-patterns

1. **Looking for `ctx.workflow_*` helpers** — use `ctx.deps.resolve_configurable(ctx, DurableWorkflowCommandDepKey, spec, route=spec.name)`.
2. **Putting Temporal task queues in `DurableWorkflowSpec`** — queues belong in `TemporalWorkflowConfig`.
3. **Using raw Temporal SDK types in handlers** — keep handlers on `DurableWorkflowSpec`, ports, and Pydantic DTOs.
4. **Skipping lifecycle** — the client connects through `temporal_lifecycle_step`.
5. **Ignoring context propagation in workers** — register interceptors on both client and worker when identity/correlation matters.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Temporal integration](https://morzecrew.github.io/forze/latest/integrations/temporal/)
- [Durable workflow contracts](https://morzecrew.github.io/forze/latest/reference/contracts/)
- [`forze-wiring`](../forze-wiring/SKILL.md)

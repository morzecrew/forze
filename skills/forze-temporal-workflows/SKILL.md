---
name: forze-temporal-workflows
description: >-
  Wires and uses Forze workflow contracts with WorkflowSpec,
  WorkflowCommandDepKey, WorkflowQueryDepKey, TemporalDepsModule, lifecycle,
  context propagation, tenant-aware workflow IDs, and Temporal tests. Use when
  orchestrating long-running workflows.
---

# Forze Temporal workflows

Use when starting, signaling, updating, querying, or testing workflow-backed handlers. The core application depends on workflow contracts; `forze_temporal` supplies the Temporal adapter.

## Workflow spec

```python
from enum import StrEnum

from forze.application.contracts.workflow import (
    WorkflowInvokeSpec,
    WorkflowSignalSpec,
    WorkflowSpec,
)


class WorkflowName(StrEnum):
    PROJECT_ONBOARDING = "project-onboarding"


project_onboarding = WorkflowSpec(
    name=WorkflowName.PROJECT_ONBOARDING,
    run=WorkflowInvokeSpec(args_type=StartOnboarding, return_type=OnboardingResult),
    signals={
        "step_completed": WorkflowSignalSpec(
            name="step_completed",
            args_type=StepCompleted,
        )
    },
)
```

The spec name is the dependency route and should match `TemporalDepsModule.workflows`.

## Runtime wiring

```python
from forze.application.execution import DepsPlan, LifecyclePlan
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

deps = DepsPlan.from_modules(temporal_module)
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

from forze.application.contracts.execution import Handler
from forze.application.contracts.workflow import WorkflowCommandDepKey, WorkflowQueryDepKey


@attrs.define(slots=True, kw_only=True, frozen=True)
class StartProjectOnboarding(Handler[StartOnboarding, WorkflowHandle]):
    commands: WorkflowCommandPort

    async def __call__(self, args: StartOnboarding) -> WorkflowHandle:
        return await self.commands.start(
            args,
            workflow_id=f"project:{args.project_id}",
        )


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetOnboardingResult(Handler[WorkflowHandle, OnboardingResult]):
    queries: WorkflowQueryPort

    async def __call__(self, args: WorkflowHandle) -> OnboardingResult:
        return await self.queries.result(args)


# Register on OperationRegistry:
# lambda ctx: StartProjectOnboarding(
#     commands=ctx.deps.resolve_configurable(
#         ctx, WorkflowCommandDepKey, project_onboarding, route=project_onboarding.name
#     ),
# )
```

## Context and tenancy

Use `ExecutionContextInterceptor` and `TemporalContextCodec` when `InvocationMetadata`, `AuthnIdentity`, or `TenantIdentity` must cross Temporal client/worker boundaries. For tenant-aware workflow config, bind `TenantIdentity` before starting workflows so generated IDs can include tenant scope.

## Workflow schedules (Temporal Schedules API)

Resolve schedule ports the same way as workflow command/query ports:

```python
from datetime import timedelta

from forze.application.contracts.workflow import (
    WorkflowScheduleCommandDepKey,
    WorkflowScheduleTiming,
)

schedules = ctx.deps.resolve_configurable(
    ctx,
    WorkflowScheduleCommandDepKey,
    project_onboarding,
    route=project_onboarding.name,
)
await schedules.upsert(
    "nightly-onboarding",
    StartOnboarding(project_id="p1"),
    WorkflowScheduleTiming(cron_expressions=("0 2 * * *",)),
)
```

Declarative bootstrap on deploy:

```python
from forze.application.contracts.workflow import WorkflowScheduleBootstrap

TemporalDepsModule(
    client=client,
    workflows={WorkflowName.PROJECT_ONBOARDING: {"queue": "project-tasks"}},
    schedule_bootstraps=[
        WorkflowScheduleBootstrap(
            workflow_name=WorkflowName.PROJECT_ONBOARDING,
            schedule_id="nightly",
            default_args=StartOnboarding(project_id="default"),
            timing=WorkflowScheduleTiming(interval=timedelta(hours=24)),
        ),
    ],
)

# Pass the same workflow config map to lifecycle:
temporal_lifecycle_step(host="...", workflow_configs={...})
```

Schedules require a Temporal server with the Schedules API (not the time-skipping test environment).

## Testing

Use the repository’s Temporal testing patterns when workflow code itself is under test. For handlers that only need to verify a workflow command was issued, register fake `WorkflowCommandDepKey` / `WorkflowQueryDepKey` factories in `Deps`. Schedule handlers can use fake `WorkflowScheduleCommandDepKey` / `WorkflowScheduleQueryDepKey` factories similarly.

## Anti-patterns

1. **Looking for `ctx.workflow_*` helpers** — use `ctx.deps.resolve_configurable(ctx, WorkflowCommandDepKey, spec, route=spec.name)`.
2. **Putting Temporal task queues in `WorkflowSpec`** — queues belong in `TemporalWorkflowConfig`.
3. **Using raw Temporal SDK types in handlers** — keep handlers on `WorkflowSpec`, ports, and Pydantic DTOs.
4. **Skipping lifecycle** — the client connects through `temporal_lifecycle_step`.
5. **Ignoring context propagation in workers** — register interceptors on both client and worker when identity/correlation matters.

## Reference

- [`pages/docs/integrations/temporal.md`](../../pages/docs/integrations/temporal.md)
- [`src/forze/application/contracts/workflow`](../../src/forze/application/contracts/workflow)
- [`src/forze_temporal/execution/deps/module.py`](../../src/forze_temporal/execution/deps/module.py)
- [`tests/unit/test_forze_temporal`](../../tests/unit/test_forze_temporal)

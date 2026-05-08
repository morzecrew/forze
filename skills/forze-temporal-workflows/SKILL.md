---
name: forze-temporal-workflows
description: >-
  Wires and uses Forze workflow contracts with WorkflowSpec,
  WorkflowCommandDepKey, WorkflowQueryDepKey, TemporalDepsModule, lifecycle,
  context propagation, tenant-aware workflow IDs, and Temporal tests. Use when
  orchestrating long-running workflows.
---

# Forze Temporal workflows

Use when starting, signaling, updating, querying, or testing workflow-backed usecases. The core application depends on workflow contracts; `forze_temporal` supplies the Temporal adapter.

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

## Usecase resolution

There is no `ctx.workflow_command(...)` helper. Resolve the routed factory explicitly.

```python
from forze.application.contracts.workflow import WorkflowCommandDepKey, WorkflowQueryDepKey


class StartProjectOnboarding(Usecase[StartOnboarding, WorkflowHandle]):
    async def main(self, args: StartOnboarding) -> WorkflowHandle:
        factory = self.ctx.dep(
            WorkflowCommandDepKey,
            route=project_onboarding.name,
        )
        workflow = factory(self.ctx, project_onboarding)
        return await workflow.start(args, workflow_id=f"project:{args.project_id}")


class GetOnboardingResult(Usecase[WorkflowHandle, OnboardingResult]):
    async def main(self, args: WorkflowHandle) -> OnboardingResult:
        factory = self.ctx.dep(WorkflowQueryDepKey, route=project_onboarding.name)
        workflow = factory(self.ctx, project_onboarding)
        return await workflow.result(args)
```

## Context and tenancy

Use `ExecutionContextInterceptor` and `TemporalContextCodec` when `CallContext`, `AuthnIdentity`, or `TenantIdentity` must cross Temporal client/worker boundaries. For tenant-aware workflow config, bind `TenantIdentity` before starting workflows so generated IDs can include tenant scope.

## Testing

Use the repository’s Temporal testing patterns when workflow code itself is under test. For usecases that only need to verify a workflow command was issued, register fake `WorkflowCommandDepKey` / `WorkflowQueryDepKey` factories in `Deps`.

## Anti-patterns

1. **Looking for `ctx.workflow_*` helpers** — use `ctx.dep(..., route=spec.name)`.
2. **Putting Temporal task queues in `WorkflowSpec`** — queues belong in `TemporalWorkflowConfig`.
3. **Using raw Temporal SDK types in usecases** — keep usecases on `WorkflowSpec`, ports, and Pydantic DTOs.
4. **Skipping lifecycle** — the client connects through `temporal_lifecycle_step`.
5. **Ignoring context propagation in workers** — register interceptors on both client and worker when identity/correlation matters.

## Reference

- [`pages/docs/integrations/temporal.md`](../../pages/docs/integrations/temporal.md)
- [`src/forze/application/contracts/workflow`](../../src/forze/application/contracts/workflow)
- [`src/forze_temporal/execution/deps/module.py`](../../src/forze_temporal/execution/deps/module.py)
- [`tests/unit/test_forze_temporal`](../../tests/unit/test_forze_temporal)

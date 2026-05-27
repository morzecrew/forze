# Durable workflow contracts

Durable workflow contracts describe and interact with **Temporal-style** long-running
orchestration engines. They live under the [Durable](durable.md) family and separate
command operations (start/signal/update/cancel) from query operations (workflow
queries and result retrieval).

## `DurableWorkflowSpec[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Names a workflow and its run, signal, query, and update invocation specs. |
| Import path | `from forze.application.contracts.durable.workflow import DurableWorkflowSpec` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required fields | `name`, `run`; optional `signals`, `queries`, `updates`. |
| Returned values | Passed to workflow dep factories to build command/query ports. |
| Common implementations | Temporal workflow adapter (`forze_temporal`). |
| Related dependency keys | `DurableWorkflowCommandDepKey`, `DurableWorkflowQueryDepKey`. |
| Minimal example | See below. |
| Related pages | [Temporal](../../integrations/temporal.md), [Background Workflow](../../recipes/background-workflow.md). |

## Invocation specs

| Type | Purpose | Required fields | Return value semantics |
|------|---------|-----------------|------------------------|
| `DurableWorkflowInvokeSpec[In, Out]` | Describes the main workflow run invocation. | `args_type`; optional `return_type`. | `return_type` describes the workflow result. |
| `DurableWorkflowSignalSpec[In]` | Describes a signal sent to a running workflow. | `name`, `args_type`. | Signals do not return values. |
| `DurableWorkflowQuerySpec[In, Out]` | Describes a read-only workflow query. | `name`, `args_type`; optional `return_type`. | Query returns `Out`. |
| `DurableWorkflowUpdateSpec[In, Out]` | Describes a workflow update. | `name`, `args_type`; optional `return_type`. | Update returns `Out`. |

Import path for all invocation specs:

    :::python
    from forze.application.contracts.durable.workflow.specs import (
        DurableWorkflowInvokeSpec,
        DurableWorkflowQuerySpec,
        DurableWorkflowSignalSpec,
        DurableWorkflowUpdateSpec,
    )

## `DurableWorkflowCommandPort[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Starts a workflow and sends mutating interactions to a running workflow. |
| Import path | `from forze.application.contracts.durable.workflow import DurableWorkflowCommandPort` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required methods | `start`, `signal`, `update`, `cancel`, `terminate`. |
| Returned values | `DurableWorkflowHandle`, `None`, or the update result model. |
| Common implementations | Temporal workflow command adapter. |
| Related dependency keys | `DurableWorkflowCommandDepKey`. |
| Minimal example | `handle = await commands.start(StartArgs(...))` |
| Related pages | [Execution](../../reference/execution.md). |

## `DurableWorkflowQueryPort[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Queries running workflows and awaits workflow completion results. |
| Import path | `from forze.application.contracts.durable.workflow import DurableWorkflowQueryPort` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required methods | `query`, `result`. |
| Returned values | Query result model or workflow result model. |
| Common implementations | Temporal workflow query adapter. |
| Related dependency keys | `DurableWorkflowQueryDepKey`. |
| Minimal example | `result = await queries.result(handle)` |
| Related pages | [Temporal](../../integrations/temporal.md). |

## `DurableWorkflowHandle`

| Section | Details |
|---------|---------|
| Purpose | Identifies a workflow execution. |
| Import path | `from forze.application.contracts.durable.workflow import DurableWorkflowHandle` |
| Type parameters | None. |
| Required fields | `workflow_id`; optional `run_id`. |
| Returned values | Returned by `DurableWorkflowCommandPort.start`. |
| Common implementations | attrs value object used by workflow adapters. |
| Related dependency keys | Returned through `DurableWorkflowCommandDepKey` implementations. |
| Minimal example | `DurableWorkflowHandle(workflow_id="project-p1")` |
| Related pages | [Contracts overview](../contracts.md). |

    :::python
    from forze.application.contracts.durable.workflow import (
        DurableWorkflowCommandDepKey,
        DurableWorkflowSpec,
    )
    from forze.application.contracts.durable.workflow.specs import (
        DurableWorkflowInvokeSpec,
    )

    workflow_spec = DurableWorkflowSpec(
        name="project-onboarding",
        run=DurableWorkflowInvokeSpec(
            args_type=StartOnboarding,
            return_type=OnboardingResult,
        ),
    )
    commands = ctx.deps.resolve_configurable(
        ctx,
        DurableWorkflowCommandDepKey,
        workflow_spec,
        route=workflow_spec.name,
    )
    handle = await commands.start(StartOnboarding(project_id="p1"))

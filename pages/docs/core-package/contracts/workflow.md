# Workflow contracts

Workflow contracts describe and interact with long-running orchestration engines.
They separate command operations such as start/signal/update/cancel from query
operations such as workflow queries and result retrieval.

## `WorkflowSpec[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Names a workflow and its run, signal, query, and update invocation specs. |
| Import path | `from forze.application.contracts.workflow import WorkflowSpec` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required fields | `name`, `run`; optional `signals`, `queries`, `updates`. |
| Returned values | Passed to workflow dep factories to build command/query ports. |
| Common implementations | Temporal workflow adapter. |
| Related dependency keys | `WorkflowCommandDepKey`, `WorkflowQueryDepKey`. |
| Minimal example | See below. |
| Related pages | [Temporal](../../integrations/temporal.md), [Background Workflow](../../recipes/background-workflow.md). |

## Invocation specs

| Type | Purpose | Required fields | Return value semantics |
|------|---------|-----------------|------------------------|
| `WorkflowInvokeSpec[In, Out]` | Describes the main workflow run invocation. | `args_type`; optional `return_type`. | `return_type` describes the workflow result. |
| `WorkflowSignalSpec[In]` | Describes a signal sent to a running workflow. | `name`, `args_type`. | Signals do not return values. |
| `WorkflowQuerySpec[In, Out]` | Describes a read-only workflow query. | `name`, `args_type`; optional `return_type`. | Query returns `Out`. |
| `WorkflowUpdateSpec[In, Out]` | Describes a workflow update. | `name`, `args_type`; optional `return_type`. | Update returns `Out`. |

Import path for all invocation specs:

    :::python
    from forze.application.contracts.workflow.specs import (
        WorkflowInvokeSpec,
        WorkflowQuerySpec,
        WorkflowSignalSpec,
        WorkflowUpdateSpec,
    )

## `WorkflowCommandPort[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Starts a workflow and sends mutating interactions to a running workflow. |
| Import path | `from forze.application.contracts.workflow import WorkflowCommandPort` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required methods | `start`, `signal`, `update`, `cancel`, `terminate`. |
| Returned values | `WorkflowHandle`, `None`, or the update result model. |
| Common implementations | Temporal workflow command adapter. |
| Related dependency keys | `WorkflowCommandDepKey`. |
| Minimal example | `handle = await commands.start(StartArgs(...))` |
| Related pages | [Execution](../execution.md). |

## `WorkflowQueryPort[In, Out]`

| Section | Details |
|---------|---------|
| Purpose | Queries running workflows and awaits workflow completion results. |
| Import path | `from forze.application.contracts.workflow import WorkflowQueryPort` |
| Type parameters | `In` run argument model, `Out` workflow result model. |
| Required methods | `query`, `result`. |
| Returned values | Query result model or workflow result model. |
| Common implementations | Temporal workflow query adapter. |
| Related dependency keys | `WorkflowQueryDepKey`. |
| Minimal example | `result = await queries.result(handle)` |
| Related pages | [Temporal](../../integrations/temporal.md). |

## `WorkflowHandle`

| Section | Details |
|---------|---------|
| Purpose | Identifies a workflow execution. |
| Import path | `from forze.application.contracts.workflow import WorkflowHandle` |
| Type parameters | None. |
| Required fields | `workflow_id`; optional `run_id`. |
| Returned values | Returned by `WorkflowCommandPort.start`. |
| Common implementations | attrs value object used by workflow adapters. |
| Related dependency keys | Returned through `WorkflowCommandDepKey` implementations. |
| Minimal example | `WorkflowHandle(workflow_id="project-p1")` |
| Related pages | [Contracts overview](../contracts.md). |

    :::python
    from forze.application.contracts.workflow import (
        WorkflowCommandDepKey,
        WorkflowSpec,
    )
    from forze.application.contracts.workflow.specs import WorkflowInvokeSpec

    workflow_spec = WorkflowSpec(
        name="project-onboarding",
        run=WorkflowInvokeSpec(args_type=StartOnboarding, return_type=OnboardingResult),
    )
    commands = ctx.dep(WorkflowCommandDepKey)(ctx, workflow_spec)
    handle = await commands.start(StartOnboarding(project_id="p1"))

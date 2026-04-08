# Temporal Integration

`forze_temporal` provides the package scaffolding for integrating with [Temporal.io](https://temporal.io), a workflow orchestration engine for long-running and distributed processes. The core `WorkflowPort` contract is fully defined; the integration package currently provides compatibility checking while production-ready adapters are under development.

## Installation

    :::bash
    uv add 'forze[temporal]'

## Current status

| Component | Status |
|-----------|--------|
| `WorkflowPort` contract | Defined in core (`forze.application.contracts.workflow`) |
| `forze_temporal` package | Scaffolding only (compatibility check) |
| Adapter implementation | User-provided (recommended pattern below) |

## WorkflowPort contract

The `WorkflowPort` protocol defines two operations:

    :::python
    from forze.application.contracts.workflow import WorkflowPort

| Method | Purpose |
|--------|---------|
| `start(name, id, args, queue?)` | Start a new workflow instance |
| `signal(id, signal, data)` | Send a signal to a running workflow |

### Method signatures

**`start`**: launches a workflow:

- `name`: workflow type/name registered in the Temporal worker
- `id`: external identifier for the workflow instance (must be unique per workflow type)
- `args`: positional arguments forwarded to the workflow start call
- `queue`: optional task queue name (defaults to the worker's default queue)

**`signal`**: sends data to a running workflow:

- `id`: workflow instance identifier
- `signal`: signal name registered on the workflow
- `data`: payload items delivered with the signal

## Implementing the adapter

Create your own adapter that wraps the Temporal Python SDK client:

    :::python
    from typing import Any, Sequence

    from temporalio.client import Client

    from forze.application.contracts.workflow import WorkflowPort


    class TemporalWorkflowAdapter:
        def __init__(self, client: Client) -> None:
            self._client = client

        async def start(
            self,
            name: str,
            id: str,
            args: Sequence[Any],
            queue: str | None = None,
        ) -> None:
            await self._client.start_workflow(
                name,
                *args,
                id=id,
                task_queue=queue or "default",
            )

        async def signal(
            self,
            id: str,
            signal: str,
            data: Sequence[dict[str, Any]],
        ) -> None:
            handle = self._client.get_workflow_handle(id)
            await handle.signal(signal, *data)

## Registering the adapter

Create a dependency module that registers your adapter under a custom key, then resolve it from `ExecutionContext`:

    :::python
    from forze.application.contracts.base import DepKey
    from forze.application.execution import Deps

    WorkflowDepKey = DepKey[WorkflowPort]("workflow")


    def temporal_module(client: Client) -> Deps:
        adapter = TemporalWorkflowAdapter(client=client)
        return Deps(deps={WorkflowDepKey: adapter})

Wire it into your dependency plan:

    :::python
    from temporalio.client import Client

    temporal_client = await Client.connect("localhost:7233")

    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            postgres_module(),
            redis_module(),
            temporal_module(temporal_client),
        ),
    )

## Using in usecases

Resolve the workflow port from the execution context and use it in your usecases:

    :::python
    from forze.application.execution import Usecase


    class StartProjectWorkflow(Usecase[UUID, None]):
        async def main(self, args: UUID) -> None:
            workflow = self.ctx.dep(WorkflowDepKey)

            await workflow.start(
                name="ProjectOnboarding",
                id=f"project-onboarding-{args}",
                args=[str(args)],
                queue="project-tasks",
            )


    class NotifyWorkflow(Usecase[NotifyArgs, None]):
        async def main(self, args: NotifyArgs) -> None:
            workflow = self.ctx.dep(WorkflowDepKey)

            await workflow.signal(
                id=f"project-onboarding-{args.project_id}",
                signal="step_completed",
                data=[{"step": args.step, "result": args.result}],
            )

## Why this approach works

Forze separates contracts from implementations by design. Even while `forze_temporal` is minimal, you can keep workflow orchestration decoupled:

- **Code against `WorkflowPort`**: your usecases depend on the protocol, not the Temporal SDK
- **Place SDK details only in adapter code**: the `TemporalWorkflowAdapter` is the only place that imports `temporalio`
- **Swap implementations later**: when `forze_temporal` ships built-in adapters, you replace your custom module without changing usecases

This is the same pattern used by all Forze integrations. The workflow port is no different from document, cache, or storage ports in terms of architecture.

## Lifecycle management

For production use, manage the Temporal client lifecycle with a custom lifecycle step:

    :::python
    from forze.application.execution import LifecycleStep
    from temporalio.client import Client


    async def temporal_startup(ctx):
        client = await Client.connect("localhost:7233")
        # Store client reference for shutdown


    async def temporal_shutdown(ctx):
        # Temporal Python SDK handles cleanup automatically
        pass


    temporal_lifecycle = LifecycleStep(
        name="temporal",
        startup=temporal_startup,
        shutdown=temporal_shutdown,
    )

# Temporal Integration

`forze_temporal` currently provides package scaffolding; production-ready adapters are not bundled yet.

## Current status

- Package exists: `forze_temporal`
- Workflow contract exists in core: `WorkflowPort`
- You are expected to provide your own adapter implementation today

## Recommended pattern (current)

Implement `WorkflowPort` in your app/infrastructure layer, then register it in your dependency container.

    :::python
    from typing import Any, Sequence
    from forze.application.contracts.workflow import WorkflowPort

    class TemporalWorkflowAdapter(WorkflowPort):
        def __init__(self, client) -> None:
            self.client = client

        async def start(
            self,
            name: str,
            id: str,
            args: Sequence[Any],
            queue: str | None = None,
        ) -> None:
            await self.client.start_workflow(
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
            handle = self.client.get_workflow_handle(id)
            await handle.signal(signal, *data)

Register your adapter via dependency key in your own module and resolve it from `ExecutionContext` where needed.

## Why this still works well

Forze already separates contracts from implementations. Even while `forze_temporal` is minimal, you can keep workflow orchestration decoupled by:

- coding against `WorkflowPort`
- placing Temporal SDK details only in adapter code
- swapping implementation later without changing usecases

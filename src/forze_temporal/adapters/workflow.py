from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.workflow import (
    WorkflowCommandPort,
    WorkflowHandle,
    WorkflowQueryPort,
    WorkflowQuerySpec,
    WorkflowSignalSpec,
    WorkflowSpec,
    WorkflowUpdateSpec,
)

from .base import TemporalBaseAdapter

# ----------------------- #
#! TODO: Need to check serialization and deserialization


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowCommandAdapter[In: BaseModel, Out: BaseModel](
    TemporalBaseAdapter,
    WorkflowCommandPort[In, Out],
):
    """Temporal-backed implementation of :class:`WorkflowCommandPort`."""

    queue: str
    """Temporal task queue name."""

    spec: WorkflowSpec[In, Out]
    """Workflow specification."""

    # ....................... #

    async def start(
        self,
        args: In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> WorkflowHandle:
        wid = self.construct_workflow_id(workflow_id)

        res = await self.client.start_workflow(
            queue=self.queue,
            name=self.spec.name,
            arg=args,
            workflow_id=wid,
            raise_on_already_started=raise_on_already_started,
        )

        return WorkflowHandle(workflow_id=res.id, run_id=res.run_id)

    # ....................... #

    async def signal[S: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        signal: WorkflowSignalSpec[S],
        args: S,
    ) -> None:
        await self.client.signal_workflow(
            workflow_id=handle.workflow_id,
            signal=signal.name,
            arg=args,
            run_id=handle.run_id,
        )

    # ....................... #

    async def update[U: BaseModel, Res: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        update: WorkflowUpdateSpec[U, Res],
        args: U,
    ) -> Res:
        res = await self.client.update_workflow(
            workflow_id=handle.workflow_id,
            update=update.name,
            arg=args,
            run_id=handle.run_id,
        )

        return res

    # ....................... #

    async def cancel(self, handle: WorkflowHandle) -> None:
        await self.client.cancel_workflow(
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
        )

    # ....................... #

    async def terminate(
        self,
        handle: WorkflowHandle,
        *,
        reason: str | None = None,
    ) -> None:
        await self.client.terminate_workflow(
            workflow_id=handle.workflow_id,
            reason=reason,
            run_id=handle.run_id,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowQueryAdapter[In: BaseModel, Out: BaseModel](
    TemporalBaseAdapter,
    WorkflowQueryPort[In, Out],
):
    """Temporal-backed implementation of :class:`WorkflowQueryPort`."""

    queue: str
    """Temporal task queue name."""

    spec: WorkflowSpec[In, Out]
    """Workflow specification."""

    # ....................... #

    async def query[Q: BaseModel, Res: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        query: WorkflowQuerySpec[Q, Res],
        args: Q,
    ) -> Res:
        res = await self.client.query_workflow(
            workflow_id=handle.workflow_id,
            query=query.name,
            arg=args,
            run_id=handle.run_id,
        )

        return res

    # ....................... #

    async def result(self, handle: WorkflowHandle) -> Out:
        return await self.client.get_workflow_result(
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
        )

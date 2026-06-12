from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandPort,
    DurableWorkflowHandle,
    DurableWorkflowQueryPort,
    DurableWorkflowQuerySpec,
    DurableWorkflowRunDescription,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
)
from forze.base.exceptions import exc

from .base import TemporalBaseAdapter

# ----------------------- #
# Serialization contract: argument models are serialized by the client's data
# converter (``pydantic_data_converter`` unless overridden); results are
# deserialized back into the spec-declared models by passing the spec's
# ``return_type`` as ``result_type`` on query/update/result calls below. A
# ``None`` return type yields the converter's raw payload unchanged.


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowCommandAdapter[In: BaseModel, Out: BaseModel](
    TemporalBaseAdapter,
    DurableWorkflowCommandPort[In, Out],
):
    """Temporal-backed implementation of :class:`DurableWorkflowCommandPort`."""

    spec: DurableWorkflowSpec[In, Out]
    """Workflow specification."""

    # ....................... #

    async def start(
        self,
        args: In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> DurableWorkflowHandle:
        await self._prepare_queue()
        wid = self.construct_workflow_id(workflow_id)

        res = await self.client.start_workflow(
            queue=await self._resolved_queue(),
            name=self.spec.name,
            arg=args,
            workflow_id=wid,
            raise_on_already_started=raise_on_already_started,
        )

        return DurableWorkflowHandle(workflow_id=res.id, run_id=res.run_id)

    # ....................... #

    async def signal[S: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        signal: DurableWorkflowSignalSpec[S],
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
        handle: DurableWorkflowHandle,
        *,
        update: DurableWorkflowUpdateSpec[U, Res],
        args: U,
    ) -> Res:
        res = await self.client.update_workflow(
            workflow_id=handle.workflow_id,
            update=update.name,
            arg=args,
            run_id=handle.run_id,
            result_type=update.return_type,
        )

        return res

    # ....................... #

    async def cancel(self, handle: DurableWorkflowHandle) -> None:
        await self.client.cancel_workflow(
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
        )

    # ....................... #

    async def terminate(
        self,
        handle: DurableWorkflowHandle,
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
    DurableWorkflowQueryPort[In, Out],
):
    """Temporal-backed implementation of :class:`DurableWorkflowQueryPort`."""

    spec: DurableWorkflowSpec[In, Out]
    """Workflow specification."""

    # ....................... #

    async def query[Q: BaseModel, Res: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        query: DurableWorkflowQuerySpec[Q, Res],
        args: Q,
    ) -> Res:
        res = await self.client.query_workflow(
            workflow_id=handle.workflow_id,
            query=query.name,
            arg=args,
            run_id=handle.run_id,
            result_type=query.return_type,
        )

        return res

    # ....................... #

    async def result(self, handle: DurableWorkflowHandle) -> Out:
        return await self.client.get_workflow_result(
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
            result_type=self.spec.run.return_type,
        )

    # ....................... #

    async def describe(
        self,
        handle: DurableWorkflowHandle,
    ) -> DurableWorkflowRunDescription:
        desc = await self.client.describe_workflow(
            workflow_id=handle.workflow_id,
            run_id=handle.run_id,
        )

        if desc.workflow_name != self.spec.name:
            raise exc.not_found(
                f"Workflow run {handle.workflow_id!r} is not for workflow {self.spec.name!r}",
            )

        return desc

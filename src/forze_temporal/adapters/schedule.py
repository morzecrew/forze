from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleCommandPort,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleQueryPort,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.base.exceptions import exc
from forze.base.exceptions.model import CoreException, ExceptionKind

from ..kernel.client.schedule_mapping import resolve_scheduled_workflow_id
from .base import TemporalBaseAdapter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowScheduleCommandAdapter[In: BaseModel](
    TemporalBaseAdapter,
    DurableWorkflowScheduleCommandPort[In],
):
    """Temporal-backed implementation of :class:`DurableWorkflowScheduleCommandPort`."""

    spec: DurableWorkflowSpec[In, BaseModel]
    """Workflow specification."""

    # ....................... #

    def _workflow_id(
        self,
        schedule_id: str,
        *,
        workflow_id_base: str | None,
    ) -> str:
        # An explicit base names the started workflow runs, so it must live in
        # the same tenant id-space as the schedule itself.
        base = (
            self.resolve_workflow_id(workflow_id_base)
            if workflow_id_base is not None
            else None
        )

        return resolve_scheduled_workflow_id(
            schedule_id,
            workflow_id_base=base,
        )

    # ....................... #

    async def create(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        await self._prepare_queue()
        sid = self.construct_schedule_id(schedule_id)
        workflow_id = self._workflow_id(
            sid,
            workflow_id_base=workflow_id_base,
        )

        await self.client.create_schedule(
            sid,
            workflow_name=self.spec.name,
            queue=await self._resolved_queue(),
            arg=args,
            timing=timing,
            workflow_id=workflow_id,
            trigger_immediately=trigger_immediately,
            note=note,
        )

        return DurableWorkflowScheduleHandle(schedule_id=sid)

    # ....................... #

    async def upsert(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        sid = self.construct_schedule_id(schedule_id)

        try:
            return await self.create(
                schedule_id,
                args,
                timing,
                workflow_id_base=workflow_id_base,
                trigger_immediately=trigger_immediately,
                note=note,
            )

        except CoreException as e:
            if e.kind != ExceptionKind.CONFLICT:
                raise

        workflow_id = self._workflow_id(
            sid,
            workflow_id_base=workflow_id_base,
        )

        await self.client.update_schedule(
            sid,
            workflow_name=self.spec.name,
            queue=await self._resolved_queue(),
            arg=args,
            timing=timing,
            workflow_id=workflow_id,
            note=note,
        )

        if trigger_immediately:
            await self.client.trigger_schedule(sid)

        return DurableWorkflowScheduleHandle(schedule_id=sid)

    # ....................... #

    async def update(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        timing: DurableWorkflowScheduleTiming | None = None,
        args: In | None = None,
        workflow_id_base: str | None = None,
        note: str | None = None,
    ) -> None:
        await self._prepare_queue()
        sid = self.resolve_schedule_id(handle.schedule_id)
        workflow_id = (
            self._workflow_id(sid, workflow_id_base=workflow_id_base)
            if workflow_id_base is not None
            else None
        )

        await self.client.update_schedule(
            sid,
            workflow_name=self.spec.name,
            queue=await self._resolved_queue(),
            arg=args,
            timing=timing,
            workflow_id=workflow_id,
            note=note,
        )

    # ....................... #

    async def delete(self, handle: DurableWorkflowScheduleHandle) -> None:
        await self.client.delete_schedule(
            self.resolve_schedule_id(handle.schedule_id),
        )

    # ....................... #

    async def pause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        await self.client.pause_schedule(
            self.resolve_schedule_id(handle.schedule_id),
            note=note,
        )

    # ....................... #

    async def unpause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        await self.client.unpause_schedule(
            self.resolve_schedule_id(handle.schedule_id),
            note=note,
        )

    # ....................... #

    async def trigger(self, handle: DurableWorkflowScheduleHandle) -> None:
        await self.client.trigger_schedule(
            self.resolve_schedule_id(handle.schedule_id),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowScheduleQueryAdapter[In: BaseModel](
    TemporalBaseAdapter,
    DurableWorkflowScheduleQueryPort[In],
):
    """Temporal-backed implementation of :class:`DurableWorkflowScheduleQueryPort`."""

    spec: DurableWorkflowSpec[In, BaseModel]
    """Workflow specification."""

    # ....................... #

    async def describe(
        self,
        handle: DurableWorkflowScheduleHandle,
    ) -> DurableWorkflowScheduleDescription:
        desc = await self.client.describe_schedule(
            self.resolve_schedule_id(handle.schedule_id),
        )

        if desc.workflow_name != self.spec.name:
            raise exc.not_found(
                f"Schedule {handle.schedule_id!r} is not for workflow {self.spec.name!r}",
            )

        return desc

    # ....................... #

    async def list(
        self,
        *,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> tuple[tuple[DurableWorkflowScheduleDescription, ...], str | None]:
        # Tenant-aware listing narrows to the tenant's own id prefix, so ids,
        # timing, and paused state of other tenants' schedules never surface.
        page = await self.client.list_schedules(
            workflow_name=self.spec.name,
            limit=limit,
            next_page_token=next_page_token,
            schedule_id_prefix=self._tenant_id_prefix(),
        )

        return page.descriptions, page.next_page_token

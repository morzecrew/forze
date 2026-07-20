"""Unit tests for Temporal workflow schedule adapters."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

pytest.importorskip("temporalio")

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, exc
from forze_temporal.adapters.schedule import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from forze_temporal.kernel.client.client import TemporalClient


class _In(BaseModel):
    x: int = 0


def _spec() -> DurableWorkflowSpec[_In, BaseModel]:
    return DurableWorkflowSpec(
        name="ItSumWorkflow",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=None),
    )


def _timing() -> DurableWorkflowScheduleTiming:
    return DurableWorkflowScheduleTiming(interval=timedelta(seconds=30))


class TestTemporalWorkflowScheduleCommandAdapter:
    @pytest.mark.asyncio
    async def test_create_delegates_to_client(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.create_schedule = AsyncMock()

        adapter = TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=False,
        )

        handle = await adapter.create(
            "nightly",
            _In(x=1),
            _timing(),
            workflow_id_base="run-{date}",
        )

        assert handle == DurableWorkflowScheduleHandle(schedule_id="nightly")
        client.create_schedule.assert_awaited_once()
        call = client.create_schedule.await_args
        assert call.args[0] == "nightly"
        assert call.kwargs["workflow_name"] == "ItSumWorkflow"
        assert call.kwargs["workflow_id"] == "run-{date}"

    @pytest.mark.asyncio
    async def test_create_prefixes_schedule_id_when_tenant_aware(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.create_schedule = AsyncMock()
        tid = TenantIdentity(tenant_id=UUID("00000000-0000-7000-8000-000000000001"))

        adapter = TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: tid,
        )

        await adapter.create("nightly", _In(), _timing())

        call = client.create_schedule.await_args
        assert call.args[0] == f"tenant:{tid.tenant_id}:nightly"


class TestTemporalWorkflowScheduleQueryAdapter:
    @pytest.mark.asyncio
    async def test_list_delegates_to_client(self) -> None:
        from forze.application.contracts.durable.workflow import DurableWorkflowScheduleDescription

        client = MagicMock(spec=TemporalClient)
        desc = DurableWorkflowScheduleDescription(
            schedule_id="s1",
            workflow_name="ItSumWorkflow",
            paused=False,
            timing=_timing(),
        )
        page = MagicMock()
        page.descriptions = (desc,)
        page.next_page_token = None
        client.list_schedules = AsyncMock(return_value=page)

        adapter = TemporalWorkflowScheduleQueryAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=False,
        )

        items, token = await adapter.list(limit=10)

        assert len(items) == 1
        assert token is None
        client.list_schedules.assert_awaited_once_with(
            workflow_name="ItSumWorkflow",
            limit=10,
            next_page_token=None,
            schedule_id_prefix=None,
        )

    @pytest.mark.asyncio
    async def test_describe_wrong_workflow_raises(self) -> None:
        from forze.application.contracts.durable.workflow import (
            DurableWorkflowScheduleDescription,
        )

        client = MagicMock(spec=TemporalClient)
        client.describe_schedule = AsyncMock(
            return_value=DurableWorkflowScheduleDescription(
                schedule_id="s1",
                workflow_name="OtherWorkflow",
                paused=False,
                timing=_timing(),
            ),
        )
        adapter = TemporalWorkflowScheduleQueryAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=False,
        )

        with pytest.raises(CoreException, match="not for workflow"):
            await adapter.describe(DurableWorkflowScheduleHandle(schedule_id="s1"))


class TestTemporalScheduleCommandLifecycle:
    @pytest.mark.asyncio
    async def test_upsert_updates_on_conflict(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.create_schedule = AsyncMock(side_effect=exc.conflict("exists"))
        client.update_schedule = AsyncMock()
        client.trigger_schedule = AsyncMock()

        adapter = TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=False,
        )

        handle = await adapter.upsert(
            "daily",
            _In(x=2),
            _timing(),
            trigger_immediately=True,
        )

        assert handle.schedule_id == "daily"
        client.update_schedule.assert_awaited_once()
        client.trigger_schedule.assert_awaited_once_with("daily")

    @pytest.mark.asyncio
    async def test_update_pause_unpause_trigger_delete(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.update_schedule = AsyncMock()
        client.pause_schedule = AsyncMock()
        client.unpause_schedule = AsyncMock()
        client.trigger_schedule = AsyncMock()
        client.delete_schedule = AsyncMock()

        adapter = TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=False,
        )
        handle = DurableWorkflowScheduleHandle(schedule_id="sched-1")

        await adapter.update(handle, timing=_timing(), note="n")
        await adapter.pause(handle, note="paused")
        await adapter.unpause(handle)
        await adapter.trigger(handle)
        await adapter.delete(handle)

        client.update_schedule.assert_awaited_once()
        client.pause_schedule.assert_awaited_once_with("sched-1", note="paused")
        client.unpause_schedule.assert_awaited_once_with("sched-1", note=None)
        client.trigger_schedule.assert_awaited_once_with("sched-1")
        client.delete_schedule.assert_awaited_once_with("sched-1")


class TestTemporalScheduleTenantScoping:
    """Schedule ops must address only the active tenant's schedule id-space."""

    _tid = UUID("00000000-0000-7000-8000-0000000000aa")
    _other = UUID("00000000-0000-7000-8000-0000000000bb")

    def _command_adapter(
        self, client: TemporalClient
    ) -> TemporalWorkflowScheduleCommandAdapter[_In]:
        return TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=self._tid),
        )

    def _query_adapter(
        self, client: TemporalClient
    ) -> TemporalWorkflowScheduleQueryAdapter[_In]:
        return TemporalWorkflowScheduleQueryAdapter(
            client=client,
            queue="tq-a",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=self._tid),
        )

    @pytest.mark.asyncio
    async def test_handle_ops_prefix_raw_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.update_schedule = AsyncMock()
        client.pause_schedule = AsyncMock()
        client.unpause_schedule = AsyncMock()
        client.trigger_schedule = AsyncMock()
        client.delete_schedule = AsyncMock()

        adapter = self._command_adapter(client)
        handle = DurableWorkflowScheduleHandle(schedule_id="sched-1")
        sid = f"tenant:{self._tid}:sched-1"

        await adapter.update(handle, timing=_timing(), note="n")
        await adapter.pause(handle, note="paused")
        await adapter.unpause(handle)
        await adapter.trigger(handle)
        await adapter.delete(handle)

        assert client.update_schedule.await_args.args[0] == sid
        client.pause_schedule.assert_awaited_once_with(sid, note="paused")
        client.unpause_schedule.assert_awaited_once_with(sid, note=None)
        client.trigger_schedule.assert_awaited_once_with(sid)
        client.delete_schedule.assert_awaited_once_with(sid)

    @pytest.mark.asyncio
    async def test_handle_ops_pass_through_own_prefixed_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.pause_schedule = AsyncMock()

        adapter = self._command_adapter(client)
        sid = f"tenant:{self._tid}:sched-1"

        await adapter.pause(DurableWorkflowScheduleHandle(schedule_id=sid))

        client.pause_schedule.assert_awaited_once_with(sid, note=None)

    @pytest.mark.asyncio
    async def test_handle_ops_reject_foreign_tenant_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.pause_schedule = AsyncMock()
        client.delete_schedule = AsyncMock()
        client.describe_schedule = AsyncMock()

        cmd = self._command_adapter(client)
        qry = self._query_adapter(client)
        handle = DurableWorkflowScheduleHandle(
            schedule_id=f"tenant:{self._other}:sched-1",
        )

        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd.pause(handle)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd.delete(handle)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await qry.describe(handle)

        client.pause_schedule.assert_not_awaited()
        client.delete_schedule.assert_not_awaited()
        client.describe_schedule.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_describe_prefixes_raw_ids(self) -> None:
        from forze.application.contracts.durable.workflow import (
            DurableWorkflowScheduleDescription,
        )

        client = MagicMock(spec=TemporalClient)
        sid = f"tenant:{self._tid}:sched-1"
        client.describe_schedule = AsyncMock(
            return_value=DurableWorkflowScheduleDescription(
                schedule_id=sid,
                workflow_name="ItSumWorkflow",
                paused=False,
                timing=_timing(),
            ),
        )

        adapter = self._query_adapter(client)

        out = await adapter.describe(
            DurableWorkflowScheduleHandle(schedule_id="sched-1"),
        )

        assert out.schedule_id == sid
        client.describe_schedule.assert_awaited_once_with(sid)

    @pytest.mark.asyncio
    async def test_list_filters_to_tenant_prefix(self) -> None:
        client = MagicMock(spec=TemporalClient)
        page = MagicMock()
        page.descriptions = ()
        page.next_page_token = None
        client.list_schedules = AsyncMock(return_value=page)

        adapter = self._query_adapter(client)

        await adapter.list(limit=5)

        client.list_schedules.assert_awaited_once_with(
            workflow_name="ItSumWorkflow",
            limit=5,
            next_page_token=None,
            schedule_id_prefix=f"tenant:{self._tid}:",
        )

    @pytest.mark.asyncio
    async def test_create_prefixes_workflow_id_base(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.create_schedule = AsyncMock()

        adapter = self._command_adapter(client)

        await adapter.create(
            "nightly",
            _In(x=1),
            _timing(),
            workflow_id_base="run-base",
        )

        call = client.create_schedule.await_args
        assert call.args[0] == f"tenant:{self._tid}:nightly"
        assert call.kwargs["workflow_id"] == f"tenant:{self._tid}:run-base"

    @pytest.mark.asyncio
    async def test_update_prefixes_workflow_id_base(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.update_schedule = AsyncMock()

        adapter = self._command_adapter(client)

        await adapter.update(
            DurableWorkflowScheduleHandle(schedule_id="sched-1"),
            timing=_timing(),
            workflow_id_base="run-base",
        )

        call = client.update_schedule.await_args
        assert call.args[0] == f"tenant:{self._tid}:sched-1"
        assert call.kwargs["workflow_id"] == f"tenant:{self._tid}:run-base"

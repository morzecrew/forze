"""Unit tests for Temporal workflow schedule adapters."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

pytest.importorskip("temporalio")

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze_temporal.adapters.schedule import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from forze_temporal.kernel.platform.client import TemporalClient


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
            workflow_id_template="run-{date}",
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
        )

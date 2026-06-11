"""Tests for durable workflow schedule contract ports."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleCommandPort,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleQueryPort,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec


class _In(BaseModel):
    n: int = 0


def _wf_spec() -> DurableWorkflowSpec[_In, BaseModel]:
    return DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=None),
    )


def _timing() -> DurableWorkflowScheduleTiming:
    return DurableWorkflowScheduleTiming(interval=timedelta(minutes=5))


class _StubScheduleCommand:
    spec = _wf_spec()

    async def create(
        self,
        schedule_id: str,
        args: _In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        return DurableWorkflowScheduleHandle(schedule_id=schedule_id)

    async def upsert(
        self,
        schedule_id: str,
        args: _In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        return DurableWorkflowScheduleHandle(schedule_id=schedule_id)

    async def update(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        timing: DurableWorkflowScheduleTiming | None = None,
        args: _In | None = None,
        workflow_id_base: str | None = None,
        note: str | None = None,
    ) -> None:
        return None

    async def delete(self, handle: DurableWorkflowScheduleHandle) -> None:
        return None

    async def pause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        return None

    async def unpause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        return None

    async def trigger(self, handle: DurableWorkflowScheduleHandle) -> None:
        return None


class _StubScheduleQuery:
    spec = _wf_spec()

    async def describe(
        self,
        handle: DurableWorkflowScheduleHandle,
    ) -> DurableWorkflowScheduleDescription:
        return DurableWorkflowScheduleDescription(
            schedule_id=handle.schedule_id,
            workflow_name="wf",
            paused=False,
            timing=_timing(),
        )

    async def list(
        self,
        *,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> tuple[tuple[DurableWorkflowScheduleDescription, ...], str | None]:
        return (), None


def test_durable_workflow_schedule_timing_requires_trigger() -> None:
    with pytest.raises(Exception):
        DurableWorkflowScheduleTiming()


@pytest.mark.asyncio
async def test_durable_workflow_schedule_command_is_protocol() -> None:
    port: DurableWorkflowScheduleCommandPort[_In] = _StubScheduleCommand()
    handle = await port.create("s1", _In(n=1), _timing())
    assert handle.schedule_id == "s1"
    assert isinstance(port, DurableWorkflowScheduleCommandPort)


@pytest.mark.asyncio
async def test_durable_workflow_schedule_query_is_protocol() -> None:
    port: DurableWorkflowScheduleQueryPort[_In] = _StubScheduleQuery()
    handle = DurableWorkflowScheduleHandle(schedule_id="s1")
    desc = await port.describe(handle)
    assert desc.workflow_name == "wf"
    assert isinstance(port, DurableWorkflowScheduleQueryPort)

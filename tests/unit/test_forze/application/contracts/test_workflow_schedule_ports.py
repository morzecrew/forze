"""Tests for workflow schedule contract ports."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.workflow import (
    WorkflowScheduleCommandPort,
    WorkflowScheduleDescription,
    WorkflowScheduleHandle,
    WorkflowScheduleQueryPort,
    WorkflowScheduleTiming,
    WorkflowSpec,
)
from forze.application.contracts.workflow.specs import WorkflowInvokeSpec


class _In(BaseModel):
    n: int = 0


def _wf_spec() -> WorkflowSpec[_In, BaseModel]:
    return WorkflowSpec(
        name="wf",
        run=WorkflowInvokeSpec(args_type=_In, return_type=None),
    )


def _timing() -> WorkflowScheduleTiming:
    return WorkflowScheduleTiming(interval=timedelta(minutes=5))


class _StubScheduleCommand:
    spec = _wf_spec()

    async def create(
        self,
        schedule_id: str,
        args: _In,
        timing: WorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> WorkflowScheduleHandle:
        return WorkflowScheduleHandle(schedule_id=schedule_id)

    async def upsert(
        self,
        schedule_id: str,
        args: _In,
        timing: WorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> WorkflowScheduleHandle:
        return WorkflowScheduleHandle(schedule_id=schedule_id)

    async def update(
        self,
        handle: WorkflowScheduleHandle,
        *,
        timing: WorkflowScheduleTiming | None = None,
        args: _In | None = None,
        workflow_id_template: str | None = None,
        note: str | None = None,
    ) -> None:
        return None

    async def delete(self, handle: WorkflowScheduleHandle) -> None:
        return None

    async def pause(
        self,
        handle: WorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        return None

    async def unpause(
        self,
        handle: WorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        return None

    async def trigger(self, handle: WorkflowScheduleHandle) -> None:
        return None


class _StubScheduleQuery:
    spec = _wf_spec()

    async def describe(
        self,
        handle: WorkflowScheduleHandle,
    ) -> WorkflowScheduleDescription:
        return WorkflowScheduleDescription(
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
    ) -> tuple[tuple[WorkflowScheduleDescription, ...], str | None]:
        return (), None


def test_workflow_schedule_timing_requires_trigger() -> None:
    with pytest.raises(Exception):
        WorkflowScheduleTiming()


@pytest.mark.asyncio
async def test_workflow_schedule_command_is_protocol() -> None:
    port: WorkflowScheduleCommandPort[_In] = _StubScheduleCommand()
    handle = await port.create("s1", _In(n=1), _timing())
    assert handle.schedule_id == "s1"
    assert isinstance(port, WorkflowScheduleCommandPort)


@pytest.mark.asyncio
async def test_workflow_schedule_query_is_protocol() -> None:
    port: WorkflowScheduleQueryPort[_In] = _StubScheduleQuery()
    handle = WorkflowScheduleHandle(schedule_id="s1")
    desc = await port.describe(handle)
    assert desc.workflow_name == "wf"
    assert isinstance(port, WorkflowScheduleQueryPort)

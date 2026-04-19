"""Tests for forze.application.contracts.workflow.ports."""

from __future__ import annotations

import pytest
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
from forze.application.contracts.workflow.specs import WorkflowInvokeSpec


class _In(BaseModel):
    n: int = 0


class _Out(BaseModel):
    msg: str = "done"


def _wf_spec() -> WorkflowSpec[_In, _Out]:
    return WorkflowSpec(
        name="wf",
        run=WorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )


class _StubWorkflowCommand:
    spec = _wf_spec()

    async def start(
        self,
        args: _In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> WorkflowHandle:
        return WorkflowHandle(workflow_id=workflow_id or "new")

    async def signal(
        self,
        handle: WorkflowHandle,
        *,
        signal: WorkflowSignalSpec[_In],
        args: _In,
    ) -> None:
        return None

    async def update(
        self,
        handle: WorkflowHandle,
        *,
        update: WorkflowUpdateSpec[_In, _Out],
        args: _In,
    ) -> _Out:
        return _Out(msg="updated")

    async def cancel(self, handle: WorkflowHandle) -> None:
        return None

    async def terminate(
        self,
        handle: WorkflowHandle,
        *,
        reason: str | None = None,
    ) -> None:
        return None


class _StubWorkflowQuery:
    spec = _wf_spec()

    async def query(
        self,
        handle: WorkflowHandle,
        *,
        query: WorkflowQuerySpec[_In, _Out],
        args: _In,
    ) -> _Out:
        return _Out(msg="q")

    async def result(self, handle: WorkflowHandle) -> _Out:
        return _Out(msg="final")


class TestWorkflowPorts:
    def test_command_runtime_checkable(self) -> None:
        assert isinstance(_StubWorkflowCommand(), WorkflowCommandPort)

    def test_query_runtime_checkable(self) -> None:
        assert isinstance(_StubWorkflowQuery(), WorkflowQueryPort)


@pytest.mark.asyncio
async def test_workflow_command_methods() -> None:
    cmd = _StubWorkflowCommand()
    handle = await cmd.start(_In(n=1), workflow_id="w1", raise_on_already_started=False)
    assert handle.workflow_id == "w1"
    await cmd.signal(
        handle,
        signal=WorkflowSignalSpec(name="s", args_type=_In),
        args=_In(n=2),
    )
    out = await cmd.update(
        handle,
        update=WorkflowUpdateSpec(name="u", args_type=_In, return_type=_Out),
        args=_In(n=3),
    )
    assert out.msg == "updated"
    await cmd.cancel(handle)
    await cmd.terminate(handle, reason="done")


@pytest.mark.asyncio
async def test_workflow_query_methods() -> None:
    q = _StubWorkflowQuery()
    handle = WorkflowHandle(workflow_id="h")
    res = await q.query(
        handle,
        query=WorkflowQuerySpec(name="q", args_type=_In, return_type=_Out),
        args=_In(n=0),
    )
    assert res.msg == "q"
    assert (await q.result(handle)).msg == "final"

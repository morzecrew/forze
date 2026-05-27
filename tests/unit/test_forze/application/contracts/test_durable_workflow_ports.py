"""Tests for forze.application.contracts.durable.workflow.ports."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandPort,
    DurableWorkflowHandle,
    DurableWorkflowQueryPort,
    DurableWorkflowQuerySpec,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec


class _In(BaseModel):
    n: int = 0


class _Out(BaseModel):
    msg: str = "done"


def _wf_spec() -> DurableWorkflowSpec[_In, _Out]:
    return DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )


class _StubWorkflowCommand:
    spec = _wf_spec()

    async def start(
        self,
        args: _In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> DurableWorkflowHandle:
        return DurableWorkflowHandle(workflow_id=workflow_id or "new")

    async def signal(
        self,
        handle: DurableWorkflowHandle,
        *,
        signal: DurableWorkflowSignalSpec[_In],
        args: _In,
    ) -> None:
        return None

    async def update(
        self,
        handle: DurableWorkflowHandle,
        *,
        update: DurableWorkflowUpdateSpec[_In, _Out],
        args: _In,
    ) -> _Out:
        return _Out(msg="updated")

    async def cancel(self, handle: DurableWorkflowHandle) -> None:
        return None

    async def terminate(
        self,
        handle: DurableWorkflowHandle,
        *,
        reason: str | None = None,
    ) -> None:
        return None


class _StubWorkflowQuery:
    spec = _wf_spec()

    async def query(
        self,
        handle: DurableWorkflowHandle,
        *,
        query: DurableWorkflowQuerySpec[_In, _Out],
        args: _In,
    ) -> _Out:
        return _Out(msg="q")

    async def result(self, handle: DurableWorkflowHandle) -> _Out:
        return _Out(msg="final")


class TestDurableWorkflowPorts:
    def test_command_runtime_checkable(self) -> None:
        assert isinstance(_StubWorkflowCommand(), DurableWorkflowCommandPort)

    def test_query_runtime_checkable(self) -> None:
        assert isinstance(_StubWorkflowQuery(), DurableWorkflowQueryPort)


@pytest.mark.asyncio
async def test_durable_workflow_command_methods() -> None:
    cmd = _StubWorkflowCommand()
    handle = await cmd.start(_In(n=1), workflow_id="w1", raise_on_already_started=False)
    assert handle.workflow_id == "w1"
    await cmd.signal(
        handle,
        signal=DurableWorkflowSignalSpec(name="s", args_type=_In),
        args=_In(n=2),
    )
    out = await cmd.update(
        handle,
        update=DurableWorkflowUpdateSpec(name="u", args_type=_In, return_type=_Out),
        args=_In(n=3),
    )
    assert out.msg == "updated"
    await cmd.cancel(handle)
    await cmd.terminate(handle, reason="done")


@pytest.mark.asyncio
async def test_durable_workflow_query_methods() -> None:
    q = _StubWorkflowQuery()
    handle = DurableWorkflowHandle(workflow_id="h")
    res = await q.query(
        handle,
        query=DurableWorkflowQuerySpec(name="q", args_type=_In, return_type=_Out),
        args=_In(n=0),
    )
    assert res.msg == "q"
    assert (await q.result(handle)).msg == "final"

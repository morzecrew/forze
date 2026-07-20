"""Coverage tests for the mock durable workflow command/query adapters."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowHandle,
    DurableWorkflowInvokeSpec,
    DurableWorkflowQuerySpec,
    DurableWorkflowRunStatus,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters.durable import (
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
)
from forze_mock.state import MockState

# ----------------------- #


class _In(BaseModel):
    n: int


class _Out(BaseModel):
    n: int


class _Sig(BaseModel):
    k: str


def _spec() -> DurableWorkflowSpec[_In, _Out]:
    return DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )


def _cmd(
    state: MockState, spec: DurableWorkflowSpec[_In, _Out] | None = None
) -> MockDurableWorkflowCommandAdapter[_In, _Out]:
    return MockDurableWorkflowCommandAdapter(spec=spec or _spec(), state=state)


def _qry(
    state: MockState, spec: DurableWorkflowSpec[_In, _Out] | None = None
) -> MockDurableWorkflowQueryAdapter[_In, _Out]:
    return MockDurableWorkflowQueryAdapter(spec=spec or _spec(), state=state)


# ----------------------- #


async def test_start_conflict_raises_when_flag_true() -> None:
    state = MockState()
    cmd = _cmd(state)
    await cmd.start(_In(n=1), workflow_id="dup")
    with pytest.raises(CoreException):
        await cmd.start(_In(n=2), workflow_id="dup")


async def test_start_conflict_suppressed_when_flag_false() -> None:
    state = MockState()
    cmd = _cmd(state)
    first = await cmd.start(_In(n=1), workflow_id="dup")
    second = await cmd.start(
        _In(n=2), workflow_id="dup", raise_on_already_started=False
    )
    # No conflict; the second start replaces the run under the same id.
    assert second.workflow_id == "dup"
    assert first.workflow_id == "dup"


async def test_signal_not_found() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowHandle(workflow_id="missing", run_id="r")
    sig = DurableWorkflowSignalSpec(name="ping", args_type=_Sig)
    with pytest.raises(CoreException):
        await cmd.signal(handle, signal=sig, args=_Sig(k="x"))


async def test_signal_appends_to_existing_run() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    sig = DurableWorkflowSignalSpec(name="ping", args_type=_Sig)
    await cmd.signal(handle, signal=sig, args=_Sig(k="x"))
    runs = state.durable_workflows["wf"]
    assert runs["wf-1"]["signals"] == [("ping", {"k": "x"})]


async def test_update_raises_not_implemented() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowHandle(workflow_id="x", run_id="r")
    upd = DurableWorkflowUpdateSpec(name="u", args_type=_In, return_type=_Out)
    with pytest.raises(CoreException):
        await cmd.update(handle, update=upd, args=_In(n=1))


async def test_cancel_missing_is_noop() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowHandle(workflow_id="nope", run_id="r")
    await cmd.cancel(handle)  # no raise


async def test_cancel_existing_sets_status() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    await cmd.cancel(handle)
    assert state.durable_workflows["wf"]["wf-1"]["status"] == (
        DurableWorkflowRunStatus.CANCELLED
    )


async def test_terminate_missing_is_noop_with_reason() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowHandle(workflow_id="nope", run_id="r")
    await cmd.terminate(handle, reason="because")  # no raise


async def test_terminate_existing_sets_status() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    await cmd.terminate(handle, reason="cleanup")
    assert state.durable_workflows["wf"]["wf-1"]["status"] == (
        DurableWorkflowRunStatus.TERMINATED
    )


async def test_query_raises_not_implemented() -> None:
    state = MockState()
    qry = _qry(state)
    handle = DurableWorkflowHandle(workflow_id="x", run_id="r")
    q = DurableWorkflowQuerySpec(name="q", args_type=_In, return_type=_Out)
    with pytest.raises(CoreException):
        await qry.query(handle, query=q, args=_In(n=1))


async def test_result_not_found() -> None:
    state = MockState()
    qry = _qry(state)
    handle = DurableWorkflowHandle(workflow_id="missing", run_id="r")
    with pytest.raises(CoreException):
        await qry.result(handle)


async def test_result_unavailable_when_no_result_set() -> None:
    state = MockState()
    cmd = _cmd(state)
    qry = _qry(state)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    with pytest.raises(CoreException):
        await qry.result(handle)


async def test_result_returns_validated_output() -> None:
    state = MockState()
    cmd = _cmd(state)
    qry = _qry(state)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    state.durable_workflows["wf"]["wf-1"]["result"] = {"n": 42}
    out = await qry.result(handle)
    assert out.n == 42


async def test_result_no_return_type() -> None:
    state = MockState()
    spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=None),
    )
    cmd = _cmd(state, spec)
    qry = _qry(state, spec)
    handle = await cmd.start(_In(n=1), workflow_id="wf-1")
    state.durable_workflows["wf"]["wf-1"]["result"] = {"n": 1}
    with pytest.raises(CoreException):
        await qry.result(handle)


async def test_describe_not_found() -> None:
    state = MockState()
    qry = _qry(state)
    handle = DurableWorkflowHandle(workflow_id="missing", run_id="r")
    with pytest.raises(CoreException):
        await qry.describe(handle)

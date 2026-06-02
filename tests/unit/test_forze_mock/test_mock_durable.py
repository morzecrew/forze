"""Durable workflow and function mock adapters."""

from datetime import timedelta

from pydantic import BaseModel

import pytest

from forze.base.serialization import PydanticModelCodec

from forze.application.contracts.durable.function import DurableFunctionEventSpec
from forze.application.contracts.durable.workflow import (
    DurableWorkflowInvokeSpec,
    DurableWorkflowRunStatus,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze_mock.adapters.durable import (
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
)
from forze_mock.state import MockState

# ----------------------- #


class _In(BaseModel):
    n: int


class _Out(BaseModel):
    n: int


class _Evt(BaseModel):
    k: str


@pytest.mark.asyncio
async def test_workflow_start_and_describe() -> None:
    state = MockState()
    spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )
    cmd = MockDurableWorkflowCommandAdapter(spec=spec, state=state)
    qry = MockDurableWorkflowQueryAdapter(spec=spec, state=state)

    handle = await cmd.start(_In(n=1))
    desc = await qry.describe(handle)
    assert desc.workflow_name == "wf"
    assert desc.status == DurableWorkflowRunStatus.RUNNING


@pytest.mark.asyncio
async def test_schedule_and_event_and_step_memo() -> None:
    state = MockState()
    wf_spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )
    sched_cmd = MockDurableWorkflowScheduleCommandAdapter(spec=wf_spec, state=state)
    sched_qry = MockDurableWorkflowScheduleQueryAdapter(spec=wf_spec, state=state)

    handle = await sched_cmd.create(
        "daily",
        _In(n=1),
        DurableWorkflowScheduleTiming(interval=timedelta(hours=24)),
    )
    desc = await sched_qry.describe(handle)
    assert desc.schedule_id == "daily"

    evt_spec = DurableFunctionEventSpec(
        name="evt",
        codec=PydanticModelCodec(model_type=_Evt),
    )
    evt = MockDurableFunctionEventAdapter(spec=evt_spec, state=state)
    eid = await evt.send(_Evt(k="x"))
    assert eid

    step = MockDurableFunctionStepAdapter(state=state, run_id="run-1")
    calls = 0

    async def body() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    assert await step.run("s1", body) == "ok"
    assert await step.run("s1", body) == "ok"
    assert calls == 1

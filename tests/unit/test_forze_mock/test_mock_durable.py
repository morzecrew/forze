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


# ----------------------- #
# tenant isolation (parity with Temporal per-tenant queue / Inngest envelope)


@pytest.mark.asyncio
async def test_workflow_runs_partitioned_by_tenant() -> None:
    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    t1, t2 = uuid4(), uuid4()
    current: dict[str, TenantIdentity] = {"id": TenantIdentity(tenant_id=t1)}
    state = MockState()
    spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )

    def _cmd() -> MockDurableWorkflowCommandAdapter[_In, _Out]:
        return MockDurableWorkflowCommandAdapter(
            spec=spec, state=state, tenant_aware=True, tenant_provider=lambda: current["id"]
        )

    def _qry() -> MockDurableWorkflowQueryAdapter[_In, _Out]:
        return MockDurableWorkflowQueryAdapter(
            spec=spec, state=state, tenant_aware=True, tenant_provider=lambda: current["id"]
        )

    handle = await _cmd().start(_In(n=1), workflow_id="shared-id")

    # Tenant 2 must not see (or collide with) tenant 1's run under the same workflow id.
    current["id"] = TenantIdentity(tenant_id=t2)
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException):
        await _qry().describe(handle)

    # A same-id workflow in tenant 2 is independent (no "already started" collision).
    other = await _cmd().start(_In(n=2), workflow_id="shared-id")
    assert other.workflow_id == "shared-id"

    # Back to tenant 1 — its run is intact.
    current["id"] = TenantIdentity(tenant_id=t1)
    desc = await _qry().describe(handle)
    assert desc.workflow_name == "wf"


@pytest.mark.asyncio
async def test_durable_fails_closed_without_tenant() -> None:
    from forze.base.exceptions import CoreException

    state = MockState()
    spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
    )
    cmd = MockDurableWorkflowCommandAdapter(
        spec=spec, state=state, tenant_aware=True, tenant_provider=lambda: None
    )

    with pytest.raises(CoreException, match="tenant_required"):
        await cmd.start(_In(n=1))

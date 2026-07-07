"""Durable workflow and function mock adapters."""

import asyncio
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


@pytest.mark.asyncio
async def test_concurrent_step_executions_converge_on_the_winner() -> None:
    # Two live executions of the same step (the reclaimed-lease overlap) both run the body —
    # an at-least-once effect — but must converge on ONE recorded result, first-write-wins,
    # matching the Postgres ``ON CONFLICT DO NOTHING`` (so the mock reproduces production).
    state = MockState()
    step = MockDurableFunctionStepAdapter(state=state, run_id="run-1")
    a_stored = asyncio.Event()

    async def fn_a() -> str:
        return "A"  # completes immediately -> journals first (the winner)

    async def fn_b() -> str:
        await a_stored.wait()  # completes only after A has journaled
        return "B"

    # B passes the memo-miss check, then parks in its body; A then runs to completion.
    tb = asyncio.create_task(step.run("s", fn_b))
    await asyncio.sleep(0)

    ra = await step.run("s", fn_a)
    a_stored.set()
    rb = await tb

    assert ra == "A"
    assert rb == "A"  # B converged on the winner, not its own "B"
    assert state.durable_step_memo["run-1:s"] == "A"


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


# ....................... #


class TestMockListRuns:
    """`DurableRunAdminPort.list_runs` on the mock: ordering, filters, keyset paging.

    # covers: DurableRunAdminPort.list_runs
    """

    async def test_lists_newest_first_and_filters(self) -> None:
        from forze.application.contracts.durable.function import DurableRunStatus
        from forze_mock.adapters.durable import MockDurableRunStore

        store = MockDurableRunStore(state=MockState())
        r1 = await store.enqueue("a", input_json=None)
        r2 = await store.enqueue("b", input_json=None)
        r3 = await store.enqueue("a", input_json=None)

        # Newest first.
        page = await store.list_runs(limit=10)
        assert [r.run_id for r in page.records] == [r3.run_id, r2.run_id, r1.run_id]
        assert page.next_cursor is None
        assert all(r.created_at is not None for r in page.records)

        # Name filter.
        only_a = await store.list_runs(name="a", limit=10)
        assert [r.run_id for r in only_a.records] == [r3.run_id, r1.run_id]

        # Status filter.
        await store.begin(r2.run_id, lease_for=timedelta(minutes=5))
        await store.complete(r2.run_id, output_json=None)
        done = await store.list_runs(status=DurableRunStatus.COMPLETED, limit=10)
        assert [r.run_id for r in done.records] == [r2.run_id]

    async def test_keyset_paging_walks_the_whole_set(self) -> None:
        from forze_mock.adapters.durable import MockDurableRunStore

        store = MockDurableRunStore(state=MockState())
        runs = [await store.enqueue("fn", input_json=None) for _ in range(5)]
        newest_first = [r.run_id for r in reversed(runs)]

        seen: list[str] = []
        cursor: str | None = None

        while True:
            page = await store.list_runs(limit=2, cursor=cursor)
            seen.extend(r.run_id for r in page.records)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        assert seen == newest_first  # no gaps, no repeats

    async def test_rejects_bad_limit_and_cursor(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_mock.adapters.durable import MockDurableRunStore

        store = MockDurableRunStore(state=MockState())

        with pytest.raises(CoreException):
            await store.list_runs(limit=0)

        with pytest.raises(CoreException):
            await store.list_runs(cursor="not-a-real-cursor")

    async def test_scopes_to_bound_tenant_and_spans_when_unbound(self) -> None:
        from uuid import uuid4

        from forze.application.contracts.tenancy import TenantIdentity
        from forze_mock.adapters.durable import MockDurableRunStore

        t1, t2 = uuid4(), uuid4()
        state = MockState()

        writer = MockDurableRunStore(state=state)
        await writer.enqueue("a", input_json=None, tenant_id=t1)
        await writer.enqueue("b", input_json=None, tenant_id=t2)

        # Bound to t1 → only that tenant's runs.
        bound = MockDurableRunStore(
            state=state, tenant_provider=lambda: TenantIdentity(tenant_id=t1)
        )
        scoped = await bound.list_runs(limit=10)
        assert {r.name for r in scoped.records} == {"a"}
        assert all(r.tenant_id == t1 for r in scoped.records)

        # Unbound → spans every tenant's runs (operator view).
        spanning = await writer.list_runs(limit=10)
        assert {r.name for r in spanning.records} == {"a", "b"}

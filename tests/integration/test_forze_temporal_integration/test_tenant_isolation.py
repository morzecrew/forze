"""Two-tenant isolation for Temporal workflow and schedule adapters (Docker dev server)."""

from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("temporalio")

from temporalio.service import RPCError

from forze.application.contracts.durable.workflow import (
    DurableWorkflowHandle,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_temporal.adapters.schedule import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from forze_temporal.adapters.workflow import TemporalWorkflowCommandAdapter

from ._workflow_defs import SumIn, SumOut


def _spec() -> DurableWorkflowSpec[SumIn, SumOut]:
    return DurableWorkflowSpec(
        name="ItSumWorkflow",
        run=DurableWorkflowInvokeSpec(args_type=SumIn, return_type=SumOut),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_workflow_handle_ops_are_tenant_isolated(temporal_dev_env) -> None:
    """Tenant B cannot signal/cancel tenant A's workflow through any id form."""

    forze_client = temporal_dev_env.forze_client
    tid_a = uuid4()
    tid_b = uuid4()

    cmd_a = TemporalWorkflowCommandAdapter(
        client=forze_client,
        queue="it-iso-wf",
        spec=_spec(),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid_a),
    )
    cmd_b = TemporalWorkflowCommandAdapter(
        client=forze_client,
        queue="it-iso-wf",
        spec=_spec(),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid_b),
    )

    handle_a = await cmd_a.start(SumIn(a=1, b=2), workflow_id="iso-wf")
    assert handle_a.workflow_id == f"tenant:{tid_a}:iso-wf"

    try:
        # B holding A's full handle is refused before reaching the server.
        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd_b.cancel(handle_a)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd_b.terminate(handle_a)

        # B addressing the same raw id lands in B's own namespace, where no
        # such workflow exists — never on A's workflow.
        with pytest.raises(RPCError):
            await cmd_b.cancel(DurableWorkflowHandle(workflow_id="iso-wf"))

    finally:
        # A can address its own workflow with the raw id it used at start.
        await cmd_a.terminate(
            DurableWorkflowHandle(workflow_id="iso-wf"),
            reason="cleanup",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schedules_are_tenant_isolated(temporal_dev_env) -> None:
    """Tenant A's schedule listing and handle ops never reach tenant B's schedules."""

    forze_client = temporal_dev_env.forze_client
    tid_a = uuid4()
    tid_b = uuid4()

    def _adapters(tid):
        cmd = TemporalWorkflowScheduleCommandAdapter(
            client=forze_client,
            queue="it-iso-sched",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        )
        qry = TemporalWorkflowScheduleQueryAdapter(
            client=forze_client,
            queue="it-iso-sched",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        )
        return cmd, qry

    cmd_a, qry_a = _adapters(tid_a)
    cmd_b, qry_b = _adapters(tid_b)

    timing = DurableWorkflowScheduleTiming(interval=timedelta(hours=1))
    handle_a = await cmd_a.create("iso-nightly", SumIn(a=1, b=1), timing)
    handle_b = await cmd_b.create("iso-nightly", SumIn(a=2, b=2), timing)

    try:
        # A's listing shows only A's id-space; B's tenant id never surfaces.
        items_a, _ = await qry_a.list(limit=100)
        assert items_a, "tenant A must see its own schedule"
        assert all(
            d.schedule_id.startswith(f"tenant:{tid_a}:") for d in items_a
        )
        assert all(str(tid_b) not in d.schedule_id for d in items_a)

        # B holding A's full handle is refused before reaching the server.
        with pytest.raises(CoreException, match="outside the active tenant"):
            await qry_b.describe(handle_a)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd_b.pause(handle_a)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await cmd_b.delete(handle_a)

        # The same raw id resolves per tenant: pausing via B's adapter pauses
        # only B's schedule.
        raw = DurableWorkflowScheduleHandle(schedule_id="iso-nightly")
        await cmd_b.pause(raw, note="b-paused")

        assert (await qry_b.describe(raw)).paused is True
        assert (await qry_a.describe(raw)).paused is False

    finally:
        await cmd_a.delete(handle_a)
        await cmd_b.delete(handle_b)

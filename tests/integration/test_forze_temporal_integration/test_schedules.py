"""Integration tests for Temporal workflow schedule adapters (Docker dev server)."""

import asyncio
import time
from datetime import timedelta

import pytest
from temporalio.service import RPCError

pytest.importorskip("temporalio")

from temporalio.worker import Worker

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze_temporal.adapters.schedule import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from forze_temporal.sandbox import sandboxed_workflow_runner

from ._workflow_defs import ItSumWorkflow, SumIn, SumOut, it_sum_pair


async def _await_workflow_result(
    forze_client,
    workflow_id: str,
    *,
    timeout: timedelta = timedelta(seconds=30),
) -> SumOut:
    """Poll until a scheduled workflow run completes."""

    deadline = time.monotonic() + timeout.total_seconds()
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            handle = forze_client.get_workflow_handle(workflow_id)
            result = await handle.result()
            return SumOut.model_validate(result)

        except RPCError as e:
            last_error = e
            if "not found" not in str(e).lower():
                raise

        await asyncio.sleep(0.25)

    if last_error is not None:
        raise last_error

    pytest.fail(f"Timed out waiting for workflow {workflow_id!r}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schedule_trigger_starts_workflow(temporal_dev_env) -> None:
    """Creating and triggering a schedule starts the configured workflow."""

    sdk_client = temporal_dev_env.client
    forze_client = temporal_dev_env.forze_client

    task_queue = "it-forze-schedule"
    spec = DurableWorkflowSpec(
        name="ItSumWorkflow",
        run=DurableWorkflowInvokeSpec(args_type=SumIn, return_type=SumOut),
    )

    cmd = TemporalWorkflowScheduleCommandAdapter(
        client=forze_client,
        queue=task_queue,
        spec=spec,
        tenant_aware=False,
    )
    qry = TemporalWorkflowScheduleQueryAdapter(
        client=forze_client,
        queue=task_queue,
        spec=spec,
        tenant_aware=False,
    )

    async with Worker(
        sdk_client,
        task_queue=task_queue,
        workflows=[ItSumWorkflow],
        activities=[it_sum_pair],
        workflow_runner=sandboxed_workflow_runner(),
    ):
        timing = DurableWorkflowScheduleTiming(interval=timedelta(hours=1))
        handle = await cmd.create(
            "it-sum-hourly",
            SumIn(a=2, b=3),
            timing,
            workflow_id_base="it-sum-scheduled-run",
        )

        await cmd.trigger(handle)

        desc = await qry.describe(handle)
        assert desc.schedule_id == "it-sum-hourly"
        assert desc.workflow_name == "ItSumWorkflow"
        assert desc.timing.interval == timedelta(hours=1)

        items, _ = await qry.list(limit=50)
        assert any(d.schedule_id == "it-sum-hourly" for d in items)

        # Temporal appends a scheduled-time suffix to the configured workflow id.
        sched_desc = await sdk_client.get_schedule_handle(handle.schedule_id).describe()
        assert sched_desc.info.recent_actions, "expected a fired action after trigger"
        fired = sched_desc.info.recent_actions[-1].action
        workflow_id = fired.workflow_id

        result = await _await_workflow_result(forze_client, workflow_id)
        assert result == SumOut(total=5)
        assert workflow_id.startswith("it-sum-scheduled-run")

        await cmd.pause(handle, note="testing")
        paused = await qry.describe(handle)
        assert paused.paused is True

        await cmd.unpause(handle)
        assert (await qry.describe(handle)).paused is False

        await cmd.delete(handle)

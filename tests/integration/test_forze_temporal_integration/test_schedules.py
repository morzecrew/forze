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
from forze.base.primitives import uuid7
from forze_temporal.adapters.schedule import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from forze_temporal.sandbox import sandboxed_workflow_runner

from ._workflow_defs import ItSumWorkflow, SumIn, SumOut, it_sum_pair
from .conftest import await_listed_schedules


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_paging_a_filtered_listing_returns_every_schedule_once(
    temporal_dev_env,
) -> None:
    """Walking a filtered listing in small pages loses and duplicates nothing.

    Paging is client-side-filtered, so the resume cursor has to carry a position *inside*
    a page, not just a page token — and that cursor has to survive a real server round
    trip. The layout below is tuned to the dev server's observed listing order (schedule
    id descending) so that, at ``limit=2``, the second page hits the limit on its *first*
    entry and leaves a wanted schedule un-yielded: ``keep, skip | keep, keep | …``. The
    assertion does not depend on that order holding — full coverage is the property; the
    mid-page stop itself is pinned deterministically by the unit battery.
    """

    forze_client = temporal_dev_env.forze_client

    run = uuid7().hex[:8]
    prefix = f"it-paging-{run}-"
    schedule_ids = tuple(f"{prefix}{index:02d}" for index in range(6))
    # Listed high-to-low, this reads keep, skip | keep, keep | skip, keep.
    wanted = {0, 2, 3, 5}
    expected = tuple(sid for index, sid in enumerate(schedule_ids) if index in wanted)

    for index, schedule_id in enumerate(schedule_ids):
        await forze_client.create_schedule(
            schedule_id,
            workflow_name="ItSumWorkflow" if index in wanted else "ItPingWorkflow",
            queue="it-forze-paging",
            arg=SumIn(a=1, b=index),
            timing=DurableWorkflowScheduleTiming(interval=timedelta(hours=1)),
            workflow_id=f"{schedule_id}-run",
        )

    try:
        # Page only once the whole set is listable, or a not-yet-visible entry would
        # read as a pagination loss.
        async def _listed():
            page = await forze_client.list_schedules(schedule_id_prefix=prefix)
            return page.descriptions

        await await_listed_schedules(_listed, count=len(schedule_ids))

        collected: list[str] = []
        cursor: str | None = None

        for _ in range(len(expected) + 2):  # bounded: 2 per call must finish sooner
            page = await forze_client.list_schedules(
                limit=2,
                next_page_token=cursor,
                schedule_id_prefix=prefix,
                workflow_name="ItSumWorkflow",
            )
            assert len(page.descriptions) <= 2, "a page overshot the requested limit"

            collected.extend(d.schedule_id for d in page.descriptions)
            cursor = page.next_page_token

            if cursor is None:
                break

        assert cursor is None, "pagination did not terminate"
        assert len(collected) == len(set(collected)), f"duplicated across pages: {collected}"
        assert sorted(collected) == sorted(expected)

    finally:
        for schedule_id in schedule_ids:
            await forze_client.delete_schedule(schedule_id)

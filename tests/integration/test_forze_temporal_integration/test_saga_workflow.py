"""Integration test: a Temporal workflow drives the shared saga coordinator.

`TemporalSaga` runs activity-shaped steps with Forze's pivot/compensation semantics
(`SagaProgress`) on a real time-skipping Temporal server: a pre-pivot failure compensates
the completed steps in reverse; a post-pivot failure fails forward (no compensation).
Durability/retries are Temporal's; the saga semantics are the same code the in-process
executor runs.
"""

import pytest

pytest.importorskip("temporalio")

from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from forze.base.primitives import uuid7
from forze_temporal.sandbox import sandboxed_workflow_runner

from ._workflow_defs import (
    SAGA_RECORDER,
    ItCheckoutSagaWorkflow,
    it_saga_charge,
    it_saga_reserve,
    it_saga_ship,
    it_saga_unreserve,
)

_ACTIVITIES = [it_saga_reserve, it_saga_unreserve, it_saga_charge, it_saga_ship]


async def _run_checkout(fail_at: str) -> object:
    SAGA_RECORDER.clear()
    env = await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter,
    )
    try:
        task_queue = "it-forze-saga"
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ItCheckoutSagaWorkflow],
            activities=_ACTIVITIES,
            workflow_runner=sandboxed_workflow_runner(),
        ):
            return await env.client.execute_workflow(
                ItCheckoutSagaWorkflow.run,
                fail_at,
                id=f"saga-{uuid7()}",
                task_queue=task_queue,
            )
    finally:
        await env.shutdown()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pre_pivot_failure_compensates_in_reverse() -> None:
    # The pivot (charge) fails before committing -> compensate the prior step (reserve).
    out = await _run_checkout("charge")

    assert out.status == "failed"
    assert "step_failed" in (out.code or "")
    assert SAGA_RECORDER == ["reserve", "charge", "unreserve"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_pivot_failure_fails_forward_without_compensation() -> None:
    # ship (retryable, after the pivot) fails -> forward-incomplete, NO compensation.
    out = await _run_checkout("ship")

    assert out.status == "failed"
    assert "forward_incomplete" in (out.code or "")
    assert SAGA_RECORDER == ["reserve", "charge", "ship"]

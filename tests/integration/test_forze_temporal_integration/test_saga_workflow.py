"""Integration test: a Temporal workflow drives the shared saga coordinator.

`TemporalSaga` runs activity-shaped steps with Forze's pivot/compensation semantics
(`SagaProgress`) on a real time-skipping Temporal server: a pre-pivot failure compensates
the completed steps in reverse; a post-pivot failure fails forward (no compensation).
Durability/retries are Temporal's; the saga semantics are the same code the in-process
executor runs.

The last two tests cover the reason the saga converts its `CoreException` into an
``ApplicationError`` at all — and settle it by *observation* rather than by reading the
``non_retryable`` flag: an uncaught saga failure must drive the run to ``FAILED``, while
the un-converted exception only fails the workflow *task*, which Temporal retries forever.
"""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest

pytest.importorskip("temporalio")

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from forze.base.primitives import uuid7
from forze_temporal.sandbox import sandboxed_workflow_runner

from ._workflow_defs import (
    SAGA_RECORDER,
    ItCheckoutSagaWorkflow,
    ItRawCoreFailureWorkflow,
    ItUncaughtSagaWorkflow,
    it_saga_charge,
    it_saga_reserve,
    it_saga_ship,
    it_saga_unreserve,
)

_ACTIVITIES = [it_saga_reserve, it_saga_unreserve, it_saga_charge, it_saga_ship]
_WORKFLOWS = [ItCheckoutSagaWorkflow, ItUncaughtSagaWorkflow, ItRawCoreFailureWorkflow]
_TASK_QUEUE = "it-forze-saga"


@asynccontextmanager
async def _saga_worker() -> AsyncIterator[Client]:
    """Time-skipping server plus a worker serving the saga workflows and activities."""

    SAGA_RECORDER.clear()
    env = await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter,
    )
    try:
        async with Worker(
            env.client,
            task_queue=_TASK_QUEUE,
            workflows=_WORKFLOWS,
            activities=_ACTIVITIES,
            workflow_runner=sandboxed_workflow_runner(),
        ):
            yield env.client

    finally:
        await env.shutdown()


async def _run_checkout(fail_at: str) -> object:
    async with _saga_worker() as client:
        return await client.execute_workflow(
            ItCheckoutSagaWorkflow.run,
            fail_at,
            id=f"saga-{uuid7()}",
            task_queue=_TASK_QUEUE,
        )


async def _event_types(handle) -> list[int]:
    history = await handle.fetch_history()
    return [event.event_type for event in history.events]


async def _await_workflow_task_failure(
    handle,
    *,
    timeout: timedelta = timedelta(seconds=20),
) -> list[int]:
    """Poll the history until a workflow *task* failure lands; return the event types."""

    deadline = time.monotonic() + timeout.total_seconds()

    while time.monotonic() < deadline:
        types = await _event_types(handle)

        if EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED in types:
            return types

        await asyncio.sleep(0.1)

    pytest.fail("no workflow task failure recorded — the control leg proves nothing")


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_uncaught_saga_failure_reaches_workflow_failed() -> None:
    """An uncaught saga failure terminates the run — observed on the server, not inferred."""

    async with _saga_worker() as client:
        handle = await client.start_workflow(
            ItUncaughtSagaWorkflow.run,
            "charge",
            id=f"saga-uncaught-{uuid7()}",
            task_queue=_TASK_QUEUE,
        )

        with pytest.raises(WorkflowFailureError) as ei:
            await handle.result()

        description = await handle.describe()
        types = await _event_types(handle)

    cause = ei.value.cause
    assert isinstance(cause, ApplicationError)
    assert cause.type == "saga.step_failed"
    assert cause.non_retryable is True

    # The run is terminal, and the history says the *workflow* failed — no retried
    # workflow tasks on the way there.
    assert description.status is WorkflowExecutionStatus.FAILED
    assert EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED in types
    assert EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED not in types

    # Compensation still ran on the way out, uncaught path included.
    assert SAGA_RECORDER == ["reserve", "charge", "unreserve"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unconverted_core_exception_leaves_the_run_alive() -> None:
    """Control: the shape without the conversion wedges the run instead of failing it.

    Pins the premise the conversion rests on. If Temporal ever started failing the
    workflow on a plain exception, this flips — and the mapping would deserve a rethink.
    """

    async with _saga_worker() as client:
        handle = await client.start_workflow(
            ItRawCoreFailureWorkflow.run,
            id=f"saga-raw-{uuid7()}",
            task_queue=_TASK_QUEUE,
        )

        types = await _await_workflow_task_failure(handle)
        description = await handle.describe()

    assert EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED in types
    assert EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED not in types
    assert description.status is WorkflowExecutionStatus.RUNNING

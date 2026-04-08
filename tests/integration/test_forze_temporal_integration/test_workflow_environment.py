"""Integration tests using Temporal's in-process time-skipping environment."""

import pytest

pytest.importorskip("temporalio")

from temporalio.worker import Worker

from ._workflow_defs import ItAddWorkflow, ItPingWorkflow, it_add_numbers


@pytest.mark.integration
@pytest.mark.asyncio
async def test_time_skipping_worker_runs_workflow_and_activity(workflow_env) -> None:
    """Workflow executes an activity and returns the result (validates test harness)."""

    task_queue = "test-forze-temporal-queue"
    async with Worker(
        workflow_env.client,
        task_queue=task_queue,
        workflows=[ItAddWorkflow],
        activities=[it_add_numbers],
    ):
        handle = await workflow_env.client.start_workflow(
            ItAddWorkflow.run,
            args=[21, 21],
            id="it-add-workflow-1",
            task_queue=task_queue,
        )

        result = await handle.result()

    assert result == 42


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_interceptor_subclass_is_composed_by_sdk(workflow_env) -> None:
    """A :class:`temporalio.worker.Interceptor` subclass is applied without breaking the worker."""

    from temporalio.worker import Interceptor, WorkflowInboundInterceptor

    class RecordingInterceptor(Interceptor):
        def workflow_interceptor_class(self, input):
            class Passthrough(WorkflowInboundInterceptor):
                pass

            return Passthrough

    task_queue = "test-forze-interceptor-queue"
    async with Worker(
        workflow_env.client,
        task_queue=task_queue,
        workflows=[ItPingWorkflow],
        interceptors=[RecordingInterceptor()],
    ):
        handle = await workflow_env.client.start_workflow(
            ItPingWorkflow.run,
            id="it-interceptor-ping-1",
            task_queue=task_queue,
        )

        assert await handle.result() == "pong"

"""Unit tests for :mod:`forze_temporal.kernel.client.workflow_mapping`."""

from datetime import datetime, timezone

import pytest

pytest.importorskip("temporalio")

from temporalio.client import WorkflowExecutionDescription, WorkflowExecutionStatus

from forze.application.contracts.durable.workflow import (
    DurableWorkflowRunDescription,
    DurableWorkflowRunStatus,
)
from forze_temporal.kernel.client.workflow_mapping import (
    description_from_temporal_execution,
    status_from_temporal,
)


def _execution_description(
    *,
    status: WorkflowExecutionStatus,
    workflow_id: str = "wf-1",
    run_id: str = "run-1",
    workflow_type: str = "ItSumWorkflow",
) -> WorkflowExecutionDescription:
    return WorkflowExecutionDescription(
        close_time=None,
        execution_time=None,
        history_length=1,
        id=workflow_id,
        namespace="default",
        parent_id=None,
        parent_run_id=None,
        root_id=None,
        root_run_id=None,
        raw_info=pytest.importorskip("temporalio").api.workflow.v1.WorkflowExecutionInfo(),  # type: ignore[attr-defined]
        run_id=run_id,
        search_attributes={},
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        status=status,
        task_queue="tq",
        typed_search_attributes=pytest.importorskip("temporalio").common.TypedSearchAttributes.empty,
        workflow_type=workflow_type,
        _context_free_data_converter=pytest.importorskip("temporalio").converter.default,
        raw_description=pytest.importorskip("temporalio").api.workflowservice.v1.DescribeWorkflowExecutionResponse(),
    )


class TestStatusFromTemporal:
    @pytest.mark.parametrize(
        ("temporal_status", "expected"),
        [
            (WorkflowExecutionStatus.RUNNING, DurableWorkflowRunStatus.RUNNING),
            (WorkflowExecutionStatus.COMPLETED, DurableWorkflowRunStatus.COMPLETED),
            (WorkflowExecutionStatus.FAILED, DurableWorkflowRunStatus.FAILED),
            (WorkflowExecutionStatus.CANCELED, DurableWorkflowRunStatus.CANCELLED),
            (WorkflowExecutionStatus.TERMINATED, DurableWorkflowRunStatus.TERMINATED),
            (
                WorkflowExecutionStatus.CONTINUED_AS_NEW,
                DurableWorkflowRunStatus.CONTINUED_AS_NEW,
            ),
            (WorkflowExecutionStatus.TIMED_OUT, DurableWorkflowRunStatus.TIMED_OUT),
        ],
    )
    def test_maps_all_statuses(
        self,
        temporal_status: WorkflowExecutionStatus,
        expected: DurableWorkflowRunStatus,
    ) -> None:
        assert status_from_temporal(temporal_status) == expected

    def test_none_status_raises(self) -> None:
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException):
            status_from_temporal(None)


class TestDescriptionFromTemporalExecution:
    def test_maps_core_fields(self) -> None:
        desc = _execution_description(status=WorkflowExecutionStatus.RUNNING)
        mapped = description_from_temporal_execution(desc)

        assert mapped == DurableWorkflowRunDescription(
            workflow_id="wf-1",
            run_id="run-1",
            workflow_name="ItSumWorkflow",
            status=DurableWorkflowRunStatus.RUNNING,
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            closed_at=None,
            failure_message=None,
            failure_type=None,
        )
        assert mapped.is_terminal is False

    def test_completed_is_terminal(self) -> None:
        desc = _execution_description(status=WorkflowExecutionStatus.COMPLETED)
        mapped = description_from_temporal_execution(desc)

        assert mapped.status == DurableWorkflowRunStatus.COMPLETED
        assert mapped.is_terminal is True

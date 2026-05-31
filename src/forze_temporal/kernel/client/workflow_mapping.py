"""Map Temporal workflow execution descriptions to Forze durable workflow models."""

from datetime import datetime, timezone

from temporalio.client import WorkflowExecutionDescription, WorkflowExecutionStatus

from forze.application.contracts.durable.workflow import (
    DurableWorkflowRunDescription,
    DurableWorkflowRunStatus,
)
from forze.base.exceptions import exc

# ----------------------- #


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt


# ....................... #


def status_from_temporal(
    status: WorkflowExecutionStatus | None,
) -> DurableWorkflowRunStatus:
    """Convert Temporal :class:`WorkflowExecutionStatus` to Forze form."""

    if status is None:
        raise exc.internal("Temporal workflow execution has no status")

    match status:
        case WorkflowExecutionStatus.RUNNING:
            return DurableWorkflowRunStatus.RUNNING
        case WorkflowExecutionStatus.COMPLETED:
            return DurableWorkflowRunStatus.COMPLETED
        case WorkflowExecutionStatus.FAILED:
            return DurableWorkflowRunStatus.FAILED
        case WorkflowExecutionStatus.CANCELED:
            return DurableWorkflowRunStatus.CANCELLED
        case WorkflowExecutionStatus.TERMINATED:
            return DurableWorkflowRunStatus.TERMINATED
        case WorkflowExecutionStatus.CONTINUED_AS_NEW:
            return DurableWorkflowRunStatus.CONTINUED_AS_NEW
        case WorkflowExecutionStatus.TIMED_OUT:
            return DurableWorkflowRunStatus.TIMED_OUT


# ....................... #


def description_from_temporal_execution(
    desc: WorkflowExecutionDescription,
) -> DurableWorkflowRunDescription:
    """Convert Temporal :class:`WorkflowExecutionDescription` to Forze form."""

    return DurableWorkflowRunDescription(
        workflow_id=desc.id,
        run_id=desc.run_id,
        workflow_name=desc.workflow_type,
        status=status_from_temporal(desc.status),
        started_at=_to_utc(desc.start_time),
        closed_at=_to_utc(desc.close_time),
        failure_message=None,
        failure_type=None,
    )

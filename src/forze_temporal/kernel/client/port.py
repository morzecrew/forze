"""Structural protocol for Temporal clients (single cluster or tenant-routed)."""

from typing import Any, Awaitable, Protocol

from pydantic import BaseModel
from temporalio.client import WorkflowHandle

from forze.application.contracts.durable.workflow import (
    DurableWorkflowRunDescription,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleTiming,
)

from .schedule_types import TemporalScheduleListPage

# ----------------------- #


class TemporalClientPort(Protocol):
    """Operations implemented by :class:`TemporalClient` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def start_workflow(
        self,
        queue: str,
        name: str,
        arg: BaseModel,
        *,
        workflow_id: str,
        raise_on_already_started: bool = True,
    ) -> Awaitable[WorkflowHandle[Any, Any]]: ...  # pragma: no cover

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> WorkflowHandle[Any, Any]: ...  # pragma: no cover

    def signal_workflow(
        self,
        workflow_id: str,
        *,
        signal: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def query_workflow(
        self,
        workflow_id: str,
        *,
        query: str,
        arg: BaseModel,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def describe_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Awaitable[DurableWorkflowRunDescription]: ...  # pragma: no cover

    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def terminate_workflow(
        self,
        workflow_id: str,
        *,
        reason: str | None = None,
        run_id: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def create_schedule(
        self,
        schedule_id: str,
        *,
        workflow_name: str,
        queue: str,
        arg: BaseModel,
        timing: DurableWorkflowScheduleTiming,
        workflow_id: str,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def update_schedule(
        self,
        schedule_id: str,
        *,
        workflow_name: str,
        queue: str,
        arg: BaseModel | None,
        timing: DurableWorkflowScheduleTiming | None,
        workflow_id: str | None,
        note: str | None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def delete_schedule(
        self, schedule_id: str
    ) -> Awaitable[None]: ...  # pragma: no cover

    def pause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def unpause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def trigger_schedule(
        self, schedule_id: str
    ) -> Awaitable[None]: ...  # pragma: no cover

    def describe_schedule(
        self,
        schedule_id: str,
    ) -> Awaitable[DurableWorkflowScheduleDescription]: ...  # pragma: no cover

    def list_schedules(
        self,
        *,
        workflow_name: str | None = None,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> Awaitable[TemporalScheduleListPage]: ...  # pragma: no cover

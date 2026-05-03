"""Structural protocol for Temporal clients (single cluster or tenant-routed)."""

from typing import Any, Awaitable, Protocol

from pydantic import BaseModel
from temporalio.client import WorkflowHandle

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
        self, workflow_id: str, *, run_id: str | None = None
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
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

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

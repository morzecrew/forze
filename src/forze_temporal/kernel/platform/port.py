"""Structural protocol for Temporal clients (single cluster or tenant-routed)."""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel
from temporalio.client import WorkflowHandle

# ----------------------- #


class TemporalClientPort(Protocol):
    """Operations implemented by :class:`TemporalClient` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    async def start_workflow(
        self,
        queue: str,
        name: str,
        arg: BaseModel,
        *,
        workflow_id: str,
        raise_on_already_started: bool = True,
    ) -> WorkflowHandle[Any, Any]:
        ...  # pragma: no cover

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> WorkflowHandle[Any, Any]:
        ...  # pragma: no cover

    async def signal_workflow(
        self,
        workflow_id: str,
        *,
        signal: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> None:
        ...  # pragma: no cover

    async def query_workflow(
        self,
        workflow_id: str,
        *,
        query: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        ...  # pragma: no cover

    async def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        ...  # pragma: no cover

    async def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Any:
        ...  # pragma: no cover

    async def cancel_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> None:
        ...  # pragma: no cover

    async def terminate_workflow(
        self,
        workflow_id: str,
        *,
        reason: str | None = None,
        run_id: str | None = None,
    ) -> None:
        ...  # pragma: no cover

"""Tests for forze.application.contracts.workflow.ports."""

from typing import Any, Awaitable, Optional, Sequence

from forze.application.contracts.workflow.ports import WorkflowPort
from forze.base.primitives import JsonDict


class _StubWorkflow:
    """Concrete implementation for testing WorkflowPort."""

    def __init__(self) -> None:
        self.started: list[tuple[str, str, Sequence[Any]]] = []
        self.signalled: list[tuple[str, str, Sequence[JsonDict]]] = []

    async def start(
        self,
        name: str,
        id: str,
        args: Sequence[Any],
        queue: Optional[str] = None,
    ) -> None:
        self.started.append((name, id, args))

    async def signal(
        self,
        id: str,
        signal: str,
        data: Sequence[JsonDict],
    ) -> None:
        self.signalled.append((id, signal, data))


class TestWorkflowPort:
    async def test_start(self) -> None:
        w = _StubWorkflow()
        await w.start("my_workflow", "wf-1", [{"key": "val"}])
        assert len(w.started) == 1
        assert w.started[0] == ("my_workflow", "wf-1", [{"key": "val"}])

    async def test_signal(self) -> None:
        w = _StubWorkflow()
        await w.signal("wf-1", "proceed", [{"step": 1}])
        assert len(w.signalled) == 1
        assert w.signalled[0] == ("wf-1", "proceed", [{"step": 1}])

    async def test_start_with_queue(self) -> None:
        w = _StubWorkflow()
        await w.start("wf", "id", [], queue="my-queue")
        assert len(w.started) == 1

    def test_protocol_structure(self) -> None:
        assert hasattr(WorkflowPort, "start")
        assert hasattr(WorkflowPort, "signal")

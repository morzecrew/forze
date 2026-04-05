"""Unit tests for :mod:`forze_temporal.kernel.platform.client`."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel
from temporalio.exceptions import WorkflowAlreadyStartedError

pytest.importorskip("temporalio")

from forze.base.errors import InfrastructureError
from forze_temporal.kernel.platform.client import TemporalClient, TemporalConfig


class _Arg(BaseModel):
    """Workflow argument model for tests."""

    n: int = 1


# ----------------------- #


class TestTemporalConfig:
    """Tests for :class:`TemporalConfig`."""

    def test_defaults(self) -> None:
        """Default namespace and lazy flag."""
        cfg = TemporalConfig()
        assert cfg.namespace == "default"
        assert cfg.lazy is False

    def test_custom(self) -> None:
        """Custom namespace and lazy."""
        cfg = TemporalConfig(namespace="other", lazy=True)
        assert cfg.namespace == "other"
        assert cfg.lazy is True


class TestTemporalClientLifecycle:
    """Initialize, close, and health checks."""

    @pytest.mark.asyncio
    async def test_initialize_connects_once(self) -> None:
        """Second initialize is a no-op when client already exists."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()

        with patch(
            "forze_temporal.kernel.platform.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ) as connect:
            client = TemporalClient()
            await client.initialize("localhost:7233")
            await client.initialize("localhost:7233")

        assert connect.await_count == 1

    @pytest.mark.asyncio
    async def test_close_clears_client(self) -> None:
        """close allows re-initialize after clearing."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()

        with patch(
            "forze_temporal.kernel.platform.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ) as connect:
            client = TemporalClient()
            await client.initialize("localhost:7233")
            await client.close()
            await client.initialize("other:7233")

        assert connect.await_count == 2

    @pytest.mark.asyncio
    async def test_health_ok(self) -> None:
        """health returns ok when count_workflows succeeds."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()

        with patch(
            "forze_temporal.kernel.platform.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ):
            client = TemporalClient()
            await client.initialize("localhost:7233")
            status, ok = await client.health()

        assert status == "ok"
        assert ok is True
        backend.count_workflows.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_health_failure_returns_message(self) -> None:
        """health catches exceptions and returns (str, False)."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock(side_effect=RuntimeError("unreachable"))

        with patch(
            "forze_temporal.kernel.platform.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ):
            client = TemporalClient()
            await client.initialize("localhost:7233")
            status, ok = await client.health()

        assert ok is False
        assert "unreachable" in status


class TestTemporalClientWorkflowApi:
    """start_workflow, handles, signal, query, update, result, cancel, terminate."""

    @staticmethod
    def _connected_client(backend: MagicMock) -> TemporalClient:
        client = TemporalClient()
        client._TemporalClient__client = backend  # type: ignore[attr-defined]
        return client

    @pytest.mark.asyncio
    async def test_operations_require_initialized_client(self) -> None:
        """Using API without initialize raises InfrastructureError."""
        client = TemporalClient()
        arg = _Arg()

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.start_workflow("q", "wf", arg, workflow_id="wid")

        with pytest.raises(InfrastructureError, match="not initialized"):
            client.get_workflow_handle("wid")

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.signal_workflow("wid", signal="s", arg=arg)

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.query_workflow("wid", query="q", arg=arg)

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.update_workflow("wid", update="u", arg=arg)

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.get_workflow_result("wid")

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.cancel_workflow("wid")

        with pytest.raises(InfrastructureError, match="not initialized"):
            await client.terminate_workflow("wid")

    @pytest.mark.asyncio
    async def test_start_workflow_success(self) -> None:
        """start_workflow delegates to temporal Client.start_workflow."""
        handle = MagicMock()
        backend = MagicMock()
        backend.start_workflow = AsyncMock(return_value=handle)

        client = self._connected_client(backend)
        arg = _Arg(n=2)

        out = await client.start_workflow("task-q", "MyWorkflow", arg, workflow_id="w-1")

        assert out is handle
        backend.start_workflow.assert_awaited_once_with(
            workflow="MyWorkflow",
            id="w-1",
            task_queue="task-q",
            arg=arg,
        )

    @pytest.mark.asyncio
    async def test_start_workflow_already_started_raises_by_default(self) -> None:
        """WorkflowAlreadyStartedError is re-raised when raise_on_already_started."""
        backend = MagicMock()
        err = WorkflowAlreadyStartedError("w-1", "wf", run_id="r1")
        backend.start_workflow = AsyncMock(side_effect=err)

        client = self._connected_client(backend)

        with pytest.raises(WorkflowAlreadyStartedError):
            await client.start_workflow("q", "wf", _Arg(), workflow_id="w-1")

    @pytest.mark.asyncio
    async def test_start_workflow_already_started_returns_handle(self) -> None:
        """When raise_on_already_started is False, get existing handle."""
        existing = MagicMock()
        backend = MagicMock()
        err = WorkflowAlreadyStartedError("w-1", "wf", run_id="r1")
        backend.start_workflow = AsyncMock(side_effect=err)
        backend.get_workflow_handle = MagicMock(return_value=existing)

        client = self._connected_client(backend)

        out = await client.start_workflow(
            "q",
            "wf",
            _Arg(),
            workflow_id="w-1",
            raise_on_already_started=False,
        )

        assert out is existing
        backend.get_workflow_handle.assert_called_once_with("w-1")

    @pytest.mark.asyncio
    async def test_get_workflow_handle(self) -> None:
        """get_workflow_handle forwards run_id."""
        handle = MagicMock()
        backend = MagicMock()
        backend.get_workflow_handle = MagicMock(return_value=handle)

        client = self._connected_client(backend)

        out = client.get_workflow_handle("w-1", run_id="r-9")

        assert out is handle
        backend.get_workflow_handle.assert_called_once_with("w-1", run_id="r-9")

    @pytest.mark.asyncio
    async def test_signal_query_update_result_cancel_terminate(self) -> None:
        """Workflow handle methods are invoked with expected arguments."""
        handle = MagicMock()
        handle.signal = AsyncMock()
        handle.query = AsyncMock(return_value={"ok": True})
        handle.execute_update = AsyncMock(return_value=42)
        handle.result = AsyncMock(return_value="done")
        handle.cancel = AsyncMock()
        handle.terminate = AsyncMock()

        backend = MagicMock()
        backend.get_workflow_handle = MagicMock(return_value=handle)

        client = self._connected_client(backend)
        arg = _Arg()

        await client.signal_workflow("w-1", signal="sig", arg=arg, run_id="r1")
        await client.query_workflow("w-1", query="q", arg=arg)
        await client.update_workflow("w-1", update="u", arg=arg)
        assert await client.get_workflow_result("w-1") == "done"
        await client.cancel_workflow("w-1")
        await client.terminate_workflow("w-1", reason="stop")

        handle.signal.assert_awaited_once_with(signal="sig", arg=arg)
        handle.query.assert_awaited_once_with(query="q", arg=arg)
        handle.execute_update.assert_awaited_once_with(update="u", arg=arg)
        handle.result.assert_awaited_once()
        handle.cancel.assert_awaited_once()
        handle.terminate.assert_awaited_once_with(reason="stop")

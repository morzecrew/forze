"""Unit tests for :mod:`forze_temporal.kernel.client.client`."""

from forze.base.exceptions import CoreException
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel
from temporalio.exceptions import WorkflowAlreadyStartedError

pytest.importorskip("temporalio")

from temporalio.client import TLSConfig
from temporalio.contrib.pydantic import pydantic_data_converter

from forze_temporal.kernel.client.client import TemporalClient, TemporalConfig

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

    def test_interceptors_optional(self) -> None:
        """Interceptors list is optional and forwarded on connect."""
        sentinel = object()
        cfg = TemporalConfig(interceptors=[sentinel])  # type: ignore[list-item]
        assert cfg.interceptors is not None
        assert cfg.interceptors[0] is sentinel

class TestTemporalConfigSecurity:
    """TLS / api-key / data-converter configuration."""

    def test_security_defaults(self) -> None:
        """Defaults: plaintext, no api key, no converter override."""
        cfg = TemporalConfig()
        assert cfg.tls is False
        assert cfg.api_key is None
        assert cfg.data_converter is None
        assert cfg.rpc_metadata is None

    def test_api_key_without_tls_raises(self) -> None:
        """api_key requires TLS to be enabled."""
        with pytest.raises(CoreException, match="requires TLS"):
            TemporalConfig(api_key="cloud-key")

        with pytest.raises(CoreException, match="requires TLS"):
            TemporalConfig(api_key="cloud-key", tls=False)

    def test_api_key_with_tls_accepted(self) -> None:
        """api_key works with tls=True or an explicit TLSConfig."""
        cfg = TemporalConfig(api_key="cloud-key", tls=True)
        assert cfg.api_key is not None
        assert cfg.api_key.get_secret_value() == "cloud-key"

        cfg = TemporalConfig(api_key="cloud-key", tls=TLSConfig())
        assert cfg.api_key is not None

    def test_api_key_not_leaked_in_repr(self) -> None:
        """The api key is excluded from repr entirely."""
        cfg = TemporalConfig(api_key="super-secret-key", tls=True)
        rendered = repr(cfg)

        assert "super-secret-key" not in rendered
        assert "api_key" not in rendered

class TestTemporalClientConnectKwargs:
    """Wiring of TemporalConfig into Client.connect."""

    @staticmethod
    def _backend() -> MagicMock:
        backend = MagicMock()
        backend.count_workflows = AsyncMock()
        return backend

    @pytest.mark.asyncio
    async def test_default_config_preserves_connect_kwargs(self) -> None:
        """Default config passes exactly the historical connect kwargs."""
        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
            new_callable=AsyncMock,
            return_value=self._backend(),
        ) as connect:
            client = TemporalClient()
            await client.initialize("localhost:7233")

        assert connect.await_args.args == ("localhost:7233",)
        kwargs = connect.await_args.kwargs
        assert set(kwargs) == {"namespace", "lazy", "data_converter", "interceptors"}
        assert kwargs["namespace"] == "default"
        assert kwargs["lazy"] is False
        assert kwargs["data_converter"] is pydantic_data_converter
        assert kwargs["interceptors"] == []

    @pytest.mark.asyncio
    async def test_security_options_propagate_to_connect(self) -> None:
        """tls/api_key/rpc_metadata/data_converter reach Client.connect."""
        tls_cfg = TLSConfig()
        converter = MagicMock()
        config = TemporalConfig(
            tls=tls_cfg,
            api_key="cloud-key",
            rpc_metadata={"x-custom": "1"},
            data_converter=converter,
        )

        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
            new_callable=AsyncMock,
            return_value=self._backend(),
        ) as connect:
            client = TemporalClient()
            await client.initialize("eu.cloud.temporal.io:7233", config=config)

        kwargs = connect.await_args.kwargs
        assert kwargs["tls"] is tls_cfg
        assert kwargs["api_key"] == "cloud-key"
        assert kwargs["rpc_metadata"] == {"x-custom": "1"}
        assert kwargs["data_converter"] is converter

    @pytest.mark.asyncio
    async def test_tls_true_propagates_without_api_key(self) -> None:
        """tls=True alone is forwarded; api_key stays unset."""
        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
            new_callable=AsyncMock,
            return_value=self._backend(),
        ) as connect:
            client = TemporalClient()
            await client.initialize(
                "localhost:7233",
                config=TemporalConfig(tls=True),
            )

        kwargs = connect.await_args.kwargs
        assert kwargs["tls"] is True
        assert "api_key" not in kwargs
        assert "rpc_metadata" not in kwargs

class TestTemporalClientLifecycle:
    """Initialize, close, and health checks."""

    @pytest.mark.asyncio
    async def test_initialize_connects_once(self) -> None:
        """Second initialize is a no-op when client already exists."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()

        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ) as connect:
            client = TemporalClient()
            await client.initialize("localhost:7233")
            await client.initialize("localhost:7233")

        assert connect.await_count == 1

    @pytest.mark.asyncio
    async def test_initialize_passes_interceptors_to_connect(self) -> None:
        """TemporalConfig.interceptors is passed through to Client.connect."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()
        marker = object()

        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
            new_callable=AsyncMock,
            return_value=backend,
        ) as connect:
            client = TemporalClient()
            await client.initialize(
                "localhost:7233",
                config=TemporalConfig(interceptors=[marker]),  # type: ignore[list-item]
            )

        assert connect.await_args.kwargs["interceptors"] == [marker]

    @pytest.mark.asyncio
    async def test_close_clears_client(self) -> None:
        """close allows re-initialize after clearing."""
        backend = MagicMock()
        backend.count_workflows = AsyncMock()

        with patch(
            "forze_temporal.kernel.client.client.Client.connect",
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
            "forze_temporal.kernel.client.client.Client.connect",
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
            "forze_temporal.kernel.client.client.Client.connect",
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

        with pytest.raises(CoreException, match="not initialized"):
            await client.start_workflow("q", "wf", arg, workflow_id="wid")

        with pytest.raises(CoreException, match="not initialized"):
            client.get_workflow_handle("wid")

        with pytest.raises(CoreException, match="not initialized"):
            await client.signal_workflow("wid", signal="s", arg=arg)

        with pytest.raises(CoreException, match="not initialized"):
            await client.query_workflow("wid", query="q", arg=arg)

        with pytest.raises(CoreException, match="not initialized"):
            await client.update_workflow("wid", update="u", arg=arg)

        with pytest.raises(CoreException, match="not initialized"):
            await client.get_workflow_result("wid")

        with pytest.raises(CoreException, match="not initialized"):
            await client.describe_workflow("wid")

        with pytest.raises(CoreException, match="not initialized"):
            await client.cancel_workflow("wid")

        with pytest.raises(CoreException, match="not initialized"):
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
        backend.get_workflow_handle.assert_called_once_with(
            "w-1",
            run_id="r-9",
            result_type=None,
        )

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
        handle.query.assert_awaited_once_with(query="q", arg=arg, result_type=None)
        handle.execute_update.assert_awaited_once_with(
            update="u",
            arg=arg,
            result_type=None,
        )
        handle.result.assert_awaited_once()
        handle.cancel.assert_awaited_once()
        handle.terminate.assert_awaited_once_with(reason="stop")

    @pytest.mark.asyncio
    async def test_describe_workflow(self) -> None:
        """describe_workflow maps Temporal describe via workflow_mapping."""
        from unittest.mock import patch

        from forze.application.contracts.durable.workflow import (
            DurableWorkflowRunDescription,
            DurableWorkflowRunStatus,
        )

        handle = MagicMock()
        temporal_desc = MagicMock()
        handle.describe = AsyncMock(return_value=temporal_desc)

        backend = MagicMock()
        backend.get_workflow_handle = MagicMock(return_value=handle)

        mapped = DurableWorkflowRunDescription(
            workflow_id="w-1",
            run_id="r-9",
            workflow_name="MyWorkflow",
            status=DurableWorkflowRunStatus.RUNNING,
        )

        client = self._connected_client(backend)

        with patch(
            "forze_temporal.kernel.client.client.description_from_temporal_execution",
            return_value=mapped,
        ) as map_fn:
            out = await client.describe_workflow("w-1", run_id="r-9")

        assert out is mapped
        backend.get_workflow_handle.assert_called_once_with(
            "w-1",
            run_id="r-9",
            result_type=None,
        )
        handle.describe.assert_awaited_once()
        map_fn.assert_called_once_with(temporal_desc)

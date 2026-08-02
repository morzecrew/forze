"""Unit tests for :mod:`forze_temporal.kernel.client.client`."""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel
from temporalio.exceptions import WorkflowAlreadyStartedError

from forze.base.exceptions import CoreException, ExceptionKind

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


def _page_token(index: int) -> bytes:
    """Server-opaque token addressing page *index* of the fake listing."""

    return f"page-{index}".encode()


class _FakeScheduleIterator:
    """Stands in for the SDK's ``ScheduleAsyncIterator``, page semantics included.

    Faithful to the contract the client depends on: ``fetch_next_page`` replaces
    ``current_page`` and rolls ``next_page_token`` forward to the page *after* the one
    just fetched — so a token captured after a fetch no longer addresses the page being
    read. That is exactly the trap the resume cursor has to avoid.
    """

    def __init__(self, pages: list[list[object]], *, start_token: bytes | None) -> None:
        self._pages = pages
        self._index = int(start_token.decode().removeprefix("page-")) if start_token else 0
        self.current_page: list[object] | None = None
        self.current_page_index = 0
        self.next_page_token: bytes | None = start_token

    async def fetch_next_page(self) -> None:
        self.current_page = self._pages[self._index] if self._index < len(self._pages) else []
        self.current_page_index = 0
        self._index += 1
        self.next_page_token = (
            _page_token(self._index) if self._index < len(self._pages) else None
        )


def _paged_backend(pages: list[list[object]]) -> MagicMock:
    """Backend whose ``list_schedules`` honours the resume token it is handed."""

    backend = MagicMock()

    async def _list_schedules(*, page_size: int, next_page_token: bytes | None):
        return _FakeScheduleIterator(pages, start_token=next_page_token)

    backend.list_schedules = AsyncMock(side_effect=_list_schedules)
    return backend


class TestTemporalClientListSchedules:
    """Schedule listing filters (workflow name, id prefix, limit)."""

    @staticmethod
    def _connected_client(backend: MagicMock) -> TemporalClient:
        client = TemporalClient()
        client._TemporalClient__client = backend  # type: ignore[attr-defined]
        return client

    @staticmethod
    def _desc(schedule_id: str):
        from datetime import timedelta

        from forze.application.contracts.durable.workflow import (
            DurableWorkflowScheduleDescription,
            DurableWorkflowScheduleTiming,
        )

        return DurableWorkflowScheduleDescription(
            schedule_id=schedule_id,
            workflow_name="Wf",
            paused=False,
            timing=DurableWorkflowScheduleTiming(interval=timedelta(minutes=5)),
        )

    def _mapper(self):
        """Map a fake entry id straight to a description carrying that id."""

        return patch(
            "forze_temporal.kernel.client.client.description_from_list_entry",
            side_effect=lambda entry: self._desc(str(entry)),
        )

    @pytest.mark.asyncio
    async def test_schedule_id_prefix_filters_before_limit(self) -> None:
        """Foreign-prefix entries are skipped and do not consume the limit."""

        backend = _paged_backend([["tenant:a:s1", "tenant:b:s1", "tenant:a:s2"]])
        client = self._connected_client(backend)

        with self._mapper():
            page = await client.list_schedules(
                limit=2,
                schedule_id_prefix="tenant:a:",
            )

        ids = tuple(d.schedule_id for d in page.descriptions)
        assert ids == ("tenant:a:s1", "tenant:a:s2")

    @pytest.mark.asyncio
    async def test_no_prefix_returns_all(self) -> None:
        backend = _paged_backend([["tenant:a:s1", "plain"]])
        client = self._connected_client(backend)

        with self._mapper():
            page = await client.list_schedules()

        assert len(page.descriptions) == 2

    @pytest.mark.asyncio
    async def test_unlimited_listing_walks_every_page(self) -> None:
        """Without a limit the walk drains all pages and ends with no cursor."""

        backend = _paged_backend([["s1", "s2"], ["s3"], ["s4", "s5"]])
        client = self._connected_client(backend)

        with self._mapper():
            page = await client.list_schedules()

        assert tuple(d.schedule_id for d in page.descriptions) == ("s1", "s2", "s3", "s4", "s5")
        assert page.next_page_token is None

    @pytest.mark.asyncio
    async def test_filtered_limit_hands_back_the_un_yielded_tail(self) -> None:
        """A limit reached mid-page resumes *inside* that page, losing no entry.

        The filters run client-side, so the limit can be hit before the page is
        exhausted. Resuming from the page's *next* token (what the iterator exposes
        after the fetch) would silently drop everything left in the page — here
        ``tenant:a:s3`` — and no caller could tell, since the page still looked full.
        """

        pages: list[list[object]] = [
            ["tenant:a:s1", "tenant:b:x1"],
            ["tenant:a:s2", "tenant:a:s3"],
            ["tenant:a:s4"],
        ]
        backend = _paged_backend(pages)
        client = self._connected_client(backend)

        collected: list[str] = []
        cursor: str | None = None

        with self._mapper():
            for _ in range(4):  # bounded: 4 matches at 2 per call must finish sooner
                page = await client.list_schedules(
                    limit=2,
                    next_page_token=cursor,
                    schedule_id_prefix="tenant:a:",
                )
                collected.extend(d.schedule_id for d in page.descriptions)
                cursor = page.next_page_token

                if cursor is None:
                    break

        assert cursor is None, "pagination did not terminate"
        assert collected == ["tenant:a:s1", "tenant:a:s2", "tenant:a:s3", "tenant:a:s4"]

    @pytest.mark.asyncio
    async def test_first_page_tail_survives_without_an_incoming_token(self) -> None:
        """The very first page has no token of its own — the offset still resumes it."""

        backend = _paged_backend([["s1", "s2", "s3"]])
        client = self._connected_client(backend)

        with self._mapper():
            first = await client.list_schedules(limit=2)

            assert first.next_page_token is not None
            second = await client.list_schedules(limit=2, next_page_token=first.next_page_token)

        assert tuple(d.schedule_id for d in first.descriptions) == ("s1", "s2")
        assert tuple(d.schedule_id for d in second.descriptions) == ("s3",)
        assert second.next_page_token is None

    @pytest.mark.asyncio
    async def test_page_boundary_cursor_stays_a_bare_token(self) -> None:
        """Stopping exactly at a page end emits the plain page token (no offset part)."""

        backend = _paged_backend([["s1", "s2"], ["s3"]])
        client = self._connected_client(backend)

        with self._mapper():
            page = await client.list_schedules(limit=2)

            assert page.next_page_token == base64.urlsafe_b64encode(_page_token(1)).decode()

            resumed = await client.list_schedules(next_page_token=page.next_page_token)

        assert tuple(d.schedule_id for d in resumed.descriptions) == ("s3",)

    @pytest.mark.asyncio
    async def test_malformed_cursor_is_rejected_as_validation(self) -> None:
        """A caller-supplied cursor that is not ours fails as validation, not internally."""

        client = self._connected_client(_paged_backend([["s1"]]))

        with pytest.raises(CoreException) as ei:
            await client.list_schedules(next_page_token="page-0.not-a-number")

        assert ei.value.kind is ExceptionKind.VALIDATION
        assert ei.value.code == "schedule.page_token_invalid"

    @pytest.mark.asyncio
    async def test_cursor_offset_past_the_page_is_rejected_not_skipped(self) -> None:
        """An offset no cursor of ours can mint must refuse, never drop the page silently."""

        client = self._connected_client(_paged_backend([["s1", "s2"], ["s3"]]))
        # Offset 9 into a 2-entry page: falling through would return ("s3",) and report a
        # complete listing, losing s1 and s2 with nothing to signal it.
        cursor = f"{base64.urlsafe_b64encode(_page_token(0)).decode()}.9"

        with self._mapper(), pytest.raises(CoreException) as ei:
            await client.list_schedules(next_page_token=cursor)

        assert ei.value.kind is ExceptionKind.VALIDATION
        assert ei.value.code == "schedule.page_token_invalid"

    @pytest.mark.asyncio
    async def test_cursor_offset_at_the_page_end_still_advances(self) -> None:
        """The boundary case is lossless, so it stays allowed — only *past* the end refuses."""

        client = self._connected_client(_paged_backend([["s1", "s2"], ["s3"]]))
        cursor = f"{base64.urlsafe_b64encode(_page_token(0)).decode()}.2"

        with self._mapper():
            page = await client.list_schedules(next_page_token=cursor)

        assert tuple(d.schedule_id for d in page.descriptions) == ("s3",)

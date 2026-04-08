"""Unit tests for :mod:`forze_temporal.adapters.workflow` and base adapter IDs."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

pytest.importorskip("temporalio")

from forze.application.contracts.workflow import (
    WorkflowHandle,
    WorkflowQuerySpec,
    WorkflowSignalSpec,
    WorkflowSpec,
    WorkflowUpdateSpec,
)
from forze.application.contracts.workflow.specs import WorkflowInvokeSpec
from forze.base.errors import CoreError
from forze_temporal.adapters.workflow import TemporalWorkflowCommandAdapter, TemporalWorkflowQueryAdapter
from forze_temporal.kernel.platform.client import TemporalClient


class _In(BaseModel):
    x: int = 0


class _Out(BaseModel):
    y: str = "ok"


class _Sig(BaseModel):
    s: int = 1


class _QIn(BaseModel):
    q: int = 2


class _QOut(BaseModel):
    r: str = "a"


class _UpIn(BaseModel):
    u: int = 3


class _UpOut(BaseModel):
    v: int = 4


def _spec() -> WorkflowSpec[_In, _Out]:
    return WorkflowSpec(
        name="Wf",
        run=WorkflowInvokeSpec(args_type=_In, return_type=_Out),
        signals={"sig": WorkflowSignalSpec(name="sig", args_type=_Sig)},
        queries={"q": WorkflowQuerySpec(name="q", args_type=_QIn, return_type=_QOut)},
        updates={"up": WorkflowUpdateSpec(name="up", args_type=_UpIn, return_type=_UpOut)},
    )


def _client() -> TemporalClient:
    c = TemporalClient()
    c._TemporalClient__client = MagicMock()  # type: ignore[attr-defined]
    return c


class TestTemporalWorkflowCommandAdapter:
    @pytest.mark.asyncio
    async def test_start_delegates_to_client(self) -> None:
        client = TemporalClient()
        backend = MagicMock()
        wh = MagicMock()
        wh.id = "wid-1"
        wh.run_id = "run-9"
        backend.start_workflow = AsyncMock(return_value=wh)
        object.__setattr__(client, "_TemporalClient__client", backend)

        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=client,
            queue="tq-a",
            spec=spec,
            tenant_aware=False,
        )
        arg = _In(x=5)

        handle = await adapter.start(arg, workflow_id="custom-1")

        assert handle == WorkflowHandle(workflow_id="wid-1", run_id="run-9")
        backend.start_workflow.assert_awaited_once_with(
            workflow="Wf",
            id="custom-1",
            task_queue="tq-a",
            arg=arg,
        )

    @pytest.mark.asyncio
    async def test_signal_update_cancel_terminate(self) -> None:
        client = TemporalClient()
        backend = MagicMock()
        wh = MagicMock()
        wh.signal = AsyncMock()
        wh.execute_update = AsyncMock(return_value=_UpOut(v=9))
        wh.cancel = AsyncMock()
        wh.terminate = AsyncMock()
        backend.get_workflow_handle = MagicMock(return_value=wh)
        object.__setattr__(client, "_TemporalClient__client", backend)

        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=client,
            queue="tq",
            spec=spec,
            tenant_aware=False,
        )
        h = WorkflowHandle(workflow_id="w1", run_id="r1")
        await adapter.signal(h, signal=spec.signals["sig"], args=_Sig(s=2))
        out = await adapter.update(h, update=spec.updates["up"], args=_UpIn(u=3))
        await adapter.cancel(h)
        await adapter.terminate(h, reason="bye")

        assert out.v == 9
        backend.get_workflow_handle.assert_called_with("w1", run_id="r1")
        wh.signal.assert_awaited_once_with(signal="sig", arg=_Sig(s=2))
        wh.execute_update.assert_awaited_once_with(update="up", arg=_UpIn(u=3))
        wh.cancel.assert_awaited_once()
        wh.terminate.assert_awaited_once_with(reason="bye")


class TestTemporalWorkflowQueryAdapter:
    @pytest.mark.asyncio
    async def test_query_and_result(self) -> None:
        client = TemporalClient()
        backend = MagicMock()
        wh = MagicMock()
        wh.query = AsyncMock(return_value=_QOut(r="x"))
        wh.result = AsyncMock(return_value=_Out(y="done"))
        backend.get_workflow_handle = MagicMock(return_value=wh)
        object.__setattr__(client, "_TemporalClient__client", backend)

        spec = _spec()
        adapter = TemporalWorkflowQueryAdapter(
            client=client,
            queue="tq",
            spec=spec,
            tenant_aware=False,
        )
        h = WorkflowHandle(workflow_id="w2", run_id="r2")

        qo = await adapter.query(h, query=spec.queries["q"], args=_QIn(q=1))
        res = await adapter.result(h)

        assert qo.r == "x"
        assert res.y == "done"
        backend.get_workflow_handle.assert_called_with("w2", run_id="r2")
        wh.query.assert_awaited_once_with(query="q", arg=_QIn(q=1))
        wh.result.assert_awaited_once()


class TestTemporalBaseAdapterWorkflowId:
    def test_construct_workflow_id_plain(self) -> None:
        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=spec,
            tenant_aware=False,
            workflow_id_factory=lambda: "gen-id-1",
        )
        assert adapter.construct_workflow_id("manual") == "manual"
        assert adapter.construct_workflow_id(None) == "gen-id-1"

    def test_construct_workflow_id_tenant_prefixed(self) -> None:
        from uuid import UUID

        tid = UUID("018f1234-5678-7abc-8def-123456789abc")
        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=spec,
            tenant_aware=True,
            tenant_provider=lambda: tid,
        )
        assert (
            adapter.construct_workflow_id("job-1")
            == f"tenant:{tid}:job-1"
        )

    def test_tenant_aware_without_provider_raises(self) -> None:
        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=spec,
            tenant_aware=True,
            tenant_provider=None,
        )
        with pytest.raises(CoreError, match="Tenant provider"):
            adapter.construct_workflow_id("x")

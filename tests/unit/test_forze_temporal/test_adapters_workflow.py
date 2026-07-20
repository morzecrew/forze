"""Unit tests for :mod:`forze_temporal.adapters.workflow` and base adapter IDs."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException

pytest.importorskip("temporalio")

from forze.application.contracts.durable.workflow import (
    DurableWorkflowHandle,
    DurableWorkflowQuerySpec,
    DurableWorkflowRunDescription,
    DurableWorkflowRunStatus,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_temporal.adapters.workflow import (
    TemporalWorkflowCommandAdapter,
    TemporalWorkflowQueryAdapter,
)
from forze_temporal.kernel.client.client import TemporalClient


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


def _spec() -> DurableWorkflowSpec[_In, _Out]:
    return DurableWorkflowSpec(
        name="Wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=_Out),
        signals={"sig": DurableWorkflowSignalSpec(name="sig", args_type=_Sig)},
        queries={"q": DurableWorkflowQuerySpec(name="q", args_type=_QIn, return_type=_QOut)},
        updates={
            "up": DurableWorkflowUpdateSpec(name="up", args_type=_UpIn, return_type=_UpOut)
        },
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

        assert handle == DurableWorkflowHandle(workflow_id="wid-1", run_id="run-9")
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
        h = DurableWorkflowHandle(workflow_id="w1", run_id="r1")
        await adapter.signal(h, signal=spec.signals["sig"], args=_Sig(s=2))
        out = await adapter.update(h, update=spec.updates["up"], args=_UpIn(u=3))
        await adapter.cancel(h)
        await adapter.terminate(h, reason="bye")

        assert out.v == 9
        backend.get_workflow_handle.assert_called_with(
            "w1",
            run_id="r1",
            result_type=None,
        )
        wh.signal.assert_awaited_once_with(signal="sig", arg=_Sig(s=2))
        wh.execute_update.assert_awaited_once_with(
            update="up",
            arg=_UpIn(u=3),
            result_type=_UpOut,
        )
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
        h = DurableWorkflowHandle(workflow_id="w2", run_id="r2")

        qo = await adapter.query(h, query=spec.queries["q"], args=_QIn(q=1))
        res = await adapter.result(h)

        assert qo.r == "x"
        assert res.y == "done"
        # The last handle fetch is result()'s, carrying the workflow's run
        # return type so the data converter deserializes into the spec model.
        backend.get_workflow_handle.assert_called_with(
            "w2",
            run_id="r2",
            result_type=_Out,
        )
        wh.query.assert_awaited_once_with(query="q", arg=_QIn(q=1), result_type=_QOut)
        wh.result.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_describe_delegates_to_client(self) -> None:
        client = MagicMock(spec=TemporalClient)
        run_desc = DurableWorkflowRunDescription(
            workflow_id="w2",
            run_id="r2",
            workflow_name="Wf",
            status=DurableWorkflowRunStatus.COMPLETED,
        )
        client.describe_workflow = AsyncMock(return_value=run_desc)

        spec = _spec()
        adapter = TemporalWorkflowQueryAdapter(
            client=client,
            queue="tq",
            spec=spec,
            tenant_aware=False,
        )
        h = DurableWorkflowHandle(workflow_id="w2", run_id="r2")

        out = await adapter.describe(h)

        assert out is run_desc
        client.describe_workflow.assert_awaited_once_with(
            workflow_id="w2",
            run_id="r2",
        )

    @pytest.mark.asyncio
    async def test_describe_wrong_workflow_name_raises_not_found(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.describe_workflow = AsyncMock(
            return_value=DurableWorkflowRunDescription(
                workflow_id="w2",
                run_id="r2",
                workflow_name="OtherWorkflow",
                status=DurableWorkflowRunStatus.RUNNING,
            ),
        )

        adapter = TemporalWorkflowQueryAdapter(
            client=client,
            queue="tq",
            spec=_spec(),
            tenant_aware=False,
        )

        with pytest.raises(CoreException, match="not for workflow"):
            await adapter.describe(DurableWorkflowHandle(workflow_id="w2", run_id="r2"))


class TestTemporalWorkflowTenantScoping:
    """Handle ops must address only the active tenant's workflow id-space."""

    from uuid import UUID as _UUID

    _tid = _UUID("00000000-0000-7000-8000-0000000000aa")
    _other = _UUID("00000000-0000-7000-8000-0000000000bb")

    def _command_adapter(
        self, client: TemporalClient
    ) -> TemporalWorkflowCommandAdapter[_In, _Out]:
        return TemporalWorkflowCommandAdapter(
            client=client,
            queue="tq",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=self._tid),
        )

    def _query_adapter(
        self, client: TemporalClient
    ) -> TemporalWorkflowQueryAdapter[_In, _Out]:
        return TemporalWorkflowQueryAdapter(
            client=client,
            queue="tq",
            spec=_spec(),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=self._tid),
        )

    @pytest.mark.asyncio
    async def test_command_ops_prefix_raw_handle_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.signal_workflow = AsyncMock()
        client.update_workflow = AsyncMock(return_value=_UpOut(v=1))
        client.cancel_workflow = AsyncMock()
        client.terminate_workflow = AsyncMock()

        spec = _spec()
        adapter = self._command_adapter(client)
        h = DurableWorkflowHandle(workflow_id="w1", run_id="r1")

        await adapter.signal(h, signal=spec.signals["sig"], args=_Sig(s=2))
        await adapter.update(h, update=spec.updates["up"], args=_UpIn(u=3))
        await adapter.cancel(h)
        await adapter.terminate(h, reason="bye")

        wid = f"tenant:{self._tid}:w1"
        client.signal_workflow.assert_awaited_once_with(
            workflow_id=wid,
            signal="sig",
            arg=_Sig(s=2),
            run_id="r1",
        )
        client.update_workflow.assert_awaited_once_with(
            workflow_id=wid,
            update="up",
            arg=_UpIn(u=3),
            run_id="r1",
            result_type=_UpOut,
        )
        client.cancel_workflow.assert_awaited_once_with(workflow_id=wid, run_id="r1")
        client.terminate_workflow.assert_awaited_once_with(
            workflow_id=wid,
            reason="bye",
            run_id="r1",
        )

    @pytest.mark.asyncio
    async def test_command_ops_pass_through_own_prefixed_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.cancel_workflow = AsyncMock()

        adapter = self._command_adapter(client)
        wid = f"tenant:{self._tid}:w1"

        await adapter.cancel(DurableWorkflowHandle(workflow_id=wid, run_id="r1"))

        client.cancel_workflow.assert_awaited_once_with(workflow_id=wid, run_id="r1")

    @pytest.mark.asyncio
    async def test_command_ops_reject_foreign_tenant_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.signal_workflow = AsyncMock()
        client.cancel_workflow = AsyncMock()

        spec = _spec()
        adapter = self._command_adapter(client)
        h = DurableWorkflowHandle(workflow_id=f"tenant:{self._other}:w1", run_id="r1")

        with pytest.raises(CoreException, match="outside the active tenant"):
            await adapter.signal(h, signal=spec.signals["sig"], args=_Sig(s=2))

        with pytest.raises(CoreException, match="outside the active tenant"):
            await adapter.cancel(h)

        client.signal_workflow.assert_not_awaited()
        client.cancel_workflow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_query_ops_prefix_raw_handle_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.query_workflow = AsyncMock(return_value=_QOut(r="x"))
        client.get_workflow_result = AsyncMock(return_value=_Out(y="done"))

        wid = f"tenant:{self._tid}:w2"
        run_desc = DurableWorkflowRunDescription(
            workflow_id=wid,
            run_id="r2",
            workflow_name="Wf",
            status=DurableWorkflowRunStatus.COMPLETED,
        )
        client.describe_workflow = AsyncMock(return_value=run_desc)

        spec = _spec()
        adapter = self._query_adapter(client)
        h = DurableWorkflowHandle(workflow_id="w2", run_id="r2")

        await adapter.query(h, query=spec.queries["q"], args=_QIn(q=1))
        await adapter.result(h)
        await adapter.describe(h)

        client.query_workflow.assert_awaited_once_with(
            workflow_id=wid,
            query="q",
            arg=_QIn(q=1),
            run_id="r2",
            result_type=_QOut,
        )
        client.get_workflow_result.assert_awaited_once_with(
            workflow_id=wid,
            run_id="r2",
            result_type=_Out,
        )
        client.describe_workflow.assert_awaited_once_with(
            workflow_id=wid,
            run_id="r2",
        )

    @pytest.mark.asyncio
    async def test_query_ops_reject_foreign_tenant_ids(self) -> None:
        client = MagicMock(spec=TemporalClient)
        client.query_workflow = AsyncMock()
        client.get_workflow_result = AsyncMock()
        client.describe_workflow = AsyncMock()

        spec = _spec()
        adapter = self._query_adapter(client)
        h = DurableWorkflowHandle(workflow_id=f"tenant:{self._other}:w2", run_id="r2")

        with pytest.raises(CoreException, match="outside the active tenant"):
            await adapter.query(h, query=spec.queries["q"], args=_QIn(q=1))

        with pytest.raises(CoreException, match="outside the active tenant"):
            await adapter.result(h)

        with pytest.raises(CoreException, match="outside the active tenant"):
            await adapter.describe(h)

        client.query_workflow.assert_not_awaited()
        client.get_workflow_result.assert_not_awaited()
        client.describe_workflow.assert_not_awaited()

    def test_resolve_workflow_id_semantics(self) -> None:
        adapter = self._command_adapter(_client())

        assert adapter.resolve_workflow_id("w") == f"tenant:{self._tid}:w"
        assert (
            adapter.resolve_workflow_id(f"tenant:{self._tid}:w")
            == f"tenant:{self._tid}:w"
        )

        with pytest.raises(CoreException, match="outside the active tenant"):
            adapter.resolve_workflow_id(f"tenant:{self._other}:w")

    def test_resolve_workflow_id_verbatim_without_tenancy(self) -> None:
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="tq",
            spec=_spec(),
            tenant_aware=False,
        )

        assert adapter.resolve_workflow_id("w") == "w"
        assert adapter.resolve_workflow_id("tenant:foo:w") == "tenant:foo:w"


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

    def test_default_workflow_id_factory_yields_distinct_valid_uuids(self) -> None:
        from uuid import UUID

        adapter_one = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=_spec(),
            tenant_aware=False,
        )
        adapter_two = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=_spec(),
            tenant_aware=False,
        )

        id_one = adapter_one.construct_workflow_id(None)
        id_two = adapter_two.construct_workflow_id(None)

        # Each default id must be a real UUID string (regression: the factory
        # previously stringified the ``uuid4`` function itself, so every id was
        # the identical ``"<function uuid4 ...>"`` garbage and collided).
        assert str(UUID(id_one)) == id_one
        assert str(UUID(id_two)) == id_two
        assert id_one != id_two

    def test_construct_workflow_id_tenant_prefixed(self) -> None:
        from uuid import UUID

        tid = UUID("018f1234-5678-7abc-8def-123456789abc")
        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=spec,
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        )
        assert adapter.construct_workflow_id("job-1") == f"tenant:{tid}:job-1"

    def test_tenant_aware_without_provider_raises(self) -> None:
        spec = _spec()
        adapter = TemporalWorkflowCommandAdapter(
            client=_client(),
            queue="q",
            spec=spec,
            tenant_aware=True,
            tenant_provider=None,
        )
        with pytest.raises(CoreException, match="Tenant provider"):
            adapter.construct_workflow_id("x")

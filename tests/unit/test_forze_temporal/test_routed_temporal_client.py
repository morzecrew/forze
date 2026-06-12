"""Unit tests for :class:`~forze_temporal.kernel.client.RoutedTemporalClient`."""

from forze.base.exceptions import CoreException, exc
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

pytest.importorskip("temporalio")

from forze.application.contracts.secrets import SecretRef
from forze_temporal.kernel.client import RoutedTemporalClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

class _MemSecrets:
    def __init__(self, hosts: dict[UUID, str]) -> None:
        self.hosts = hosts

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, host in self.hosts.items():
            if ref.path == f"tenants/{tid}/temporal":
                return host
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/temporal" for tid in self.hosts)

def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/temporal")

@pytest.mark.asyncio
async def test_routed_temporal_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    tenant: UUID | None = None

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        await routed.health()

@pytest.mark.asyncio
async def test_routed_temporal_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: "host-a:7233",
            _T2: "host-b:7233",
        }
    )
    cur: UUID | None = None

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    instances: list[MagicMock] = []

    def _make_client() -> MagicMock:
        inst = MagicMock()
        inst.initialize = AsyncMock()
        inst.close = AsyncMock()
        inst.health = AsyncMock(return_value=("ok", True))
        instances.append(inst)
        return inst

    with patch(
        "forze_temporal.kernel.client.routed_client.TemporalClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1

def test_routed_temporal_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    with pytest.raises(CoreException, match="max_entries"):
        RoutedTemporalClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )

@pytest.mark.asyncio
async def test_routed_temporal_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.health()

@pytest.mark.asyncio
async def test_routed_temporal_delegates_to_inner_client() -> None:
    from pydantic import BaseModel

    class _Arg(BaseModel):
        n: int = 1

    secrets = _MemSecrets({_T1: "localhost:7233"})
    cur: UUID | None = _T1

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=4,
    )
    await routed.startup()

    inner = MagicMock()
    inner.initialize = AsyncMock()
    inner.health = AsyncMock(return_value=("ok", True))
    inner.signal_workflow = AsyncMock()
    inner.query_workflow = AsyncMock(return_value={"q": 1})
    inner.update_workflow = AsyncMock(return_value={"u": 1})
    inner.get_workflow_result = AsyncMock(return_value={"r": 1})
    inner.describe_workflow = AsyncMock(return_value=MagicMock())
    inner.cancel_workflow = AsyncMock()
    inner.terminate_workflow = AsyncMock()
    inner.create_schedule = AsyncMock()
    inner.update_schedule = AsyncMock()
    inner.delete_schedule = AsyncMock()
    inner.pause_schedule = AsyncMock()
    inner.unpause_schedule = AsyncMock()
    inner.trigger_schedule = AsyncMock()
    inner.describe_schedule = AsyncMock(return_value=MagicMock())
    inner.list_schedules = AsyncMock(return_value=MagicMock())
    wf_handle = MagicMock()
    inner.get_workflow_handle = MagicMock(return_value=wf_handle)

    arg = _Arg()

    with patch(
        "forze_temporal.kernel.client.routed_client.TemporalClient",
        return_value=inner,
    ):
        await routed.health()
        await routed.signal_workflow("wf", signal="sig", arg=arg, run_id="run-1")
        await routed.query_workflow("wf", query="qry", arg=arg)
        await routed.update_workflow("wf", update="upd", arg=arg)
        await routed.get_workflow_result("wf", run_id="run-1")
        await routed.describe_workflow("wf")
        await routed.cancel_workflow("wf")
        await routed.terminate_workflow("wf", reason="done")
        await routed.create_schedule(
            "sched",
            workflow_name="Wf",
            queue="q",
            arg=arg,
            timing=MagicMock(),
            workflow_id="wf-sched",
        )
        await routed.update_schedule(
            "sched",
            workflow_name="Wf",
            queue="q",
            arg=arg,
            timing=None,
            workflow_id=None,
            note=None,
        )
        await routed.delete_schedule("sched")
        await routed.pause_schedule("sched", note="pause")
        await routed.unpause_schedule("sched")
        await routed.trigger_schedule("sched")
        await routed.describe_schedule("sched")
        await routed.list_schedules(workflow_name="Wf", limit=10)

        assert routed.get_workflow_handle("wf", run_id="run-1") is wf_handle

    inner.signal_workflow.assert_awaited_once_with(
        "wf",
        signal="sig",
        arg=arg,
        run_id="run-1",
    )
    inner.get_workflow_handle.assert_called_once_with(
        "wf",
        run_id="run-1",
        result_type=None,
    )


@pytest.mark.asyncio
async def test_routed_temporal_get_workflow_handle_requires_cache() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: _T1,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="No Temporal client"):
        routed.get_workflow_handle("wf-1")

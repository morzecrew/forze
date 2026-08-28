"""Integration tests for :class:`~forze_temporal.kernel.client.RoutedTemporalClient`."""

from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("temporalio")

from temporalio.worker import Worker

from forze.application.contracts.secrets import SecretRef
from forze_temporal.kernel.client import RoutedTemporalClient, TemporalClient
from forze_temporal.sandbox import sandboxed_workflow_runner
from tests.integration._routed_lru_helpers import temporal_hosts_for_lru_eviction
from tests.support.secrets_fixtures import (
    MemSecretsByPath,
    MemSecretsTenantByPath,
    tenant_holder,
    tenant_secret_ref,
)

from ._workflow_defs import ItSumWorkflow, SumIn, SumOut, it_sum_pair


def _tenant_ref(tid: UUID) -> SecretRef:
    return tenant_secret_ref(tid, "temporal")


def _sum_total(out: SumOut | dict[str, object]) -> int:
    if isinstance(out, SumOut):
        return out.total

    return SumOut.model_validate(out).total

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_sum_workflow_and_result(
    workflow_env_with_host_target,
) -> None:
    env, host_target = workflow_env_with_host_target
    task_queue = f"forze-routed-temporal-{uuid4().hex[:10]}"
    t1 = uuid4()
    secrets = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant={t1: host_target})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[ItSumWorkflow],
        activities=[it_sum_pair],
        workflow_runner=sandboxed_workflow_runner(),
    ):
        try:
            # Time-skipping test server does not implement CountWorkflowExecutions; health() stays False.
            _, ok = await routed.health()
            assert ok is False

            wid = f"wf-sum-{uuid4().hex[:12]}"
            handle = await routed.start_workflow(
                task_queue,
                "ItSumWorkflow",
                SumIn(a=10, b=12),
                workflow_id=wid,
            )

            sync_handle = routed.get_workflow_handle(wid)
            assert sync_handle.id == wid

            result_handle = await handle.result()
            assert _sum_total(result_handle) == 22

            routed_result = await routed.get_workflow_result(wid)
            assert _sum_total(routed_result) == 22
        finally:
            await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_mapping_secret_ref(
    workflow_env_with_host_target,
) -> None:
    env, host_target = workflow_env_with_host_target
    task_queue = f"forze-routed-map-{uuid4().hex[:10]}"
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/temporal/{uuid4().hex[:12]}")
    secrets = MemSecretsByPath({custom.path: host_target})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant={t1: custom},
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[ItSumWorkflow],
        activities=[it_sum_pair],
        workflow_runner=sandboxed_workflow_runner(),
    ):
        try:
            wid = f"wf-map-{uuid4().hex[:12]}"
            await routed.start_workflow(
                task_queue,
                "ItSumWorkflow",
                SumIn(a=1, b=1),
                workflow_id=wid,
            )
            out = await routed.get_workflow_result(wid)
            assert _sum_total(out) == 2
        finally:
            await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_startup_tenant_and_handle_guards(
    workflow_env_with_host_target,
) -> None:
    env, host_target = workflow_env_with_host_target
    t1 = uuid4()
    secrets = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant={t1: host_target})
    tenant_get, tenant_set = tenant_holder()

    unrouted = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    with pytest.raises(CoreException, match="not started"):
        await unrouted.health()

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreException, match="Tenant ID"):
            routed.get_workflow_handle("any")

        tenant_set(t1)
        with pytest.raises(CoreException, match="No Temporal client"):
            routed.get_workflow_handle("any")

        await routed.health()
        _ = routed.get_workflow_handle(f"pending-{uuid4().hex[:8]}")
    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_get_handle_before_started() -> None:
    secrets = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant={uuid4(): "127.0.0.1:1"})
    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    with pytest.raises(CoreException, match="not started"):
        routed.get_workflow_handle("x")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_secret_errors(workflow_env_with_host_target) -> None:
    _, host_target = workflow_env_with_host_target
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = tenant_holder()

    miss = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant={t_ok: host_target}, missing_tenant=t_miss)
    r1 = RoutedTemporalClient(
        secrets=miss,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r1.startup()
    try:
        tenant_set(t_miss)
        with pytest.raises(CoreException):
            await r1.health()
    finally:
        await r1.close()

    br = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant={t_ok: host_target}, broken_tenant=t_break)
    r2 = RoutedTemporalClient(
        secrets=br,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(CoreException, match="Failed to resolve Temporal secret"
        ):
            await r2.health()
    finally:
        await r2.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_temporal_lru_evict(workflow_env_with_host_target) -> None:
    env, host_target = workflow_env_with_host_target
    task_queue = f"forze-routed-temporal-lru-{uuid4().hex[:10]}"
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = MemSecretsTenantByPath(
        resource_suffix="temporal",
        values_by_tenant=temporal_hosts_for_lru_eviction(host_target, t1, t2, t3),
    )
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_tenant_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = TemporalClient.close

    async def counting_close(self: TemporalClient) -> None:
        closes.append(1)
        await real_close(self)

    async def touch_sum(tenant: UUID, tag: str) -> None:
        tenant_set(tenant)
        wid = f"lru-{tag}-{uuid4().hex[:10]}"
        await routed.start_workflow(
            task_queue,
            "ItSumWorkflow",
            SumIn(a=0, b=0),
            workflow_id=wid,
        )
        out = await routed.get_workflow_result(wid)
        assert _sum_total(out) == 0

    try:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ItSumWorkflow],
            activities=[it_sum_pair],
            workflow_runner=sandboxed_workflow_runner(),
        ):
            with patch.object(TemporalClient, "close", counting_close):
                await touch_sum(t1, "a")
                await touch_sum(t2, "b")
                await touch_sum(t1, "c")
                await touch_sum(t3, "d")
                assert sum(closes) == 1

            tenant_set(t1)
            wid_r = f"lru-re-{uuid4().hex[:10]}"
            await routed.start_workflow(
                task_queue,
                "ItSumWorkflow",
                SumIn(a=3, b=4),
                workflow_id=wid_r,
            )
            assert _sum_total(await routed.get_workflow_result(wid_r)) == 7

            await routed.evict_tenant(t1)
            tenant_set(t1)
            wid_e = f"lru-post-evict-{uuid4().hex[:10]}"
            await routed.start_workflow(
                task_queue,
                "ItSumWorkflow",
                SumIn(a=1, b=1),
                workflow_id=wid_e,
            )
            assert _sum_total(await routed.get_workflow_result(wid_e)) == 2
    finally:
        await routed.close()

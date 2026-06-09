"""Integration tests for :class:`~forze_gcs.kernel.client.RoutedGCSClient`."""

from __future__ import annotations

import json
from unittest.mock import patch
from uuid import uuid4

import pytest

pytest.importorskip("gcloud.aio.storage")

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_gcs.kernel.client import GCSClient, RoutedGCSClient

from tests.support.secrets_fixtures import (
    MemSecretsByPath,
    MemSecretsTenantJson,
    tenant_holder,
    tenant_secret_ref,
)

_GCS_PROJECT = "forze-gcs-test"
_GCS_SUFFIX = "gcs"


def _payload(*, project_id: str = _GCS_PROJECT) -> dict[str, str]:
    return {"project_id": project_id}


def _ref(tenant_id) -> SecretRef:
    return tenant_secret_ref(tenant_id, _GCS_SUFFIX)


def _tenant_json(
    payloads: dict,
    *,
    missing_tenant=None,
    broken_tenant=None,
) -> MemSecretsTenantJson:
    return MemSecretsTenantJson(
        resource_suffix=_GCS_SUFFIX,
        payloads_by_tenant=payloads,
        missing_tenant=missing_tenant,
        broken_tenant=broken_tenant,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_object_lifecycle(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        status, ok = await routed.health()
        assert status == "ok" and ok is True

        bucket = f"routed-gcs-{uuid4().hex[:16]}"
        key = "docs/readme.txt"
        data = b"routed gcs bytes"

        await routed.create_bucket(bucket)
        await routed.ensure_bucket(bucket)
        assert await routed.bucket_exists(bucket)

        await routed.upload_bytes(
            bucket,
            key,
            data,
            content_type="text/plain",
            metadata={"filename": "readme.txt"},
        )
        assert await routed.object_exists(bucket, key)
        head = await routed.head_object(bucket, key)
        assert head.content_type == "text/plain"
        assert await routed.download_bytes(bucket, key) == data

        items, total = await routed.list_objects(bucket, prefix="docs", limit=10, offset=0)
        assert total == 1 and len(items) == 1

        await routed.delete_object(bucket, key)
        assert not await routed.object_exists(bucket, key)

        async with routed.client() as storage:
            assert storage is not None
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_mapping_secret_ref(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/gcs/{uuid4().hex[:12]}")
    secrets = MemSecretsByPath({custom.path: json.dumps(_payload())})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant={t1: custom},
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_startup_and_tenant_guards(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    with pytest.raises(CoreException, match="not started"):
        await routed.health()

    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreException, match="Tenant ID"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_secret_errors(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = tenant_holder()

    r1 = RoutedGCSClient(
        secrets=_tenant_json({t_ok: _payload()}, missing_tenant=t_miss),
        secret_ref_for_tenant=_ref,
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

    r2 = RoutedGCSClient(
        secrets=_tenant_json({t_ok: _payload()}, broken_tenant=t_break),
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(CoreException, match="Failed to resolve GCS secret"):
            await r2.health()
    finally:
        await r2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_invalid_json(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t1 = uuid4()
    secrets = MemSecretsByPath({f"tenants/{t1}/{_GCS_SUFFIX}": "{not-json"})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreException, match="GCSRoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_gcs_lru_and_evict(fake_gcs_container: str) -> None:
    _ = fake_gcs_container
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _tenant_json(
        {
            t1: _payload(project_id="gcs-proj-a"),
            t2: _payload(project_id="gcs-proj-b"),
            t3: _payload(project_id="gcs-proj-c"),
        },
    )
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = GCSClient.close

    async def counting_close(self: GCSClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(GCSClient, "close", counting_close):
            tenant_set(t1)
            await routed.health()
            tenant_set(t2)
            await routed.health()
            tenant_set(t1)
            await routed.health()
            tenant_set(t3)
            await routed.health()
            assert sum(closes) == 1

        await routed.evict_tenant(t1)
        tenant_set(t1)
        assert (await routed.health())[1] is True
    finally:
        await routed.close()

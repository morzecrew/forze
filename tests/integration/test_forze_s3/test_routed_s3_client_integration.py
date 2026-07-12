"""Integration tests for :class:`~forze_s3.kernel.client.RoutedS3Client`."""

from __future__ import annotations

import json
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_s3.kernel.client import RoutedS3Client, S3Client, S3Config

from tests.integration._routed_lru_helpers import s3_payloads_for_lru_eviction
from tests.support.secrets_fixtures import (
    MemSecretsByPath,
    MemSecretsTenantJson,
    tenant_holder,
    tenant_secret_ref,
)

_S3_SUFFIX = "s3"


def _payload(backend) -> dict[str, str]:  # noqa: ANN001 - session backend fixture
    return {
        "endpoint": backend.endpoint,
        "access_key_id": backend.access_key,
        "secret_access_key": backend.secret_key,
    }


def _ref(tenant_id: UUID) -> SecretRef:
    return tenant_secret_ref(tenant_id, _S3_SUFFIX)


def _tenant_json(
    payloads: dict[UUID, dict[str, str]],
    *,
    missing_tenant: UUID | None = None,
    broken_tenant: UUID | None = None,
) -> MemSecretsTenantJson:
    return MemSecretsTenantJson(
        resource_suffix=_S3_SUFFIX,
        payloads_by_tenant=payloads,
        missing_tenant=missing_tenant,
        broken_tenant=broken_tenant,
    )

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_health_and_object_crud(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload(s3_backend)})
    tenant_get, tenant_set = tenant_holder()
    cfg = S3Config(s3={"addressing_style": "path"})

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        botocore_config=cfg,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        status, ok = await routed.health()
        assert status == "ok" and ok is True

        bucket = f"forze-routed-s3-{uuid4().hex[:16]}"
        key = "docs/readme.txt"
        data = b"hello routed s3"
        metadata = {"filename": "readme.txt", "size": str(len(data))}

        async with routed.client() as _c:
            _ = _c

        assert not await routed.bucket_exists(bucket)
        await routed.create_bucket(bucket)
        await routed.ensure_bucket(bucket)
        assert await routed.bucket_exists(bucket)

        second = f"alt-{uuid4().hex[:8]}"
        await routed.create_bucket(second)
        await routed.ensure_bucket(second)
        assert await routed.bucket_exists(second)

        await routed.upload_bytes(
            bucket=bucket,
            key=key,
            data=data,
            content_type="text/plain",
            metadata=metadata,
            tags={"kind": "integration"},
        )
        assert await routed.object_exists(bucket, key)
        head = await routed.head_object(bucket, key)
        assert head.content_type == "text/plain"
        assert head.metadata["filename"] == "readme.txt"
        assert (await routed.download_bytes(bucket, key)).data == data

        items, total = await routed.list_objects(
            bucket, prefix="docs", limit=10, offset=0
        )
        assert total == 1 and len(items) == 1
        assert items[0].key == key

        await routed.delete_object(bucket, key)
        assert not await routed.object_exists(bucket, key)
    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_mapping_secret_ref(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/s3/{uuid4().hex[:12]}")
    secrets = MemSecretsByPath({custom.path: json.dumps(_payload(s3_backend))})
    tenant_get, tenant_set = tenant_holder()
    cfg = S3Config(s3={"addressing_style": "path"})

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant={t1: custom},
        tenant_provider=tenant_get,
        botocore_config=cfg,
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
async def test_routed_s3_requires_startup_and_tenant(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload(s3_backend)})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedS3Client(
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
async def test_routed_s3_secret_errors(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = tenant_holder()

    miss = _tenant_json({t_ok: _payload(s3_backend)}, missing_tenant=t_miss)
    r1 = RoutedS3Client(
        secrets=miss,
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

    br = _tenant_json({t_ok: _payload(s3_backend)}, broken_tenant=t_break)
    r2 = RoutedS3Client(
        secrets=br,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(CoreException, match="Failed to resolve S3 secret"):
            await r2.health()
    finally:
        await r2.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_invalid_json_raises_core_error(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t1 = uuid4()
    secrets = MemSecretsByPath({f"tenants/{t1}/{_S3_SUFFIX}": "{not-valid-json"})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreException, match="S3RoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_lru_and_evict(s3_backend) -> None:  # noqa: ANN001 - session backend fixture
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    p = _payload(s3_backend)
    secrets = _tenant_json(
        s3_payloads_for_lru_eviction(s3_backend.endpoint, t1, t2, t3, base_payload=p),
    )
    tenant_get, tenant_set = tenant_holder()
    cfg = S3Config(s3={"addressing_style": "path"})

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        botocore_config=cfg,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = S3Client.close

    async def counting_close(self: S3Client) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(S3Client, "close", counting_close):
            tenant_set(t1)
            await routed.health()
            tenant_set(t2)
            await routed.health()
            tenant_set(t1)
            await routed.health()
            tenant_set(t3)
            await routed.health()
            assert sum(closes) == 1

        tenant_set(t1)
        assert (await routed.health())[1] is True

        await routed.evict_tenant(t1)
        await routed.evict_tenant(uuid4())
        tenant_set(t1)
        assert (await routed.health())[1] is True
    finally:
        await routed.close()

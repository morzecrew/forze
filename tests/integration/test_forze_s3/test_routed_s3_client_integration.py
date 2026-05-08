"""Integration tests for :class:`~forze_s3.kernel.platform.RoutedS3Client`."""

from __future__ import annotations

import json
from collections.abc import Callable
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from forze_s3.kernel.platform import RoutedS3Client, S3Client, S3Config

MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/s3")


def _payload(endpoint: str) -> dict[str, str]:
    return {
        "endpoint": endpoint,
        "access_key_id": MINIO_ROOT_USER,
        "secret_access_key": MINIO_ROOT_PASSWORD,
    }


class _MemSecretsJson:
    """JSON blobs per tenant path or arbitrary path (mapping refs)."""

    def __init__(
        self,
        path_to_json: dict[str, str],
        *,
        missing_path: str | None = None,
        broken_path: str | None = None,
    ) -> None:
        self._paths = path_to_json
        self._missing_path = missing_path
        self._broken_path = broken_path

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_path is not None and ref.path == self._broken_path:
            raise RuntimeError("vault unavailable")
        if self._missing_path is not None and ref.path == self._missing_path:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._paths


class _MemSecretsTenantJson(_MemSecretsJson):
    def __init__(
        self,
        payloads: dict[UUID, dict[str, str]],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {
            f"tenants/{tid}/s3": json.dumps(payload) for tid, payload in payloads.items()
        }
        mp = f"tenants/{missing_tenant}/s3" if missing_tenant else None
        bp = f"tenants/{broken_tenant}/s3" if broken_tenant else None
        super().__init__(paths, missing_path=mp, broken_path=bp)


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_health_and_object_crud(minio_container) -> None:
    _container, endpoint = minio_container
    t1 = uuid4()
    secrets = _MemSecretsTenantJson({t1: _payload(endpoint)})
    tenant_get, tenant_set = _tenant_holder()
    cfg: S3Config = {"s3": {"addressing_style": "path"}}

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
        assert head["content_type"] == "text/plain"
        assert head["metadata"]["filename"] == "readme.txt"
        assert await routed.download_bytes(bucket, key) == data

        items, total = await routed.list_objects(bucket, prefix="docs", limit=10, offset=0)
        assert total == 1 and len(items) == 1
        assert items[0]["Key"] == key

        await routed.delete_object(bucket, key)
        assert not await routed.object_exists(bucket, key)
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_mapping_secret_ref(minio_container) -> None:
    _container, endpoint = minio_container
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/s3/{uuid4().hex[:12]}")
    secrets = _MemSecretsJson({custom.path: json.dumps(_payload(endpoint))})
    tenant_get, tenant_set = _tenant_holder()
    cfg: S3Config = {"s3": {"addressing_style": "path"}}

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
async def test_routed_s3_requires_startup_and_tenant(minio_container) -> None:
    _container, endpoint = minio_container
    t1 = uuid4()
    secrets = _MemSecretsTenantJson({t1: _payload(endpoint)})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()

    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreError, match="Tenant ID"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_secret_errors(minio_container) -> None:
    _container, endpoint = minio_container
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = _tenant_holder()

    miss = _MemSecretsTenantJson({t_ok: _payload(endpoint)}, missing_tenant=t_miss)
    r1 = RoutedS3Client(
        secrets=miss,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r1.startup()
    try:
        tenant_set(t_miss)
        with pytest.raises(SecretNotFoundError):
            await r1.health()
    finally:
        await r1.close()

    br = _MemSecretsTenantJson({t_ok: _payload(endpoint)}, broken_tenant=t_break)
    r2 = RoutedS3Client(
        secrets=br,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(InfrastructureError, match="Failed to resolve S3 secret"):
            await r2.health()
    finally:
        await r2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_invalid_json_raises_core_error(minio_container) -> None:
    _container, endpoint = minio_container
    t1 = uuid4()
    secrets = _MemSecretsJson(
        {f"tenants/{t1}/s3": "{not-valid-json"},
    )
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedS3Client(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreError, match="S3RoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_s3_lru_and_evict(minio_container) -> None:
    _container, endpoint = minio_container
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    p = _payload(endpoint)
    secrets = _MemSecretsTenantJson({t1: p, t2: p, t3: p})
    tenant_get, tenant_set = _tenant_holder()
    cfg: S3Config = {"s3": {"addressing_style": "path"}}

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

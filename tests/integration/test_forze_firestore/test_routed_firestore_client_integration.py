"""Integration tests for :class:`~forze_firestore.kernel.client.RoutedFirestoreClient`."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_firestore.kernel.client import FirestoreClient, RoutedFirestoreClient
from tests.support.secrets_fixtures import (
    MemSecretsByPath,
    MemSecretsTenantJson,
    tenant_holder,
    tenant_secret_ref,
)

_FS_PROJECT = "forze-firestore-test"
_FS_SUFFIX = "firestore"


def _payload(*, project_id: str = _FS_PROJECT, database: str = "(default)") -> dict[str, str]:
    return {"project_id": project_id, "database": database}


def _ref(tenant_id) -> SecretRef:
    return tenant_secret_ref(tenant_id, _FS_SUFFIX)


def _tenant_json(
    payloads: dict,
    *,
    missing_tenant=None,
    broken_tenant=None,
) -> MemSecretsTenantJson:
    return MemSecretsTenantJson(
        resource_suffix=_FS_SUFFIX,
        payloads_by_tenant=payloads,
        missing_tenant=missing_tenant,
        broken_tenant=broken_tenant,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_crud_and_query(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert routed.is_in_transaction() is False

        status, ok = await routed.health()
        assert status == "ok" and ok is True

        coll_name = f"routed_{uuid4().hex[:8]}"
        coll = await routed.collection(coll_name)
        await routed.set_document(coll, "a", {"sku": "keep-a"})
        await routed.set_document(coll, "b", {"sku": "keep-b"}, merge=True)
        await routed.insert_many(
            coll,
            [("c", {"sku": "drop"}), ("d", {"sku": "keep-c"})],
        )

        doc_a = await routed.get_document(coll, "a")
        assert doc_a is not None and doc_a.get("sku") == "keep-a"
        assert await routed.count_documents(coll) == 4

        rows = await routed.query_stream(coll, limit=10)
        assert len(rows) == 4

        await routed.delete_document(coll, "c")
        assert await routed.get_document(coll, "c") is None
        assert await routed.count_documents(coll) == 3
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_transaction(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        coll = await routed.collection(f"tx_{uuid4().hex[:8]}")

        async with routed.transaction():
            routed.require_transaction()
            assert routed.is_in_transaction() is True
            await routed.set_document(coll, "tx1", {"v": 1})

        doc = await routed.get_document(coll, "tx1")
        assert doc is not None and doc.get("v") == 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_mapping_secret_ref(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/fs/{uuid4().hex[:12]}")
    secrets = MemSecretsByPath({custom.path: json.dumps(_payload())})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
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
async def test_routed_firestore_startup_and_tenant_guards(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
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

        with pytest.raises(CoreException, match="Transactional context"):
            routed.require_transaction()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_secret_errors(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = tenant_holder()

    miss = _tenant_json({t_ok: _payload()}, missing_tenant=t_miss)
    r1 = RoutedFirestoreClient(
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

    broken = _tenant_json({t_ok: _payload()}, broken_tenant=t_break)
    r2 = RoutedFirestoreClient(
        secrets=broken,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(CoreException, match="Failed to resolve Firestore secret"):
            await r2.health()
    finally:
        await r2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_invalid_json(firestore_emulator_container) -> None:
    _ = firestore_emulator_container
    t1 = uuid4()
    secrets = MemSecretsByPath({f"tenants/{t1}/{_FS_SUFFIX}": "{bad-json"})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreException, match="FirestoreRoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_firestore_lru_and_evict(firestore_emulator_container) -> None:
    from unittest.mock import patch

    _ = firestore_emulator_container
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _tenant_json(
        {
            t1: _payload(project_id="forze-fs-a"),
            t2: _payload(project_id="forze-fs-b"),
            t3: _payload(project_id="forze-fs-c"),
        },
    )
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedFirestoreClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = FirestoreClient.close

    async def counting_close(self: FirestoreClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(FirestoreClient, "close", counting_close):
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
        await routed.evict_tenant(uuid4())
        tenant_set(t1)
        assert (await routed.health())[1] is True
    finally:
        await routed.close()

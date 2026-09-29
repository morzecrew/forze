"""Unit tests for the object-storage tenant provisioner."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.storage import ObjectStorageTenantProvisioner

# ----------------------- #


class _FakeClient:
    """Like the real clients, refuses an operation outside an open ``client()`` scope."""

    def __init__(self) -> None:
        self.ensured: list[str] = []
        self.scoped = False

    @asynccontextmanager
    async def client(self) -> AsyncIterator[object]:
        self.scoped = True

        try:
            yield object()

        finally:
            self.scoped = False

    async def ensure_bucket(self, bucket: str) -> None:
        if not self.scoped:
            raise RuntimeError("S3 client is not initialized")

        self.ensured.append(bucket)

    def __getattr__(self, _name: str) -> Any:  # pragma: no cover - unused client surface
        raise AttributeError(_name)


@pytest.mark.asyncio
async def test_provision_ensures_per_tenant_bucket() -> None:
    tid = uuid4()
    client = _FakeClient()
    provisioner = ObjectStorageTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        bucket=lambda t: f"tenant-{t}",
    )

    await provisioner.provision(TenantIdentity(tenant_id=tid))

    assert client.ensured == [f"tenant-{tid}"]


@pytest.mark.asyncio
async def test_provision_ensures_static_shared_bucket() -> None:
    client = _FakeClient()
    provisioner = ObjectStorageTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        bucket="shared",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    assert client.ensured == ["shared"]


@pytest.mark.asyncio
async def test_deprovision_is_a_noop() -> None:
    client = _FakeClient()
    provisioner = ObjectStorageTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        bucket="shared",
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert client.ensured == []  # buckets are not auto-deleted

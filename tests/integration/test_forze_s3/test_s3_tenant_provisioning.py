"""Onboarding a tenant creates that tenant's bucket, on that tenant's backend.

Through the tenancy management adapter, as ``provision_tenant`` is called in an app: with a
plain client (which only works inside its scope) and with a tenant-routed client whose tenant
provider answers the admin's own tenant, as an app's holder does while an admin works.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.storage import ObjectStorageTenantProvisioner
from forze_identity.tenancy.execution.deps.deps import ConfigurableTenantManagement
from forze_mock import MockDepsModule, MockState
from forze_s3.kernel.client import RoutedS3Client, S3Config
from tests.support.execution_context import context_from_deps
from tests.support.secrets_fixtures import MemSecretsByPath, tenant_secret_ref

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CONFIG = S3Config(s3={"addressing_style": "path"})


def _bucket(tenant_id: UUID) -> str:
    return f"tenant-{tenant_id}"


async def test_a_plain_client_provisions_the_onboarded_tenant(s3_client: Any) -> None:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    management = ConfigurableTenantManagement(
        provisioner=ObjectStorageTenantProvisioner(client=s3_client, bucket=_bucket),
    )(ctx)

    onboarded = await management.provision_tenant(tenant_key="acme")

    async with s3_client.client():
        assert await s3_client.bucket_exists(_bucket(onboarded.tenant_id))


class _EveryOtherTenant(dict[str, str]):
    """Secrets for any tenant but the admin's, which points at a dead endpoint; records reads."""

    def __init__(self, live: str, seeded: dict[str, str]) -> None:
        super().__init__(seeded)
        self._live = live
        self.read: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.read.append(key)
        return super().__getitem__(key)

    def __missing__(self, key: str) -> str:
        return self._live


async def test_a_routed_client_provisions_on_the_onboarded_tenants_backend(
    s3_backend: Any, s3_client: Any
) -> None:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    home = TenantIdentity(tenant_id=uuid4())
    live = json.dumps(
        {
            "endpoint": s3_backend.endpoint,
            "access_key_id": s3_backend.access_key,
            "secret_access_key": s3_backend.secret_key,
        }
    )
    dead = json.dumps(
        {"endpoint": "http://127.0.0.1:1", "access_key_id": "x", "secret_access_key": "y"}
    )
    paths = _EveryOtherTenant(live, {tenant_secret_ref(home.tenant_id, "s3").path: dead})
    routed = RoutedS3Client(
        secrets=MemSecretsByPath(paths),
        secret_ref_for_tenant=lambda t: tenant_secret_ref(t, "s3"),
        # An app's own holder, answering the admin's tenant throughout.
        tenant_provider=lambda: home.tenant_id,
        botocore_config=_CONFIG,
    )
    await routed.startup()

    try:
        management = ConfigurableTenantManagement(
            provisioner=ObjectStorageTenantProvisioner(client=routed, bucket=_bucket),
        )(ctx)

        # The admin's own tenant has an unreachable backend: the onboarded tenant's bucket
        # must be created on the onboarded tenant's backend, reached with its credentials.
        with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4()), tenant=home):
            onboarded = await management.provision_tenant(tenant_key="acme")

        # Only the onboarded tenant's credentials were read, never the admin's.
        assert set(paths.read) == {tenant_secret_ref(onboarded.tenant_id, "s3").path}

        async with s3_client.client():
            assert await s3_client.bucket_exists(_bucket(onboarded.tenant_id))

    finally:
        await routed.close()

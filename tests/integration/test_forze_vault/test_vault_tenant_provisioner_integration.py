"""Integration test: per-tenant Vault Transit key provisioning (real Vault).

A tenant's Transit key is created on onboarding via the provisioner, then the keyring —
wired through the *same* directory — wraps that tenant's data under it. Proves the
provisioned key and the encrypt-path key never drift, and that opt-in teardown removes it.
"""

from uuid import uuid4

import pytest

pytest.importorskip("hvac")

from forze.application.contracts.crypto import AesGcmAead, TenantTemplateKeyDirectory
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_vault import (
    VaultConfig,
    VaultTransitKeyManagement,
    VaultTransitTenantProvisioner,
)
from forze_vault.kernel.client import VaultClient

# ----------------------- #


@pytest.fixture
async def vault_client(vault_container):
    container, _hvac_client = vault_container

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        transit_mount="transit",
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        yield client
    finally:
        await client.close()


def _directory() -> TenantTemplateKeyDirectory:
    return TenantTemplateKeyDirectory(
        template="tenant-{tenant_id}-cmk", default_key_id="app-cmk"
    )


# ....................... #


@pytest.mark.integration
async def test_provisioned_tenant_key_encrypts_via_keyring(vault_client) -> None:
    directory = _directory()
    provisioner = VaultTransitTenantProvisioner(
        client=vault_client, directory=directory
    )
    tenant = TenantIdentity(tenant_id=uuid4())

    # Onboard the tenant: its Transit key now exists.
    await provisioner.provision(tenant)
    # Idempotent — re-provisioning a tenant must not fail.
    await provisioner.provision(tenant)

    keyring = Keyring(
        kms=VaultTransitKeyManagement(client=vault_client),
        aead=AesGcmAead(),
        directory=directory,
    )

    blob = await keyring.encrypt(b"tenant secret", tenant=tenant, aad=b"ctx")
    assert await keyring.decrypt(blob, aad=b"ctx") == b"tenant secret"


@pytest.mark.integration
async def test_encrypt_before_provision_fails(vault_client) -> None:
    """Without provisioning, the per-tenant key is absent and encryption fails."""

    keyring = Keyring(
        kms=VaultTransitKeyManagement(client=vault_client),
        aead=AesGcmAead(),
        directory=_directory(),
    )

    with pytest.raises(CoreException):
        await keyring.encrypt(b"x", tenant=TenantIdentity(tenant_id=uuid4()))


@pytest.mark.integration
async def test_deprovision_deletes_key_when_allowed(vault_client) -> None:
    directory = _directory()
    tenant = TenantIdentity(tenant_id=uuid4())

    provisioner = VaultTransitTenantProvisioner(
        client=vault_client, directory=directory, allow_deletion=True
    )
    await provisioner.provision(tenant)

    key_ref = await directory.resolve(tenant)
    await vault_client.transit_generate_data_key(key_ref.key_id)  # works while present

    await provisioner.deprovision(tenant)
    # Deprovision is idempotent — deleting an already-absent key is a no-op.
    await provisioner.deprovision(tenant)

    with pytest.raises(CoreException):
        await vault_client.transit_generate_data_key(key_ref.key_id)

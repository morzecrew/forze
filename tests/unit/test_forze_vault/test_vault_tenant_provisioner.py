"""Unit tests for the Vault Transit tenant provisioner + create/delete-key client methods."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("hvac")

from hvac.exceptions import InvalidPath, VaultError

from forze.application.contracts.crypto import TenantTemplateKeyDirectory
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_vault import VaultTransitTenantProvisioner
from forze_vault.kernel.client import VaultClient, VaultClientPort, VaultConfig

# ----------------------- #


def _config() -> VaultConfig:
    return VaultConfig(url="http://127.0.0.1:8200", token="t", transit_mount="transit")


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid4())


# ----------------------- #
# Client (mocked hvac)


async def test_client_create_key_is_idempotent_passthrough() -> None:
    client = VaultClient(config=_config())
    mock = MagicMock()
    client._client = mock

    await client.transit_create_key("tenant-1", key_type="aes256-gcm96")

    mock.secrets.transit.create_key.assert_called_once_with(
        name="tenant-1", key_type="aes256-gcm96", mount_point="transit"
    )


async def test_client_create_key_wraps_vault_errors() -> None:
    client = VaultClient(config=_config())
    mock = MagicMock()
    mock.secrets.transit.create_key.side_effect = VaultError("boom")
    client._client = mock

    with pytest.raises(CoreException):
        await client.transit_create_key("tenant-1", key_type="aes256-gcm96")


async def test_client_delete_key_enables_deletion_first() -> None:
    client = VaultClient(config=_config())
    mock = MagicMock()
    client._client = mock

    await client.transit_delete_key("tenant-1")

    mock.secrets.transit.update_key_configuration.assert_called_once_with(
        name="tenant-1", deletion_allowed=True, mount_point="transit"
    )
    mock.secrets.transit.delete_key.assert_called_once_with(
        name="tenant-1", mount_point="transit"
    )


async def test_client_delete_key_absent_is_noop() -> None:
    client = VaultClient(config=_config())
    mock = MagicMock()
    mock.secrets.transit.read_key.side_effect = InvalidPath()  # absent key
    client._client = mock

    # Already absent → safe to repeat, no raise, no destructive call.
    await client.transit_delete_key("gone")

    mock.secrets.transit.delete_key.assert_not_called()


# ----------------------- #
# Provisioner


def _provisioner(client: object, **kw: object) -> VaultTransitTenantProvisioner:
    return VaultTransitTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        directory=TenantTemplateKeyDirectory(
            template="tenant-{tenant_id}", default_key_id="app"
        ),
        **kw,  # type: ignore[arg-type]
    )


async def test_provision_creates_key_named_by_directory() -> None:
    client = MagicMock(spec=VaultClientPort)
    client.transit_create_key = AsyncMock()
    tenant = _tenant()

    await _provisioner(client).provision(tenant)

    client.transit_create_key.assert_awaited_once_with(
        f"tenant-{tenant.tenant_id}", key_type="aes256-gcm96"
    )


async def test_provision_honours_custom_key_type() -> None:
    client = MagicMock(spec=VaultClientPort)
    client.transit_create_key = AsyncMock()
    tenant = _tenant()

    await _provisioner(client, key_type="ecdsa-p256").provision(tenant)

    client.transit_create_key.assert_awaited_once_with(
        f"tenant-{tenant.tenant_id}", key_type="ecdsa-p256"
    )


async def test_deprovision_is_noop_by_default() -> None:
    client = MagicMock(spec=VaultClientPort)
    client.transit_delete_key = AsyncMock()

    await _provisioner(client).deprovision(_tenant())

    client.transit_delete_key.assert_not_awaited()


async def test_deprovision_deletes_when_allowed() -> None:
    client = MagicMock(spec=VaultClientPort)
    client.transit_delete_key = AsyncMock()
    tenant = _tenant()

    await _provisioner(client, allow_deletion=True).deprovision(tenant)

    client.transit_delete_key.assert_awaited_once_with(f"tenant-{tenant.tenant_id}")

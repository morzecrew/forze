"""Integration tests for :class:`~forze_vault.adapters.VaultKvSecrets`."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, resolve_structured
from forze.base.exceptions import CoreException
from forze_vault.adapters import VaultKvSecrets
from forze_vault.kernel.client import VaultClient, VaultConfig

# ----------------------- #

class _Creds(BaseModel):
    endpoint: str
    access_key_id: str
    secret_access_key: str

@pytest.fixture
async def vault_secrets(vault_container):
    _container, hvac_client = vault_container
    url = _container.get_connection_url()
    token = _container.root_token

    config = VaultConfig(url=url, token=token, mount_point="secret", verify=False)
    client = VaultClient(config=config)
    await client.initialize()

    hvac_client.secrets.kv.v2.create_or_update_secret(
        path="tenants/t1/dsn",
        secret={"value": "postgresql://localhost/db1"},
        mount_point="secret",
    )
    hvac_client.secrets.kv.v2.create_or_update_secret(
        path="tenants/t1/s3",
        secret={
            "endpoint": "http://127.0.0.1:9000",
            "access_key_id": "minio",
            "secret_access_key": "minioadmin",
        },
        mount_point="secret",
    )

    try:
        yield VaultKvSecrets(client=client)

    finally:
        await client.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_vault_resolve_plain_string_secret(vault_secrets: VaultKvSecrets) -> None:
    raw = await vault_secrets.resolve_str(SecretRef(path="tenants/t1/dsn"))
    assert raw == "postgresql://localhost/db1"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_vault_resolve_structured_secret(vault_secrets: VaultKvSecrets) -> None:
    creds = await resolve_structured(
        vault_secrets,
        SecretRef(path="tenants/t1/s3"),
        _Creds,
    )
    assert creds.endpoint == "http://127.0.0.1:9000"
    assert creds.access_key_id == "minio"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_vault_exists(vault_secrets: VaultKvSecrets) -> None:
    assert await vault_secrets.exists(SecretRef(path="tenants/t1/dsn")) is True
    assert await vault_secrets.exists(SecretRef(path="tenants/missing")) is False

@pytest.mark.integration
@pytest.mark.asyncio
async def test_vault_secret_not_found(vault_secrets: VaultKvSecrets) -> None:
    with pytest.raises(CoreException):
        await vault_secrets.resolve_str(SecretRef(path="tenants/does-not-exist"))

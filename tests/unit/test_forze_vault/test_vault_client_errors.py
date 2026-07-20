"""Unit tests for Vault client KV error translation (mocked hvac)."""

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("hvac")

from hvac.exceptions import InvalidPath, VaultError

from forze.application.contracts.secrets import SecretRef
from forze_vault.kernel.client import VaultClient, VaultConfig

# ----------------------- #

@pytest.mark.asyncio
async def test_read_kv_data_not_found() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()
    client._client = mock_hvac

    with pytest.raises(CoreException):
        await client.read_kv_data("missing/path")

@pytest.mark.asyncio
async def test_read_kv_data_vault_error() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_version.side_effect = VaultError("down")
    client._client = mock_hvac

    with pytest.raises(CoreException, match="Vault read failed"):
        await client.read_kv_data("any/path")

@pytest.mark.asyncio
async def test_kv_exists_false_on_invalid_path() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_metadata.side_effect = InvalidPath()
    client._client = mock_hvac

    assert await client.kv_exists("missing") is False
    mock_hvac.secrets.kv.v2.read_secret_version.assert_not_called()

@pytest.mark.asyncio
async def test_kv_exists_uses_metadata_endpoint() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_metadata.return_value = {
        "data": {"versions": {"1": {}}},
    }
    client._client = mock_hvac

    assert await client.kv_exists("present") is True
    mock_hvac.secrets.kv.v2.read_secret_metadata.assert_called_once_with(
        path="present",
        mount_point="secret",
    )
    mock_hvac.secrets.kv.v2.read_secret_version.assert_not_called()

@pytest.mark.asyncio
async def test_kv_exists_maps_vault_error() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_metadata.side_effect = VaultError("down")
    client._client = mock_hvac

    with pytest.raises(CoreException, match="Vault exists check failed"):
        await client.kv_exists("any")

@pytest.mark.asyncio
async def test_kv_exists_maps_unexpected_error() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_metadata.side_effect = RuntimeError("boom")
    client._client = mock_hvac

    with pytest.raises(CoreException, match="Vault exists check failed"):
        await client.kv_exists("any")

@pytest.mark.asyncio
async def test_read_kv_data_unwraps_value_field() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.secrets.kv.v2.read_secret_version.return_value = {
        "data": {"data": {"value": "plain-dsn"}},
    }
    client._client = mock_hvac

    from forze_vault.adapters.secrets import VaultKvSecrets

    sec = VaultKvSecrets(client=client)
    raw = await sec.resolve_str(SecretRef(path="tenants/t1"))
    assert raw == "plain-dsn"

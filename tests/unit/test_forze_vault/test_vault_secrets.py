"""Unit tests for Vault secrets adapter and client error mapping."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("hvac")

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import InfrastructureError
from forze_vault.adapters.secrets import VaultKvSecrets, _encode_kv_payload
from forze_vault.kernel.platform import VaultClient, VaultConfig

# ----------------------- #


def test_encode_kv_payload_single_value() -> None:
    assert _encode_kv_payload({"value": "postgresql://x"}) == "postgresql://x"


def test_encode_kv_payload_object() -> None:
    out = _encode_kv_payload({"endpoint": "http://minio", "access_key_id": "a"})
    assert '"endpoint"' in out
    assert '"access_key_id"' in out


@pytest.mark.asyncio
async def test_vault_kv_secrets_resolve_str() -> None:
    client = MagicMock(spec=VaultClient)
    client.read_kv_data = AsyncMock(return_value={"value": "dsn://x"})
    client.kv_exists = AsyncMock(return_value=True)

    sec = VaultKvSecrets(client=client)
    assert await sec.resolve_str(SecretRef(path="tenants/t1/dsn")) == "dsn://x"
    client.read_kv_data.assert_awaited_once_with("tenants/t1/dsn")


@pytest.mark.asyncio
async def test_vault_kv_secrets_exists() -> None:
    client = MagicMock(spec=VaultClient)
    client.kv_exists = AsyncMock(return_value=False)

    sec = VaultKvSecrets(client=client)
    assert await sec.exists(SecretRef(path="missing")) is False


@pytest.mark.asyncio
async def test_vault_client_not_initialized_raises() -> None:
    client = VaultClient(
        config=VaultConfig(url="http://127.0.0.1:8200", token="t"),
    )
    with pytest.raises(InfrastructureError, match="not initialized"):
        await client.read_kv_data("any")

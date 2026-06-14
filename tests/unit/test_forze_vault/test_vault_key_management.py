"""Unit tests for the Vault Transit key-management adapter and client methods."""

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("hvac")

from hvac.exceptions import InvalidPath, VaultError

from forze.application.contracts.crypto import KeyManagementDepKey, KeyRef
from forze.base.exceptions import CoreException
from forze_vault import VaultTransitKeyManagement
from forze_vault.execution import VaultDepsModule
from forze_vault.kernel.client import VaultClient, VaultClientPort, VaultConfig

# ----------------------- #

_DEK = b"0123456789abcdef0123456789abcdef"  # 32 bytes
_CIPHERTEXT = "vault:v3:c29tZS13cmFwcGVk"


def _config() -> VaultConfig:
    return VaultConfig(url="http://127.0.0.1:8200", token="t", transit_mount="transit")


# ----------------------- #
# Client (mocked hvac)


async def test_client_generate_data_key_decodes_plaintext() -> None:
    client = VaultClient(config=_config())
    mock_hvac = MagicMock()
    mock_hvac.secrets.transit.generate_data_key.return_value = {
        "data": {
            "plaintext": base64.b64encode(_DEK).decode(),
            "ciphertext": _CIPHERTEXT,
        }
    }
    client._client = mock_hvac

    plaintext, ciphertext = await client.transit_generate_data_key("app")

    assert plaintext == _DEK
    assert ciphertext == _CIPHERTEXT
    mock_hvac.secrets.transit.generate_data_key.assert_called_once_with(
        name="app", key_type="plaintext", mount_point="transit"
    )


async def test_client_decrypt_decodes_plaintext() -> None:
    client = VaultClient(config=_config())
    mock_hvac = MagicMock()
    mock_hvac.secrets.transit.decrypt_data.return_value = {
        "data": {"plaintext": base64.b64encode(_DEK).decode()}
    }
    client._client = mock_hvac

    assert await client.transit_decrypt("app", _CIPHERTEXT) == _DEK
    mock_hvac.secrets.transit.decrypt_data.assert_called_once_with(
        name="app", ciphertext=_CIPHERTEXT, mount_point="transit"
    )


async def test_client_generate_missing_key_is_not_found() -> None:
    client = VaultClient(config=_config())
    mock_hvac = MagicMock()
    mock_hvac.secrets.transit.generate_data_key.side_effect = InvalidPath()
    client._client = mock_hvac

    with pytest.raises(CoreException, match="No Transit key"):
        await client.transit_generate_data_key("missing")


async def test_client_decrypt_vault_error_is_infrastructure() -> None:
    client = VaultClient(config=_config())
    mock_hvac = MagicMock()
    mock_hvac.secrets.transit.decrypt_data.side_effect = VaultError("boom")
    client._client = mock_hvac

    with pytest.raises(CoreException, match="transit decrypt failed"):
        await client.transit_decrypt("app", _CIPHERTEXT)


# ----------------------- #
# Adapter (mocked client)


async def test_adapter_generate_data_key_builds_datakey() -> None:
    client = MagicMock(spec=VaultClient)
    client.transit_generate_data_key = AsyncMock(return_value=(_DEK, _CIPHERTEXT))

    kms = VaultTransitKeyManagement(client=client)
    data_key = await kms.generate_data_key(KeyRef(key_id="app"))

    assert data_key.plaintext == _DEK
    assert data_key.wrapped == _CIPHERTEXT.encode("ascii")
    assert data_key.key_id == "app"
    assert data_key.key_version == "v3"  # parsed from the vault:vN: token
    client.transit_generate_data_key.assert_awaited_once_with("app")


async def test_adapter_unwrap_round_trips_through_client() -> None:
    client = MagicMock(spec=VaultClient)
    client.transit_decrypt = AsyncMock(return_value=_DEK)

    kms = VaultTransitKeyManagement(client=client)
    plaintext = await kms.unwrap_data_key(
        wrapped=_CIPHERTEXT.encode("ascii"),
        key_ref=KeyRef(key_id="app"),
    )

    assert plaintext == _DEK
    client.transit_decrypt.assert_awaited_once_with("app", _CIPHERTEXT)


# ----------------------- #
# Deps module wiring


def test_deps_module_registers_key_management_when_set() -> None:
    client = MagicMock(spec=VaultClientPort)
    kms = VaultTransitKeyManagement(client=client)

    deps = VaultDepsModule(client=client, key_management=kms)()

    assert deps.exists(KeyManagementDepKey)


def test_deps_module_omits_key_management_by_default() -> None:
    client = MagicMock(spec=VaultClientPort)

    deps = VaultDepsModule(client=client)()

    assert not deps.exists(KeyManagementDepKey)

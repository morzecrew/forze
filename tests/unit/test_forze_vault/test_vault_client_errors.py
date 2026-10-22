"""Unit tests for Vault client KV error translation (mocked hvac)."""

from collections.abc import Awaitable, Callable
from typing import Any
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


# ....................... #

_LEAK = "value alice@example.test"

# client method: (hvac call that fails, a call reaching it)
_FAILING_CALLS: dict[str, tuple[str, Callable[[VaultClient], Awaitable[Any]]]] = {
    "read_kv_data": ("secrets.kv.v2.read_secret_version", lambda c: c.read_kv_data("p")),
    "read_kv_data_versioned": (
        "secrets.kv.v2.read_secret_version",
        lambda c: c.read_kv_data_versioned("p"),
    ),
    "read_kv_metadata": ("secrets.kv.v2.read_secret_metadata", lambda c: c.read_kv_metadata("p")),
    "write_kv_data": (
        "secrets.kv.v2.create_or_update_secret",
        lambda c: c.write_kv_data("p", {"v": 1}),
    ),
    "kv_exists": ("secrets.kv.v2.read_secret_metadata", lambda c: c.kv_exists("p")),
    "transit_generate_data_key": (
        "secrets.transit.generate_data_key",
        lambda c: c.transit_generate_data_key("k"),
    ),
    "transit_decrypt": (
        "secrets.transit.decrypt_data",
        lambda c: c.transit_decrypt("k", "vault:v1:x"),
    ),
    "transit_sign": ("secrets.transit.sign_data", lambda c: c.transit_sign("k", b"data")),
    "transit_public_key": ("secrets.transit.read_key", lambda c: c.transit_public_key("k")),
    "transit_rewrap": (
        "secrets.transit.rewrap_data",
        lambda c: c.transit_rewrap("k", "vault:v1:x"),
    ),
    "transit_create_key": (
        "secrets.transit.create_key",
        lambda c: c.transit_create_key("k", key_type="rsa-2048"),
    ),
    "transit_delete_key": ("secrets.transit.delete_key", lambda c: c.transit_delete_key("k")),
    "db_generate_credentials": (
        "secrets.database.generate_credentials",
        lambda c: c.db_generate_credentials("r"),
    ),
    "renew_lease": ("sys.renew_lease", lambda c: c.renew_lease("lease/1", 60)),
    "revoke_lease": ("sys.revoke_lease", lambda c: c.revoke_lease("lease/1")),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [VaultError(_LEAK), RuntimeError(_LEAK)], ids=["vault", "other"])
@pytest.mark.parametrize(("attr", "call"), _FAILING_CALLS.values(), ids=list(_FAILING_CALLS))
async def test_the_backend_text_stays_out_of_the_summary(
    attr: str,
    call: Callable[[VaultClient], Awaitable[Any]],
    error: Exception,
) -> None:
    # The summary is what ``str()`` renders and what run stores record; Vault's text can
    # echo request values, so it rides in details only.
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    failing = mock_hvac

    for part in attr.split("."):
        failing = getattr(failing, part)

    failing.side_effect = error
    client._client = mock_hvac

    with pytest.raises(CoreException) as ei:
        await call(client)

    assert _LEAK not in str(ei.value)
    assert ei.value.details == {"error": str(error)}

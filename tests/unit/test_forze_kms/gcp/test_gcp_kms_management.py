"""Unit tests for the GCP KMS key-management adapter and deps wiring (mocked client)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("google.cloud.kms")

from forze.application.contracts.crypto import KeyManagementDepKey, KeyRef
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kms.gcp import GcpKmsClient, GcpKmsClientPort, GcpKmsKeyManagement
from forze_kms.gcp.execution import GcpKmsDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #

_KEY = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
_WRAPPED = b"\x0a\x60ciphertext-blob-from-kms"


# ----------------------- #
# Adapter (mocked client)


async def test_generate_data_key_wraps_a_client_side_dek() -> None:
    client = MagicMock(spec=GcpKmsClient)
    client.encrypt = AsyncMock(return_value=_WRAPPED)

    kms = GcpKmsKeyManagement(client=client)
    data_key = await kms.generate_data_key(KeyRef(key_id=_KEY))

    assert len(data_key.plaintext) == 32  # DEK minted client-side (AES-256)
    assert data_key.wrapped == _WRAPPED
    assert data_key.key_id == _KEY
    assert data_key.key_version is None  # KMS rotation is transparent
    # The exact bytes minted client-side are what got wrapped under the CryptoKey.
    client.encrypt.assert_awaited_once_with(_KEY, data_key.plaintext)


async def test_generate_data_key_honors_dek_length() -> None:
    client = MagicMock(spec=GcpKmsClient)
    client.encrypt = AsyncMock(return_value=_WRAPPED)

    kms = GcpKmsKeyManagement(client=client, dek_bytes=16)
    data_key = await kms.generate_data_key(KeyRef(key_id=_KEY))

    assert len(data_key.plaintext) == 16


async def test_unwrap_round_trips_through_client() -> None:
    client = MagicMock(spec=GcpKmsClient)
    client.decrypt = AsyncMock(return_value=b"the-dek")

    kms = GcpKmsKeyManagement(client=client)
    plaintext = await kms.unwrap_data_key(wrapped=_WRAPPED, key_ref=KeyRef(key_id=_KEY))

    assert plaintext == b"the-dek"
    client.decrypt.assert_awaited_once_with(_KEY, _WRAPPED)


def test_unsupported_dek_length_fails_closed() -> None:
    client = MagicMock(spec=GcpKmsClient)

    with pytest.raises(CoreException) as ei:
        GcpKmsKeyManagement(client=client, dek_bytes=24)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.crypto.dek_length_unsupported"


# ----------------------- #
# Deps module wiring


def test_deps_module_registers_key_management() -> None:
    client = MagicMock(spec=GcpKmsClientPort)

    deps = GcpKmsDepsModule(client=client)()

    assert deps.exists(KeyManagementDepKey)


def test_deps_module_uses_supplied_key_management() -> None:
    client = MagicMock(spec=GcpKmsClientPort)
    custom = GcpKmsKeyManagement(client=client, dek_bytes=16)

    ctx = context_from_modules(GcpKmsDepsModule(client=client, key_management=custom))

    assert ctx.deps.provide(KeyManagementDepKey) is custom

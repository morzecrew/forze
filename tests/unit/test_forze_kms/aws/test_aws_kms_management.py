"""Unit tests for the AWS KMS key-management adapter and deps wiring (mocked client)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("aioboto3")

from forze.application.contracts.crypto import KeyManagementDepKey, KeyRef
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kms.aws import AwsKmsClient, AwsKmsClientPort, AwsKmsKeyManagement
from forze_kms.aws.execution import AwsKmsDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #

_DEK = b"0123456789abcdef0123456789abcdef"  # 32 bytes
_BLOB = b"\x01\x02wrapped-ciphertext-blob"


# ----------------------- #
# Adapter (mocked client)


async def test_generate_data_key_builds_datakey() -> None:
    client = MagicMock(spec=AwsKmsClient)
    client.generate_data_key = AsyncMock(return_value=(_DEK, _BLOB))

    kms = AwsKmsKeyManagement(client=client)
    data_key = await kms.generate_data_key(KeyRef(key_id="alias/app-cmk"))

    assert data_key.plaintext == _DEK
    assert data_key.wrapped == _BLOB
    assert data_key.key_id == "alias/app-cmk"
    assert data_key.key_version is None  # KMS rotation is transparent
    client.generate_data_key.assert_awaited_once_with(
        "alias/app-cmk", key_spec="AES_256"
    )


async def test_generate_data_key_honors_dek_length() -> None:
    client = MagicMock(spec=AwsKmsClient)
    client.generate_data_key = AsyncMock(return_value=(_DEK[:16], _BLOB))

    kms = AwsKmsKeyManagement(client=client, dek_bytes=16)
    await kms.generate_data_key(KeyRef(key_id="cmk"))

    client.generate_data_key.assert_awaited_once_with("cmk", key_spec="AES_128")


async def test_unwrap_passes_key_id_for_confused_deputy_guard() -> None:
    client = MagicMock(spec=AwsKmsClient)
    client.decrypt = AsyncMock(return_value=_DEK)

    kms = AwsKmsKeyManagement(client=client)
    plaintext = await kms.unwrap_data_key(wrapped=_BLOB, key_ref=KeyRef(key_id="cmk"))

    assert plaintext == _DEK
    client.decrypt.assert_awaited_once_with(_BLOB, key_id="cmk")


def test_unsupported_dek_length_fails_closed() -> None:
    client = MagicMock(spec=AwsKmsClient)

    with pytest.raises(CoreException) as ei:
        AwsKmsKeyManagement(client=client, dek_bytes=24)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.crypto.dek_length_unsupported"


# ----------------------- #
# Deps module wiring


def test_deps_module_registers_key_management() -> None:
    client = MagicMock(spec=AwsKmsClientPort)

    deps = AwsKmsDepsModule(client=client)()

    assert deps.exists(KeyManagementDepKey)


def test_deps_module_uses_supplied_key_management() -> None:
    client = MagicMock(spec=AwsKmsClientPort)
    custom = AwsKmsKeyManagement(client=client, dek_bytes=16)

    ctx = context_from_modules(AwsKmsDepsModule(client=client, key_management=custom))

    assert ctx.deps.provide(KeyManagementDepKey) is custom

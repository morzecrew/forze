"""Unit tests for the Yandex Cloud KMS client, adapter, and deps wiring (mocked stub).

Yandex Cloud publishes no KMS emulator, so — unlike AWS (floci) and GCP
(fake-cloud-kms) — these mocked-stub tests are the primary coverage: they pin the
gRPC request/response mapping and the error translation that an integration test
would otherwise exercise. A real-credentials integration test lives under
``tests/integration/test_forze_kms/yc`` (skipped unless credentials are supplied).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("yandexcloud")

import grpc
from yandex.cloud.kms.v1.symmetric_key_pb2 import SymmetricAlgorithm

from forze.application.contracts.crypto import KeyManagementDepKey, KeyRef
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kms.yc import YcKmsClient, YcKmsClientPort, YcKmsKeyManagement
from forze_kms.yc.execution import YcKmsDepsModule
from forze_kms.yc.kernel.client import YcGeneratedDataKey
from tests.support.execution_context import context_from_modules

# ----------------------- #

_KEY = "abjq7s0000000000key"
_DEK = b"0123456789abcdef0123456789abcdef"  # 32 bytes
_WRAPPED = b"\x01\x02wrapped-data-key"


class _GenerateResponse:
    def __init__(
        self, plaintext: bytes, ciphertext: bytes, version_id: str = "1"
    ) -> None:
        self.data_key_plaintext = plaintext
        self.data_key_ciphertext = ciphertext
        self.version_id = version_id


class _DecryptResponse:
    def __init__(self, plaintext: bytes) -> None:
        self.plaintext = plaintext


class _RpcError(grpc.RpcError):
    """A gRPC error carrying a status code, like a real sync-channel failure."""

    def __init__(self, status: grpc.StatusCode) -> None:
        self._status = status

    def code(self) -> grpc.StatusCode:
        return self._status


def _client_with_stub(stub: Any) -> YcKmsClient:
    """Inject a fake SymmetricCrypto stub into an (un-initialized) client."""

    client = YcKmsClient()
    # The stub is a private (name-mangled) field set by ``initialize``.
    client._YcKmsClient__stub = stub  # type: ignore[attr-defined]

    return client


# ----------------------- #
# Client (mocked gRPC stub)


async def test_generate_data_key_builds_request_and_unpacks_response() -> None:
    stub = MagicMock()
    stub.GenerateDataKey.return_value = _GenerateResponse(_DEK, _WRAPPED)

    client = _client_with_stub(stub)
    generated = await client.generate_data_key(_KEY)

    assert generated.plaintext == _DEK
    assert generated.ciphertext == _WRAPPED
    # Yandex Cloud reports the wrapping version (AWS's blob does not) — surfaced.
    assert generated.version_id == "1"

    request = stub.GenerateDataKey.call_args[0][0]
    assert request.key_id == _KEY
    assert request.data_key_spec == SymmetricAlgorithm.Value("AES_256")


async def test_generate_data_key_honors_the_algorithm() -> None:
    stub = MagicMock()
    stub.GenerateDataKey.return_value = _GenerateResponse(_DEK[:16], _WRAPPED)

    client = _client_with_stub(stub)
    await client.generate_data_key(_KEY, algorithm="AES_128")

    request = stub.GenerateDataKey.call_args[0][0]
    assert request.data_key_spec == SymmetricAlgorithm.Value("AES_128")


async def test_decrypt_builds_request_and_unpacks_response() -> None:
    stub = MagicMock()
    stub.Decrypt.return_value = _DecryptResponse(_DEK)

    client = _client_with_stub(stub)

    assert await client.decrypt(_KEY, _WRAPPED) == _DEK

    request = stub.Decrypt.call_args[0][0]
    assert request.key_id == _KEY
    assert request.ciphertext == _WRAPPED


async def test_invalid_ciphertext_is_a_validation_error() -> None:
    """A corrupt/foreign wrapped key (INVALID_ARGUMENT) is caller-caused, not a 500."""

    stub = MagicMock()
    stub.Decrypt.side_effect = _RpcError(grpc.StatusCode.INVALID_ARGUMENT)

    client = _client_with_stub(stub)

    with pytest.raises(CoreException) as ei:
        await client.decrypt(_KEY, b"garbage")

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert ei.value.code == "core.crypto.wrapped_key_invalid"


async def test_unavailable_is_an_infrastructure_error() -> None:
    stub = MagicMock()
    stub.Decrypt.side_effect = _RpcError(grpc.StatusCode.UNAVAILABLE)

    client = _client_with_stub(stub)

    with pytest.raises(CoreException) as ei:
        await client.decrypt(_KEY, _WRAPPED)

    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE


async def test_uninitialized_client_fails_closed() -> None:
    with pytest.raises(CoreException):
        await YcKmsClient().generate_data_key(_KEY)


# ----------------------- #
# Adapter (mocked client)


async def test_generate_data_key_builds_datakey() -> None:
    client = MagicMock(spec=YcKmsClient)
    client.generate_data_key = AsyncMock(
        return_value=YcGeneratedDataKey(
            plaintext=_DEK, ciphertext=_WRAPPED, version_id="3"
        )
    )

    kms = YcKmsKeyManagement(client=client)
    data_key = await kms.generate_data_key(KeyRef(key_id=_KEY))

    assert data_key.plaintext == _DEK
    assert data_key.wrapped == _WRAPPED
    assert data_key.key_id == _KEY
    # The wrapping version rides along for observability; decrypt never needs it, so
    # rotation stays transparent.
    assert data_key.key_version == "3"
    client.generate_data_key.assert_awaited_once_with(_KEY, algorithm="AES_256")


async def test_generate_data_key_honors_dek_length() -> None:
    client = MagicMock(spec=YcKmsClient)
    client.generate_data_key = AsyncMock(
        return_value=YcGeneratedDataKey(plaintext=_DEK[:16], ciphertext=_WRAPPED)
    )

    kms = YcKmsKeyManagement(client=client, dek_bytes=16)
    await kms.generate_data_key(KeyRef(key_id=_KEY))

    client.generate_data_key.assert_awaited_once_with(_KEY, algorithm="AES_128")


async def test_unwrap_round_trips_through_client() -> None:
    client = MagicMock(spec=YcKmsClient)
    client.decrypt = AsyncMock(return_value=_DEK)

    kms = YcKmsKeyManagement(client=client)

    assert await kms.unwrap_data_key(wrapped=_WRAPPED, key_ref=KeyRef(key_id=_KEY)) == _DEK
    client.decrypt.assert_awaited_once_with(_KEY, _WRAPPED)


async def test_unwrap_ignores_the_envelope_key_version() -> None:
    """The version rides along for observability only — Decrypt reads it from the
    ciphertext, so an envelope's recorded version must not steer the unwrap."""

    client = MagicMock(spec=YcKmsClient)
    client.decrypt = AsyncMock(return_value=_DEK)

    kms = YcKmsKeyManagement(client=client)
    await kms.unwrap_data_key(
        wrapped=_WRAPPED, key_ref=KeyRef(key_id=_KEY, version="7")
    )

    client.decrypt.assert_awaited_once_with(_KEY, _WRAPPED)  # version not passed


def test_unsupported_dek_length_fails_closed() -> None:
    client = MagicMock(spec=YcKmsClient)

    with pytest.raises(CoreException) as ei:
        YcKmsKeyManagement(client=client, dek_bytes=24)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.crypto.dek_length_unsupported"


# ----------------------- #
# Deps module wiring


def test_deps_module_registers_only_the_client_by_default() -> None:
    """``CryptoDepsModule`` supplies the port; registering it here too would conflict."""

    client = MagicMock(spec=YcKmsClientPort)

    assert not YcKmsDepsModule(client=client)().exists(KeyManagementDepKey)


def test_deps_module_registers_a_supplied_key_management() -> None:
    client = MagicMock(spec=YcKmsClientPort)
    custom = YcKmsKeyManagement(client=client, dek_bytes=16)

    ctx = context_from_modules(YcKmsDepsModule(client=client, key_management=custom))

    assert ctx.deps.provide(KeyManagementDepKey) is custom

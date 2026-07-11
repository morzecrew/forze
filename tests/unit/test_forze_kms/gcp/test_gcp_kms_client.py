"""The GCP KMS kernel client (mocked google-cloud-kms).

The emulator suite covers the crypto happy paths; these pin what it cannot: the real-GCP
(secure transport) branch, the un-initialized guards, the partial-initialization cleanup,
and version destruction — which the emulator does not implement.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("google.cloud.kms")

from google.api_core.exceptions import AlreadyExists
from google.cloud.kms_v1.types import CryptoKeyVersion

from forze.base.exceptions import CoreException
from forze_kms.gcp import GcpKmsClient, GcpKmsConfig

# ----------------------- #

_KEY = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
_RING = "projects/p/locations/global/keyRings/r"


def _gcp() -> MagicMock:
    inner = MagicMock()
    inner.encrypt = AsyncMock(return_value=SimpleNamespace(ciphertext=b"ct"))
    inner.decrypt = AsyncMock(return_value=SimpleNamespace(plaintext=b"pt"))
    inner.create_crypto_key = AsyncMock(return_value=SimpleNamespace(name=_KEY))

    return inner


def _client_with(inner: MagicMock, *, timeout: float | None = None) -> GcpKmsClient:
    client = GcpKmsClient()
    client._GcpKmsClient__client = inner  # type: ignore[attr-defined]  # noqa: SLF001
    client._GcpKmsClient__request_timeout = timeout  # type: ignore[attr-defined]  # noqa: SLF001

    return client


def _version(state: Any, name: str) -> SimpleNamespace:
    return SimpleNamespace(state=state, name=name)


async def _aiter(items: list[Any]) -> Any:
    """The async-pageable list_crypto_key_versions returns."""

    class _Pager:
        def __aiter__(self) -> Any:
            async def _gen() -> Any:
                for item in items:
                    yield item

            return _gen()

    return _Pager()


# ....................... #


class TestConfig:
    def test_a_non_positive_timeout_fails_closed(self) -> None:
        with pytest.raises(CoreException):
            GcpKmsConfig(request_timeout=0)


# ....................... #


class TestLifecycle:
    async def test_operations_before_initialize_fail_closed(self) -> None:
        with pytest.raises(CoreException):
            await GcpKmsClient().encrypt(_KEY, b"x")

    async def test_close_before_initialize_is_a_no_op(self) -> None:
        await GcpKmsClient().close()

    async def test_health_reports_ok_once_initialized(self) -> None:
        assert await _client_with(_gcp()).health() == ("ok", True)

    async def test_health_reports_a_failure_without_raising(self) -> None:
        message, ok = await GcpKmsClient().health()

        assert ok is False
        assert message

    async def test_real_gcp_uses_the_default_secure_transport(self) -> None:
        """No endpoint → application-default credentials, no insecure channel."""

        client = GcpKmsClient()
        built: dict[str, Any] = {}

        def _factory(**kwargs: Any) -> MagicMock:
            built.update(kwargs)

            return _gcp()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.KeyManagementServiceAsyncClient",
                _factory,
            )
            await client.initialize(config=GcpKmsConfig(request_timeout=3))

        assert built == {"credentials": None}

        async with client.client() as c:
            assert c is not None

    async def test_a_failed_client_build_closes_the_emulator_channel(self) -> None:
        """Nothing is published until every piece is built, so a retry cannot orphan it."""

        channel = MagicMock()
        channel.close = AsyncMock()
        client = GcpKmsClient()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.grpc.aio.insecure_channel",
                lambda *_a, **_kw: channel,
            )
            # Stub the transport too: whether google's real one accepts a mock channel is
            # its business, not what this test is about — and letting it decide makes the
            # failure that reaches `initialize` depend on the installed grpc.
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.KeyManagementServiceGrpcAsyncIOTransport",
                MagicMock(),
            )
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.KeyManagementServiceAsyncClient",
                MagicMock(side_effect=RuntimeError("boom")),
            )

            with pytest.raises(RuntimeError, match="boom"):
                await client.initialize(endpoint="localhost:1")

        channel.close.assert_awaited_once()

        # ...and no half-built state was left behind.
        with pytest.raises(CoreException):
            await client.encrypt(_KEY, b"x")

    async def test_close_tears_down_an_owned_emulator_channel(self) -> None:
        channel = MagicMock()
        channel.close = AsyncMock()
        client = GcpKmsClient()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.grpc.aio.insecure_channel",
                lambda *_a, **_kw: channel,
            )
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.KeyManagementServiceGrpcAsyncIOTransport",
                MagicMock(),
            )
            mp.setattr(
                "forze_kms.gcp.kernel.client.client.KeyManagementServiceAsyncClient",
                MagicMock(return_value=_gcp()),
            )
            await client.initialize(endpoint="localhost:1")

        await client.close()

        channel.close.assert_awaited_once()


# ....................... #


class TestCryptoOperations:
    async def test_encrypt_wraps_under_the_named_key(self) -> None:
        inner = _gcp()

        assert await _client_with(inner).encrypt(_KEY, b"dek") == b"ct"
        inner.encrypt.assert_awaited_once_with(name=_KEY, plaintext=b"dek")

    async def test_decrypt_unwraps_under_the_named_key(self) -> None:
        inner = _gcp()

        assert await _client_with(inner).decrypt(_KEY, b"ct") == b"pt"
        inner.decrypt.assert_awaited_once_with(name=_KEY, ciphertext=b"ct")

    async def test_the_configured_timeout_reaches_every_call(self) -> None:
        inner = _gcp()

        await _client_with(inner, timeout=4.0).encrypt(_KEY, b"dek")

        assert inner.encrypt.await_args.kwargs["timeout"] == 4.0


# ....................... #


class TestKeyAdministration:
    async def test_ensure_crypto_key_creates_it(self) -> None:
        inner = _gcp()

        name = await _client_with(inner).ensure_crypto_key(_RING, "k")

        assert name == _KEY
        assert inner.create_crypto_key.await_args.kwargs["crypto_key_id"] == "k"

    async def test_an_existing_key_is_a_no_op(self) -> None:
        """Idempotent provisioning: AlreadyExists resolves to the key's own name."""

        inner = _gcp()
        inner.create_crypto_key = AsyncMock(side_effect=AlreadyExists("exists"))

        assert await _client_with(inner).ensure_crypto_key(_RING, "k") == _KEY

    async def test_destroy_versions_skips_already_destroyed_ones(self) -> None:
        """GCP cannot delete a CryptoKey, so teardown destroys the versions holding
        material — and must not re-destroy the ones already gone."""

        enabled = _version(CryptoKeyVersion.CryptoKeyVersionState.ENABLED, f"{_KEY}/1")
        disabled = _version(CryptoKeyVersion.CryptoKeyVersionState.DISABLED, f"{_KEY}/2")
        destroyed = _version(
            CryptoKeyVersion.CryptoKeyVersionState.DESTROYED, f"{_KEY}/3"
        )

        inner = _gcp()
        inner.list_crypto_key_versions = AsyncMock(
            return_value=await _aiter([enabled, disabled, destroyed])
        )
        inner.destroy_crypto_key_version = AsyncMock()

        count = await _client_with(inner).destroy_crypto_key_versions(_KEY)

        assert count == 2
        destroyed_names = [
            call.kwargs["name"]
            for call in inner.destroy_crypto_key_version.await_args_list
        ]
        assert destroyed_names == [f"{_KEY}/1", f"{_KEY}/2"]

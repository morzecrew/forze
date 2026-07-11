from __future__ import annotations

from forze_kms.yc._compat import require_kms_yc

require_kms_yc()

# ....................... #

import asyncio
from typing import Any, Mapping, final

import attrs
import yandexcloud
from yandex.cloud.kms.v1.symmetric_crypto_service_pb2 import (
    GenerateDataKeyRequest,
    SymmetricDecryptRequest,
)
from yandex.cloud.kms.v1.symmetric_crypto_service_pb2_grpc import (
    SymmetricCryptoServiceStub,
)
from yandex.cloud.kms.v1.symmetric_key_pb2 import SymmetricAlgorithm

from forze.base.exceptions import exc

from .._logger import logger
from .errors import exc_interceptor
from .port import YcKmsClientPort
from .value_objects import YcKmsConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class YcKmsClient(YcKmsClientPort):
    """Async Yandex Cloud KMS client over the (synchronous) ``yandexcloud`` SDK.

    The Yandex Cloud SDK is blocking gRPC, so :meth:`initialize` builds the stub
    and every call runs in a worker thread (``asyncio.to_thread``) — the same
    shape the Vault client uses for ``hvac``. The stub is built once and reused;
    gRPC's sync channel is thread-safe for concurrent calls.
    """

    __stub: Any | None = attrs.field(default=None, init=False)
    __request_timeout: float | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        *,
        iam_token: str | None = None,
        oauth_token: str | None = None,
        service_account_key: Mapping[str, str] | None = None,
        config: YcKmsConfig | None = None,
    ) -> None:
        """Build the long-lived Yandex Cloud KMS stub.

        No-ops if already initialized. Exactly one credential form is normally
        given: a short-lived *iam_token*, a long-lived *oauth_token*, or a
        *service_account_key* (the authorized-key JSON, whose IAM tokens the SDK
        refreshes). With none of them the SDK falls back to the instance metadata
        service (a Yandex Cloud VM / serverless runtime).
        """

        async with self.__init_lock:
            if self.__stub is not None:
                return

            cfg = config if config is not None else YcKmsConfig()
            self.__request_timeout = cfg.request_timeout

            def _build() -> Any:
                kwargs: dict[str, Any] = {}

                if iam_token is not None:
                    kwargs["iam_token"] = iam_token

                if oauth_token is not None:
                    kwargs["token"] = oauth_token

                if service_account_key is not None:
                    kwargs["service_account_key"] = dict(service_account_key)

                if cfg.endpoint is not None:
                    kwargs["endpoint"] = cfg.endpoint

                # The SDK is re-exported implicitly (it is not in ``__all__``).
                sdk = yandexcloud.SDK(  # pyright: ignore[reportPrivateImportUsage]
                    **kwargs
                )

                return sdk.client(SymmetricCryptoServiceStub)

            self.__stub = await asyncio.to_thread(_build)

            logger.trace("YC KMS client connected", endpoint=cfg.endpoint)

    # ....................... #

    async def close(self) -> None:
        """Release the stub.

        The ``yandexcloud`` SDK exposes no channel-close, so the reference is
        dropped and the gRPC channel is reclaimed with it.
        """

        async with self.__init_lock:
            self.__stub = None
            self.__request_timeout = None

            logger.trace("YC KMS client closed")

    # ....................... #

    def __require_stub(self) -> Any:
        if self.__stub is None:
            raise exc.internal("YC KMS client is not initialized")

        return self.__stub

    # ....................... #

    def __timeout_kwargs(self) -> dict[str, Any]:
        if self.__request_timeout is None:
            return {}

        return {"timeout": self.__request_timeout}

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Report whether the client is initialized (no cheap key-less KMS ping)."""

        try:
            self.__require_stub()
            return "ok", True

        except Exception as e:
            logger.debug("YC KMS health check failed", exc_info=True)
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("yckms.generate_data_key")
    async def generate_data_key(
        self,
        key_id: str,
        *,
        algorithm: str = "AES_256",
    ) -> tuple[bytes, bytes]:
        """Generate a data key under *key_id* via ``SymmetricCrypto.GenerateDataKey``.

        :returns: ``(plaintext, ciphertext)`` — the raw data key and the wrapped blob
            (self-describing: it names its own key version).
        """

        stub = self.__require_stub()
        request = GenerateDataKeyRequest(
            key_id=key_id,
            data_key_spec=SymmetricAlgorithm.Value(algorithm),
        )

        response = await asyncio.to_thread(
            stub.GenerateDataKey, request, **self.__timeout_kwargs()
        )

        plaintext: bytes = response.data_key_plaintext
        ciphertext: bytes = response.data_key_ciphertext

        if not plaintext or not ciphertext:
            raise exc.internal("YC KMS GenerateDataKey returned no key material")

        return plaintext, ciphertext

    # ....................... #

    @exc_interceptor.coroutine("yckms.decrypt")
    async def decrypt(self, key_id: str, ciphertext: bytes) -> bytes:
        """Decrypt a wrapped data key via ``SymmetricCrypto.Decrypt``."""

        stub = self.__require_stub()
        request = SymmetricDecryptRequest(key_id=key_id, ciphertext=ciphertext)

        response = await asyncio.to_thread(
            stub.Decrypt, request, **self.__timeout_kwargs()
        )

        plaintext: bytes = response.plaintext

        if not plaintext:
            raise exc.internal("YC KMS Decrypt returned no plaintext")

        return plaintext

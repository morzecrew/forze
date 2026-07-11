from __future__ import annotations

from forze_kms.gcp._compat import require_kms_gcp

require_kms_gcp()

# ....................... #

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, final

import attrs
import grpc
from google.api_core.exceptions import AlreadyExists
from google.cloud.kms_v1.services.key_management_service import (
    KeyManagementServiceAsyncClient,
)
from google.cloud.kms_v1.services.key_management_service.transports import (
    KeyManagementServiceGrpcAsyncIOTransport,
)
from google.cloud.kms_v1.types import CryptoKey, CryptoKeyVersion

from forze.base.exceptions import exc

from .._logger import logger
from .errors import exc_interceptor
from .port import GcpKmsClientPort
from .value_objects import GcpKmsConfig

# ----------------------- #

# gRPC ignores the standard ``NO_PROXY``; this channel option makes a plaintext
# emulator connection bypass an ambient ``http_proxy`` instead of being routed
# through it (which fails against a local emulator).
_EMULATOR_CHANNEL_OPTS = [("grpc.enable_http_proxy", 0)]

_DESTROYABLE_STATES = frozenset(
    {
        CryptoKeyVersion.CryptoKeyVersionState.ENABLED,
        CryptoKeyVersion.CryptoKeyVersionState.DISABLED,
    }
)
"""Versions that still hold key material — the ones a teardown must destroy."""

# ....................... #


@final
@attrs.define(slots=True)
class GcpKmsClient(GcpKmsClientPort):
    """Async GCP KMS client over ``google-cloud-kms``.

    :meth:`initialize` builds one long-lived async client that every operation
    reuses until :meth:`close`. Against real GCP it uses the default secure
    transport and application-default (or injected) credentials; against a
    plaintext emulator (*endpoint* set) it builds an insecure gRPC channel.
    """

    __client: KeyManagementServiceAsyncClient | None = attrs.field(
        default=None, init=False
    )
    __channel: grpc.aio.Channel | None = attrs.field(default=None, init=False)
    """The insecure channel opened for an emulator endpoint (owned here, closed on
    :meth:`close`). ``None`` for the real-GCP path, where the client owns its
    transport."""

    __request_timeout: float | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        *,
        endpoint: str | None = None,
        credentials: Any | None = None,
        config: GcpKmsConfig | None = None,
    ) -> None:
        """Open the long-lived async KMS client.

        No-ops if already initialized. *endpoint* points at a plaintext
        emulator (``host:port``, insecure gRPC); ``None`` targets real GCP with
        the default secure transport. *credentials* is an optional
        ``google.auth`` credentials object (``None`` = application-default);
        it is ignored on the emulator path.
        """

        async with self.__init_lock:
            if self.__client is not None:
                return

            cfg = config if config is not None else GcpKmsConfig()
            self.__request_timeout = cfg.request_timeout

            if endpoint is not None:
                channel = grpc.aio.insecure_channel(
                    endpoint, options=_EMULATOR_CHANNEL_OPTS
                )
                transport = KeyManagementServiceGrpcAsyncIOTransport(channel=channel)
                self.__channel = channel
                self.__client = KeyManagementServiceAsyncClient(transport=transport)

            else:
                self.__client = KeyManagementServiceAsyncClient(credentials=credentials)

            logger.trace("GCP KMS client connected", endpoint=endpoint)

    # ....................... #

    async def close(self) -> None:
        """Close the long-lived client (and the emulator channel, if owned)."""

        async with self.__init_lock:
            client = self.__client
            channel = self.__channel
            self.__client = None
            self.__channel = None
            self.__request_timeout = None

            try:
                if channel is not None:
                    await channel.close()

                elif client is not None:
                    # google-cloud-kms's generated transport close() is untyped.
                    await client.transport.close()  # type: ignore[no-untyped-call]

            finally:
                logger.trace("GCP KMS client closed")

    # ....................... #

    def __require_client(self) -> KeyManagementServiceAsyncClient:
        if self.__client is None:
            raise exc.internal("GCP KMS client is not initialized")

        return self.__client

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[KeyManagementServiceAsyncClient]:
        """Yield the underlying async KMS client for raw calls (e.g. provisioning)."""

        yield self.__require_client()

    # ....................... #

    def __timeout_kwargs(self) -> dict[str, Any]:
        if self.__request_timeout is None:
            return {}

        return {"timeout": self.__request_timeout}

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Report whether the client is initialized (no cheap key-less KMS ping)."""

        try:
            self.__require_client()
            return "ok", True

        except Exception as e:
            logger.debug("GCP KMS health check failed", exc_info=True)
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("gcpkms.encrypt")
    async def encrypt(self, key_name: str, plaintext: bytes) -> bytes:
        """Encrypt *plaintext* under CryptoKey *key_name* via ``Encrypt``."""

        c = self.__require_client()
        resp = await c.encrypt(  # pyright: ignore[reportUnknownMemberType]
            name=key_name,
            plaintext=plaintext,
            **self.__timeout_kwargs(),
        )

        return resp.ciphertext

    # ....................... #

    @exc_interceptor.coroutine("gcpkms.decrypt")
    async def decrypt(self, key_name: str, ciphertext: bytes) -> bytes:
        """Decrypt *ciphertext* under CryptoKey *key_name* via ``Decrypt``."""

        c = self.__require_client()
        resp = await c.decrypt(  # pyright: ignore[reportUnknownMemberType]
            name=key_name,
            ciphertext=ciphertext,
            **self.__timeout_kwargs(),
        )

        return resp.plaintext

    # ....................... #
    # Key administration (per-tenant provisioning)

    @exc_interceptor.coroutine("gcpkms.ensure_crypto_key")
    async def ensure_crypto_key(self, parent: str, crypto_key_id: str) -> str:
        """Create a symmetric CryptoKey under *parent*, tolerating an existing one."""

        c = self.__require_client()

        try:
            created = await c.create_crypto_key(  # pyright: ignore[reportUnknownMemberType]
                parent=parent,
                crypto_key_id=crypto_key_id,
                crypto_key=CryptoKey(
                    purpose=CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
                ),
                **self.__timeout_kwargs(),
            )

        except AlreadyExists:
            return f"{parent}/cryptoKeys/{crypto_key_id}"

        return created.name

    # ....................... #

    @exc_interceptor.coroutine("gcpkms.destroy_crypto_key_versions")
    async def destroy_crypto_key_versions(self, key_name: str) -> int:
        """Schedule every non-destroyed version of *key_name* for destruction."""

        c = self.__require_client()
        destroyed = 0

        versions = await c.list_crypto_key_versions(  # pyright: ignore[reportUnknownMemberType]
            parent=key_name,
            **self.__timeout_kwargs(),
        )

        async for version in versions:
            if version.state in _DESTROYABLE_STATES:
                await c.destroy_crypto_key_version(  # pyright: ignore[reportUnknownMemberType]
                    name=version.name,
                    **self.__timeout_kwargs(),
                )
                destroyed += 1

        return destroyed

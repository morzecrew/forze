from __future__ import annotations

from forze_kms.aws._compat import require_kms_aws

require_kms_aws()

# ....................... #

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    cast,
    final,
)

import aioboto3
import attrs
from pydantic import SecretStr

if TYPE_CHECKING:
    # Type-only: ``types-aiobotocore-kms`` is a stub package with no runtime
    # value, so keep it off the import path.
    from types_aiobotocore_kms.client import KMSClient as AsyncKmsClient

from forze.base.exceptions import exc

from .._logger import logger
from .errors import exc_interceptor
from .port import AwsKmsClientPort
from .value_objects import AwsKmsConfig, AwsKmsConnectionOpts

# ----------------------- #

_MIN_PENDING_WINDOW_DAYS = 7
"""Shortest deletion window KMS accepts — used to retire a CMK whose alias never landed."""


@final
@attrs.define(slots=True)
class AwsKmsClient(AwsKmsClientPort):
    """Async AWS KMS client with a long-lived, shared connection.

    :meth:`initialize` opens a single ``aioboto3`` KMS client that every
    operation reuses until :meth:`close`. aiobotocore clients are coroutine-safe
    for concurrent calls (one pooled ``aiohttp`` session, credential refresh
    serialized behind a lock), so the one instance serves concurrent operations
    directly — no per-call scope bookkeeping. Un-lifecycled usage (no
    :meth:`initialize`) still works: :meth:`client` opens a transient client per
    scope as a fallback.
    """

    __opts: AwsKmsConnectionOpts | None = attrs.field(default=None, init=False)
    __session: aioboto3.Session | None = attrs.field(default=None, init=False)

    __persistent_client: AsyncKmsClient | None = attrs.field(default=None, init=False)
    """Long-lived KMS client opened by :meth:`initialize`, shared by all callers."""

    __exit_stack: AsyncExitStack | None = attrs.field(default=None, init=False)
    """Owns the persistent client's async context; closed by :meth:`close`."""

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        *,
        endpoint: str | None = None,
        region_name: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | SecretStr | None = None,
        config: AwsKmsConfig | None = None,
    ) -> None:
        """Configure the client and open a long-lived ``aioboto3`` KMS client.

        No-ops if already initialized. Concurrent calls serialize on an internal
        lock. *endpoint* is optional (``None`` lets botocore resolve the real AWS
        KMS endpoint from the region); pass it for an S3-compatible/LocalStack
        endpoint. *region_name*, when given, overrides ``config.region_name``.

        Credentials are optional: with both *access_key_id* and
        *secret_access_key* ``None`` the default botocore credential chain
        resolves them (env, shared config, container/instance roles). Passing
        only one of the two raises a configuration error.
        """

        async with self.__init_lock:
            if self.__session is not None:
                return

            cfg = config if config is not None else AwsKmsConfig()

            if region_name is not None:
                cfg = attrs.evolve(cfg, region_name=region_name)

            aio_config = cfg.to_aio_config()

            self.__opts = AwsKmsConnectionOpts(
                endpoint=endpoint,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                config=aio_config,
            )
            self.__session = aioboto3.Session()

            stack = AsyncExitStack()

            try:
                self.__persistent_client = await stack.enter_async_context(
                    self.__create_client_cm()
                )

            except BaseException:
                await stack.aclose()
                self.__persistent_client = None
                self.__session = None
                self.__opts = None
                raise

            self.__exit_stack = stack
            logger.trace("AWS KMS client connected", endpoint=endpoint)

    # ....................... #

    async def close(self) -> None:
        """Close the long-lived client and release session and options."""

        async with self.__init_lock:
            stack = self.__exit_stack
            self.__exit_stack = None
            self.__persistent_client = None

            try:
                if stack is not None:
                    await stack.aclose()

            finally:
                self.__session = None
                self.__opts = None

            logger.trace("AWS KMS client closed")

    # ....................... #

    def __require_session(self) -> aioboto3.Session:
        if self.__session is None:
            raise exc.internal("AWS KMS session is not initialized")

        return self.__session

    # ....................... #

    def __create_client_cm(self) -> AsyncContextManager[AsyncKmsClient]:
        """Build the ``aiobotocore`` KMS client async context manager from opts."""

        session = self.__require_session()
        opts = self.__opts

        if opts is None:
            raise exc.internal("AWS KMS client options are not initialized")

        kwargs: dict[str, Any] = {"config": opts.config}

        if opts.endpoint is not None:
            kwargs["endpoint_url"] = opts.endpoint

        if opts.access_key_id is not None and opts.secret_access_key is not None:
            kwargs["aws_access_key_id"] = opts.access_key_id
            kwargs["aws_secret_access_key"] = opts.secret_access_key.get_secret_value()

        cm = session.client("kms", **kwargs)  # type: ignore

        return cast("AsyncContextManager[AsyncKmsClient]", cm)

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncKmsClient]:
        """Yield the underlying KMS client for a scope.

        Hands out the long-lived client opened by :meth:`initialize` (shared, so
        the scope neither opens nor closes it); when the client was never
        initialized it opens a transient client for the scope instead. Useful for
        raw KMS calls beyond the two port operations (e.g. key provisioning).
        """

        persistent = self.__persistent_client

        if persistent is not None:
            yield persistent
            return

        async with self.__create_client_cm() as c:
            yield c

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Check KMS connectivity by listing keys.

        :returns: ``("ok", True)`` on success, or ``(error_message, False)``.
        """

        try:
            async with self.client() as c:
                await c.list_keys(Limit=1)
                return "ok", True

        except Exception as e:
            logger.debug("AWS KMS health check failed", exc_info=True)
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("awskms.generate_data_key")
    async def generate_data_key(
        self,
        key_id: str,
        *,
        key_spec: str = "AES_256",
    ) -> tuple[bytes, bytes]:
        """Generate a data key under CMK *key_id* via ``GenerateDataKey``.

        :returns: ``(plaintext, ciphertext_blob)`` — the raw data key and the
            opaque KMS-wrapped blob (self-describing: it names its own CMK).
        """

        async with self.client() as c:
            resp = await c.generate_data_key(
                KeyId=key_id,
                KeySpec=cast(Any, key_spec),
            )

        plaintext = resp.get("Plaintext")
        blob = resp.get("CiphertextBlob")

        if not plaintext or not blob:
            raise exc.internal("AWS KMS GenerateDataKey returned no key material")

        return bytes(plaintext), bytes(blob)

    # ....................... #

    @exc_interceptor.coroutine("awskms.decrypt")
    async def decrypt(
        self,
        ciphertext_blob: bytes,
        *,
        key_id: str | None = None,
    ) -> bytes:
        """Decrypt a KMS ``ciphertext_blob`` via ``Decrypt``, returning plaintext.

        When *key_id* is set it is passed as ``KeyId`` so KMS rejects a blob not
        wrapped under that CMK (a server-side confused-deputy guard).
        """

        kwargs: dict[str, Any] = {"CiphertextBlob": ciphertext_blob}

        if key_id is not None:
            kwargs["KeyId"] = key_id

        async with self.client() as c:
            resp = await c.decrypt(**kwargs)

        plaintext = resp.get("Plaintext")

        if not plaintext:
            raise exc.internal("AWS KMS Decrypt returned no plaintext")

        return bytes(plaintext)

    # ....................... #
    # Key administration (per-tenant provisioning)

    @exc_interceptor.coroutine("awskms.find_key_id_by_alias")
    async def find_key_id_by_alias(self, alias: str) -> str | None:
        """Resolve *alias* to its CMK id via ``DescribeKey``, or ``None`` if absent."""

        async with self.client() as c:
            try:
                resp = await c.describe_key(KeyId=alias)

            except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
                code = (e.response or {}).get("Error", {}).get("Code")

                if code in {"NotFoundException", "NotFound"}:
                    return None

                raise

        return resp.get("KeyMetadata", {}).get("KeyId")

    # ....................... #

    @exc_interceptor.coroutine("awskms.create_key_with_alias")
    async def create_key_with_alias(
        self,
        alias: str,
        *,
        description: str | None = None,
    ) -> str:
        """Create a symmetric CMK via ``CreateKey`` and point *alias* at it.

        A CMK is only reachable through its alias here, so if aliasing fails the key it
        just minted would be an orphan — billable, unaddressable, and re-created on every
        retry. The key is therefore scheduled for deletion on that path, best-effort: a
        failure to clean up must not mask the aliasing error that caused it.
        """

        async with self.client() as c:
            resp = await c.create_key(Description=description or "")
            key_id = resp.get("KeyMetadata", {}).get("KeyId")

            if not key_id:
                raise exc.internal("AWS KMS CreateKey returned no KeyId")

            try:
                await c.create_alias(AliasName=alias, TargetKeyId=key_id)

            except BaseException:
                with suppress(Exception):
                    await c.schedule_key_deletion(
                        KeyId=key_id,
                        PendingWindowInDays=_MIN_PENDING_WINDOW_DAYS,
                    )

                raise

        return key_id

    # ....................... #

    @exc_interceptor.coroutine("awskms.delete_alias")
    async def delete_alias(self, alias: str) -> None:
        """Delete *alias*, tolerating an already-absent one (idempotent teardown)."""

        async with self.client() as c:
            try:
                await c.delete_alias(AliasName=alias)

            except c.exceptions.ClientError as e:  # type: ignore[attr-defined]
                code = (e.response or {}).get("Error", {}).get("Code")

                if code in {"NotFoundException", "NotFound"}:
                    return

                raise

    # ....................... #

    @exc_interceptor.coroutine("awskms.schedule_key_deletion")
    async def schedule_key_deletion(
        self,
        key_id: str,
        *,
        pending_window_days: int = 30,
    ) -> None:
        """Schedule the CMK for deletion after *pending_window_days* (KMS allows 7-30)."""

        async with self.client() as c:
            await c.schedule_key_deletion(
                KeyId=key_id,
                PendingWindowInDays=pending_window_days,
            )

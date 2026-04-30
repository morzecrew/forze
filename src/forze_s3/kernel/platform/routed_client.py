"""S3 client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from typing import AsyncIterator
from uuid import UUID

import attrs
from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
)
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from .client import S3Client, S3Config, S3Head
from .routing_credentials import S3RoutingCredentials

# ----------------------- #


@attrs.define(slots=True)
class RoutedS3Client:
    """Routes each operation to a lazily created :class:`S3Client` for the current tenant.

    Credentials are JSON secrets (see :class:`S3RoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_str` / ``resolve_structured``.

    Register this instance under :data:`~forze_s3.execution.deps.S3ClientDepKey` and
    use :func:`~forze_s3.execution.lifecycle.routed_s3_lifecycle_step` for startup/shutdown.

    Do not combine with :func:`~forze_s3.execution.lifecycle.s3_lifecycle_step` on the same
    registered instance.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    botocore_config: S3Config | None = None
    max_cached_tenants: int = 100

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, S3Client] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    def _get_secret_ref(self, tenant_id: UUID) -> SecretRef:
        if callable(self.secret_ref_for_tenant):
            return self.secret_ref_for_tenant(tenant_id)

        return self.secret_ref_for_tenant[tenant_id]

    # ....................... #

    async def startup(self) -> None:
        self._started = True

    # ....................... #

    async def close(self) -> None:
        async with self._lock:
            to_close = list(self._clients.values())
            self._clients.clear()

        for c in to_close:
            await c.close()

        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        async with self._lock:
            client = self._clients.pop(tenant_id, None)

        if client is not None:
            await client.close()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed S3 access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> S3Client:
        if not self._started:
            raise InfrastructureError("Routed S3 client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self._get_secret_ref(tid)

            try:
                creds = await resolve_structured(
                    self.secrets,
                    ref,
                    S3RoutingCredentials,
                )

            except SecretNotFoundError:
                raise

            except CoreError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve S3 secret for tenant {tid}: {e}",
                ) from e

            client = S3Client()
            await client.initialize(
                creds.endpoint,
                creds.access_key_id,
                creds.secret_access_key,
                config=self.botocore_config,
            )
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncIterator[AsyncS3Client]:
        inner = await self._get_client()

        async with inner.client() as c:
            yield c

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.health()

    # ....................... #

    async def bucket_exists(self, bucket: str) -> bool:
        inner = await self._get_client()

        async with inner.client():
            return await inner.bucket_exists(bucket)

    async def create_bucket(self, bucket: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.create_bucket(bucket)

    async def ensure_bucket(self, bucket: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.ensure_bucket(bucket)

    async def object_exists(self, bucket: str, key: str) -> bool:
        inner = await self._get_client()

        async with inner.client():
            return await inner.object_exists(bucket, key)

    async def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.upload_bytes(
                bucket,
                key,
                data,
                content_type=content_type,
                metadata=metadata,
                tags=tags,
            )

    async def download_bytes(self, bucket: str, key: str) -> bytes:
        inner = await self._get_client()

        async with inner.client():
            return await inner.download_bytes(bucket, key)

    async def delete_object(self, bucket: str, key: str) -> None:
        inner = await self._get_client()

        async with inner.client():
            await inner.delete_object(bucket, key)

    async def list_objects(
        self,
        bucket: str,
        prefix: str | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[ObjectTypeDef], int]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.list_objects(
                bucket,
                prefix,
                limit=limit,
                offset=offset,
            )

    async def head_object(self, bucket: str, key: str) -> S3Head:
        inner = await self._get_client()

        async with inner.client():
            return await inner.head_object(bucket, key)

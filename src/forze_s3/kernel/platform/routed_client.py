"""S3 client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Mapping, final
from uuid import UUID

import attrs
from types_aiobotocore_s3.client import S3Client as AsyncS3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy import require_tenant_id
from forze.base.exceptions import exc
from forze.base.primitives.lru_registry import SimpleLruRegistry

from .client import S3Client
from .port import S3ClientPort
from .routing_credentials import S3RoutingCredentials
from .value_objects import S3Config, S3Head

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedS3Client(S3ClientPort):
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

    _registry: SimpleLruRegistry[UUID, S3Client] = attrs.field(init=False)
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise exc.internal("max_cached_tenants must be at least 1")

        self._registry = SimpleLruRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
        )

    # ....................... #

    async def startup(self) -> None:
        self._started = True

    # ....................... #

    async def close(self) -> None:
        await self._registry.close_all()
        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self._registry.evict(tenant_id)

    # ....................... #

    async def _create_client(self, tid: UUID) -> S3Client:
        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tid)

        try:
            creds = await resolve_structured(
                self.secrets,
                ref,
                S3RoutingCredentials,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve S3 secret for tenant {tid}: {e}",
            ) from e

        client = S3Client()
        await client.initialize(
            creds.endpoint,
            creds.access_key_id,
            creds.secret_access_key,
            config=self.botocore_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> S3Client:
        if not self._started:
            raise exc.internal("Routed S3 client is not started")

        return await self._registry.get_or_create(
            require_tenant_id(
                self.tenant_provider,
                message="Tenant ID is required for routed S3 access",
            ),
        )

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncS3Client]:
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

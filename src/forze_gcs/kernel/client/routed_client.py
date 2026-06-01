"""GCS client that resolves GCP credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Mapping, final
from uuid import UUID

import attrs
from gcloud.aio.storage import Storage

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.integrations.storage.client import (
    ObjectStorageHead,
    ObjectStorageListedObject,
)
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.primitives.fingerprint import gcp_credential_dedup_tag, stable_fingerprint
from forze.base.primitives.gcp_service_file import materialize_service_account_json

from .client import GCSClient
from .port import GCSClientPort
from .routing_credentials import GCSRoutingCredentials
from .value_objects import GCSConfig

# ----------------------- #


def _service_file_for_init(creds: GCSRoutingCredentials) -> tuple[str | None, bool]:
    if creds.service_file is not None:
        return creds.service_file, False

    if creds.service_account_json is None:
        return None, False

    return materialize_service_account_json(
        creds.service_account_json,
        prefix="forze-gcs-",
    )


@final
@attrs.define(slots=True)
class RoutedGCSClient(GCSClientPort):
    """Routes each operation to a lazily created :class:`GCSClient` for the current tenant.

    Credentials are JSON secrets (see :class:`GCSRoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register under :data:`~forze_gcs.execution.deps.GCSClientDepKey` and use
    :func:`~forze_gcs.execution.lifecycle.routed_gcs_lifecycle_step` for startup/shutdown.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    client_config: GCSConfig | None = None
    max_cached_tenants: int = 100

    __pool: TenantClientRegistry[GCSClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            guarded=False,
        )

    # ....................... #

    async def startup(self) -> None:
        await self.__pool.startup()

    # ....................... #

    async def close(self) -> None:
        await self.__pool.close()

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self.__pool.evict(tenant_id)

    # ....................... #

    async def _fingerprint_for(self, tenant_id: UUID) -> str:
        creds = await resolve_structured_for_tenant(
            GCSRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="GCS",
        )

        return stable_fingerprint(
            creds.project_id,
            gcp_credential_dedup_tag(
                service_file=creds.service_file,
                service_account_json=creds.service_account_json,
            ),
        )

    # ....................... #

    async def _create_client(self, tid: UUID) -> GCSClient:
        creds = await resolve_structured_for_tenant(
            GCSRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="GCS",
        )
        client = GCSClient()
        service_file, service_file_owned = _service_file_for_init(creds)

        await client.initialize(
            creds.project_id,
            service_file=service_file,
            service_file_owned=service_file_owned,
            config=self.client_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> GCSClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed GCS access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[Storage]:
        inner = await self._get_client()

        async with inner.client() as storage:
            yield storage

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.health()

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
    ) -> tuple[list[ObjectStorageListedObject], int]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.list_objects(
                bucket,
                prefix,
                limit=limit,
                offset=offset,
            )

    async def head_object(self, bucket: str, key: str) -> ObjectStorageHead:
        inner = await self._get_client()

        async with inner.client():
            return await inner.head_object(bucket, key)

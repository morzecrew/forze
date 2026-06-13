"""S3 client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Callable, Mapping, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

if TYPE_CHECKING:
    # Type-only stub package; kept off the runtime import path.
    from types_aiobotocore_s3.client import S3Client as AsyncS3Client

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.integrations.storage import RoutedObjectStorageClientBase
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .client import S3Client
from .port import S3ClientPort
from .routing_credentials import S3RoutingCredentials
from .value_objects import S3Config

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedS3Client(RoutedObjectStorageClientBase[S3Client], S3ClientPort):
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
    creds_type: type[BaseModel] = attrs.field(default=S3RoutingCredentials, init=False)
    backend: str = attrs.field(default="S3", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed S3 access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(S3RoutingCredentials, creds)

        return build_routing_fingerprint(
            public=[c.endpoint, c.access_key_id],
            secret=[c.secret_access_key],
        )

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: S3RoutingCredentials,
    ) -> S3Client:
        client = S3Client()
        await client.initialize(
            creds.endpoint,
            creds.access_key_id,
            creds.secret_access_key,
            config=self.botocore_config,
        )

        return client

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncS3Client]:
        inner = await self._get_client()

        async with inner.client() as c:
            yield c

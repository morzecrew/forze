"""GCS client that resolves GCP credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Mapping, cast, final
from uuid import UUID

import attrs
from gcloud.aio.storage import Storage
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.integrations.storage import RoutedObjectStorageClientBase

from .client import GCSClient
from .port import GCSClientPort
from .routing_credentials import (
    GCSRoutingCredentials,
    credential_file_for_init,
    routing_fingerprint,
)
from .value_objects import GCSConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedGCSClient(RoutedObjectStorageClientBase[GCSClient], GCSClientPort):
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
    creds_type: type[BaseModel] = attrs.field(default=GCSRoutingCredentials, init=False)
    backend: str = attrs.field(default="GCS", init=False)
    credential_file_prefix: str = attrs.field(default="forze-gcs-", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed GCS access",
        init=False,
    )

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(GCSRoutingCredentials, creds))

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: GCSRoutingCredentials,
    ) -> GCSClient:
        client = GCSClient()
        credential_path = credential_file_for_init(
            creds,
            prefix=self.credential_file_prefix,
        )

        await client.initialize(
            creds.project_id,
            service_file=credential_path.path,
            service_file_owned=credential_path.owned,
            config=self.client_config,
        )

        return client

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[Storage]:
        inner = await self._get_client()

        async with inner.client() as storage:
            yield storage

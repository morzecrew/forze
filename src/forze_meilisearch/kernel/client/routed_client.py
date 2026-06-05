"""Meilisearch client that resolves URL and API key per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Mapping, cast, final

if TYPE_CHECKING:
    from meilisearch_python_sdk.models.search import SearchParams

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .client import MeilisearchClient
from .port import MeilisearchClientPort
from .routing_credentials import MeilisearchRoutingCredentials
from .value_objects import MeilisearchConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedMeilisearchClient(
    StructuredSecretRoutedTenantClientBase[MeilisearchClient],
    MeilisearchClientPort,
):
    """Routes each operation to a lazily created :class:`MeilisearchClient` for the current tenant.

    Credentials are JSON secrets (see :class:`MeilisearchRoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register under :data:`~forze_meilisearch.execution.deps.MeilisearchClientDepKey` and use
    :func:`~forze_meilisearch.execution.lifecycle.routed_meilisearch_lifecycle_step`.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    client_config: MeilisearchConfig | None = None
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=MeilisearchRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="Meilisearch", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Meilisearch access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(MeilisearchRoutingCredentials, creds)

        return build_routing_fingerprint(public=[c.url], secret=[c.api_key])

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: MeilisearchRoutingCredentials,
    ) -> MeilisearchClient:
        client = MeilisearchClient()

        await client.initialize(
            creds.url,
            creds.api_key,
            config=self.client_config,
        )

        return client

    # ....................... #

    async def aclose(self) -> None:
        await self.close()

    async def health(self) -> bool:
        inner = await self._get_client()
        return await inner.health()

    def index(self, uid: str) -> Any:
        tid = self._require_tenant_id()
        inner = self._peek_client(tid)

        if inner is None:
            raise exc.internal(
                "Routed Meilisearch inner client is not initialized for this tenant; "
                "call an async port method first.",
            )

        return inner.index(uid)

    async def get_or_create_index(
        self,
        uid: str,
        *,
        primary_key: str | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.get_or_create_index(uid, primary_key=primary_key)

    async def multi_search(
        self,
        queries: list["SearchParams"],
        *,
        federation: JsonDict | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.multi_search(queries, federation=federation)

    async def wait_for_task(
        self,
        task_uid: int,
        *,
        timeout: timedelta | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.wait_for_task(task_uid, timeout=timeout)

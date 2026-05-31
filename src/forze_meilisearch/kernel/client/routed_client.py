"""Meilisearch client that resolves URL and API key per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Mapping, final

if TYPE_CHECKING:
    from meilisearch_python_sdk.models.search import SearchParams

from uuid import UUID

import attrs

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import secret_dedup_fingerprint, stable_fingerprint

from .client import MeilisearchClient
from .port import MeilisearchClientPort
from .routing_credentials import MeilisearchRoutingCredentials
from .value_objects import MeilisearchConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedMeilisearchClient(MeilisearchClientPort):
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

    __pool: TenantClientRegistry[MeilisearchClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.aclose(),
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
            MeilisearchRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Meilisearch",
        )

        return stable_fingerprint(creds.url, secret_dedup_fingerprint(creds.api_key))

    # ....................... #

    async def _create_client(self, tid: UUID) -> MeilisearchClient:
        creds = await resolve_structured_for_tenant(
            MeilisearchRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Meilisearch",
        )
        client = MeilisearchClient()

        await client.initialize(
            creds.url,
            creds.api_key,
            config=self.client_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> MeilisearchClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Meilisearch access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    async def aclose(self) -> None:
        await self.close()

    async def health(self) -> bool:
        inner = await self._get_client()
        return await inner.health()

    def index(self, uid: str) -> Any:
        tid = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Meilisearch access",
        )
        inner = self.__pool.peek(tid)

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

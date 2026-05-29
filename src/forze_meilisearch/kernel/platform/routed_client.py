"""Meilisearch client that resolves URL and API key per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Mapping, final

if TYPE_CHECKING:
    from meilisearch_python_sdk.models.search import SearchParams

from uuid import UUID

import attrs

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy import require_tenant_id
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import secret_dedup_fingerprint, stable_fingerprint
from forze.base.primitives.lru_registry import SimpleLruRegistry

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

    _registry: SimpleLruRegistry[UUID, MeilisearchClient] = attrs.field(init=False)
    _fingerprints: dict[UUID, str] = attrs.field(factory=dict, init=False, repr=False)
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise exc.internal("max_cached_tenants must be at least 1")

        self._registry = SimpleLruRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.aclose(),
            dedup_key=lambda tid: self._fingerprints[tid],
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
        self._fingerprints.pop(tenant_id, None)
        await self._registry.evict(tenant_id)

    # ....................... #

    async def _resolve_creds(self, tenant_id: UUID) -> MeilisearchRoutingCredentials:
        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tenant_id)

        try:
            return await resolve_structured(
                self.secrets,
                ref,
                MeilisearchRoutingCredentials,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve Meilisearch secret for tenant {tenant_id}: {e}",
            ) from e

    # ....................... #

    async def _ensure_fingerprint(self, tenant_id: UUID) -> str:
        cached = self._fingerprints.get(tenant_id)

        if cached is not None:
            return cached

        creds = await self._resolve_creds(tenant_id)
        fingerprint = stable_fingerprint(creds.url, secret_dedup_fingerprint(creds.api_key))
        self._fingerprints[tenant_id] = fingerprint

        return fingerprint

    # ....................... #

    async def _create_client(self, tid: UUID) -> MeilisearchClient:
        creds = await self._resolve_creds(tid)
        client = MeilisearchClient()

        await client.initialize(
            creds.url,
            creds.api_key,
            config=self.client_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> MeilisearchClient:
        if not self._started:
            raise exc.internal("Routed Meilisearch client is not started")

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Meilisearch access",
        )
        await self._ensure_fingerprint(tenant_id)

        return await self._registry.get_or_create(tenant_id)

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
        inner = self._registry.peek(tid)

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

"""HTTP client that resolves base URL and headers per tenant via secrets."""

from typing import Any, Callable, Mapping, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.primitives import JsonDict

from .client import HttpxClient
from .credentials import credential_auth_headers
from .port import HttpxClientPort
from .routing_credentials import HttpRoutingCredentials, routing_fingerprint
from .value_objects import HttpxConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedHttpxClient(
    StructuredSecretRoutedTenantClientBase[HttpxClient],
    HttpxClientPort,
):
    """Routes HTTP requests to a per-tenant :class:`HttpxClient`."""

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    client_config: HttpxConfig | None = None
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(default=HttpRoutingCredentials, init=False)
    backend: str = "http"
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed HTTP access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(HttpRoutingCredentials, creds))

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: HttpRoutingCredentials,
    ) -> HttpxClient:
        client = HttpxClient()

        await client.initialize(
            creds.base_url,
            config=self.client_config,
            default_headers=credential_auth_headers(creds),
        )

        return client

    # ....................... #

    async def aclose(self) -> None:
        await self.close()

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await super().evict_tenant(tenant_id)

    # ....................... #

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: JsonDict | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any:
        inner = await self._get_client()

        return await inner.request(
            method,
            url,
            params=params,
            json=json,
            headers=headers,
            timeout=timeout,
        )

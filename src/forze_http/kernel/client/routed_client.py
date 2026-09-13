"""HTTP client that resolves base URL and headers per tenant via secrets."""

from collections.abc import Callable, Mapping
from typing import Any, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.primitives import JsonDict

from .._logger import logger
from .cleartext import is_cleartext_destination
from .client import HttpClient
from .credentials import credential_auth_headers
from .port import HttpClientPort
from .routing_credentials import HttpRoutingCredentials, routing_fingerprint
from .value_objects import HttpConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedHttpClient(
    StructuredSecretRoutedTenantClientBase[HttpClient],
    HttpClientPort,
):
    """Routes HTTP requests to a per-tenant :class:`HttpClient`."""

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    client_config: HttpConfig | None = None
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
    ) -> HttpClient:
        client = HttpClient()
        headers = credential_auth_headers(creds)

        # A tenant-routed service has no base_url at wiring — it comes from this tenant's
        # secret — so the config's cleartext check cannot see it and the question has to be
        # asked here, once per tenant client. The tenant id rather than the credential: the
        # point is which tenant to fix, and the headers are the credential.
        if headers and is_cleartext_destination(creds.base_url):
            logger.warning(
                "http.routed.cleartext_credentials",
                tenant_id=str(tenant_id),
                base_url=creds.base_url,
                detail=(
                    "a tenant's routed HTTP credentials point at a plaintext base_url, so "
                    "they are readable by anything on the path; use https for this "
                    "tenant's route, or terminate TLS closer to the caller"
                ),
            )

        await client.initialize(
            creds.base_url,
            config=self.client_config,
            default_headers=headers,
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
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> Any:
        inner = await self._get_client()

        return await inner.request(
            method,
            url,
            params=params,
            json=json,
            data=data,
            headers=headers,
            timeout=timeout,
            raise_for_status=raise_for_status,
        )

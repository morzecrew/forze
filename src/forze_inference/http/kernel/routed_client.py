"""Served-model client that resolves its endpoint per tenant via secrets."""

from collections.abc import Callable, Mapping
from typing import Any, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)

from .client import DEFAULT_REQUEST_TIMEOUT_S, InferenceHttpClient
from .port import InferenceHttpClientPort
from .routing_credentials import (
    InferenceHttpRoutingCredentials,
    credential_headers,
    routing_fingerprint,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedInferenceHttpClient(
    StructuredSecretRoutedTenantClientBase[InferenceHttpClient],
    InferenceHttpClientPort,
):
    """Routes each inference call to a per-tenant :class:`InferenceHttpClient`.

    This is what raises a served-model route to ``dedicated`` isolation: a tenant's features
    are sent to *that tenant's own* model server, resolved from its own secret, rather than
    to a shared endpoint distinguished only by a model name. Clients are created lazily per
    tenant and cached (bounded by :attr:`max_cached_tenants`); a credential rotation changes
    the fingerprint and transparently rebuilds the tenant's client.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=InferenceHttpRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="inference_http", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed inference access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(InferenceHttpRoutingCredentials, creds))

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: InferenceHttpRoutingCredentials,
    ) -> InferenceHttpClient:
        client = InferenceHttpClient()

        await client.initialize(
            creds.base_url,
            default_headers=credential_headers(creds),
            timeout_s=self.timeout_s,
        )

        return client

    # ....................... #

    async def post_json(
        self,
        path: str,
        body: Mapping[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        inner = await self._get_client()

        return await inner.post_json(path, body, timeout=timeout)

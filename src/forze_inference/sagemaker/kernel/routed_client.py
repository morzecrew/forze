"""SageMaker runtime client that resolves AWS credentials per tenant via secrets."""

from collections.abc import Callable, Mapping
from typing import Any, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)

from .client import SageMakerRuntimeClient
from .port import SageMakerRuntimeClientPort
from .routing_credentials import SageMakerRoutingCredentials, routing_fingerprint

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedSageMakerRuntimeClient(
    StructuredSecretRoutedTenantClientBase[SageMakerRuntimeClient],
    SageMakerRuntimeClientPort,
):
    """Routes each invocation to a per-tenant :class:`SageMakerRuntimeClient`.

    This is what raises a SageMaker route to ``dedicated`` isolation: each tenant's features
    are invoked under *that tenant's own AWS identity*, so endpoint access is enforced by IAM
    rather than only by the endpoint name the app happens to resolve. Clients are created
    lazily per tenant and cached (bounded by :attr:`max_cached_tenants`); a credential
    rotation changes the fingerprint and transparently rebuilds the tenant's client.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=SageMakerRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="sagemaker_runtime", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed SageMaker access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(SageMakerRoutingCredentials, creds))

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: SageMakerRoutingCredentials,
    ) -> SageMakerRuntimeClient:
        client = SageMakerRuntimeClient()

        await client.initialize(
            region_name=creds.region_name,
            endpoint_url=creds.endpoint_url,
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
        )

        return client

    # ....................... #

    async def invoke_endpoint(
        self,
        endpoint_name: str,
        *,
        body: bytes,
        content_type: str = "application/json",
        accept: str = "application/json",
        target_variant: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        inner = await self._get_client()

        return await inner.invoke_endpoint(
            endpoint_name,
            body=body,
            content_type=content_type,
            accept=accept,
            target_variant=target_variant,
            timeout=timeout,
        )

"""Inngest client that resolves app credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from collections.abc import Callable, Mapping
from typing import cast, final
from uuid import UUID

import attrs
import inngest
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import exc
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .client import InngestClient
from .config import InngestConfig
from .port import InngestClientPort
from .routing_credentials import InngestRoutingCredentials

# ----------------------- #


def _to_inngest_config(creds: InngestRoutingCredentials) -> InngestConfig:
    return InngestConfig(
        event_key=creds.event_key,  # type: ignore[arg-type]
        signing_key=creds.signing_key,  # type: ignore[arg-type]
        is_production=creds.is_production,
        request_timeout=creds.request_timeout,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedInngestClient(
    StructuredSecretRoutedTenantClientBase[InngestClient],
    InngestClientPort,
):
    """Routes ``send`` to a lazily created :class:`InngestClient` for the current tenant.

    Credentials are JSON secrets (see :class:`InngestRoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_structured`.

    Framework ``serve()`` registration typically uses a single app; multi-tenant **event
    emission** uses :meth:`send` under a bound :class:`~forze.application.contracts.tenancy.TenantIdentity`.
    :attr:`native` requires an initialized inner client for the current tenant.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=InngestRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="Inngest", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Inngest access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(InngestRoutingCredentials, creds)
        timeout_fp = (
            str(int(c.request_timeout.total_seconds() * 1000))
            if c.request_timeout is not None
            else ""
        )

        return build_routing_fingerprint(
            public=[c.app_id, str(c.is_production), timeout_fp],
            secret=[c.event_key, c.signing_key],
        )

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: InngestRoutingCredentials,
    ) -> InngestClient:
        return InngestClient(app_id=creds.app_id, config=_to_inngest_config(creds))

    # ....................... #

    @property
    def native(self) -> inngest.Inngest:
        self._pool.require_started()

        tenant_id = self._require_tenant_id()
        inner = self._peek_client(tenant_id)

        if inner is None:
            raise exc.internal(
                "Routed Inngest inner client is not initialized for this tenant; "
                "call send or warm the client first.",
            )

        return inner.native

    # ....................... #

    async def send(
        self,
        events: inngest.Event | list[inngest.Event],
    ) -> list[str]:
        inner = await self._get_client()
        return await inner.send(events)

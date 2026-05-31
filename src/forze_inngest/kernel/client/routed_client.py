"""Inngest client that resolves app credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from typing import Callable, Mapping, final
from uuid import UUID

import attrs
import inngest
from pydantic import SecretStr

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.exceptions import exc
from forze.base.primitives.fingerprint import (
    secret_dedup_fingerprint,
    stable_fingerprint,
)

from .client import InngestClient
from .config import InngestConfig
from .port import InngestClientPort
from .routing_credentials import InngestRoutingCredentials

# ----------------------- #


async def _dispose_inngest_client(_client: InngestClient) -> None:
    return None


# ....................... #


def _to_inngest_config(creds: InngestRoutingCredentials) -> InngestConfig:
    event_key: str | None = None

    if creds.event_key is not None:
        event_key = (
            creds.event_key.get_secret_value()
            if isinstance(creds.event_key, SecretStr)
            else creds.event_key
        )

    signing_key: str | None = None

    if creds.signing_key is not None:
        signing_key = (
            creds.signing_key.get_secret_value()
            if isinstance(creds.signing_key, SecretStr)
            else creds.signing_key
        )

    return InngestConfig(
        event_key=event_key,
        signing_key=signing_key,
        is_production=creds.is_production,
        request_timeout=creds.request_timeout,
    )


# ....................... #


@final
@attrs.define(slots=True)
class RoutedInngestClient(InngestClientPort):
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

    __pool: TenantClientRegistry[InngestClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=_dispose_inngest_client,
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
            InngestRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Inngest",
        )

        timeout_fp = (
            str(int(creds.request_timeout.total_seconds() * 1000))
            if creds.request_timeout is not None
            else ""
        )

        return stable_fingerprint(
            creds.app_id,
            secret_dedup_fingerprint(creds.event_key),
            secret_dedup_fingerprint(creds.signing_key),
            str(creds.is_production),
            timeout_fp,
        )

    # ....................... #

    async def _create_client(self, tid: UUID) -> InngestClient:
        creds = await resolve_structured_for_tenant(
            InngestRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Inngest",
        )

        return InngestClient(app_id=creds.app_id, config=_to_inngest_config(creds))

    # ....................... #

    async def _get_client(self) -> InngestClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Inngest access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    @property
    def native(self) -> inngest.Inngest:
        self.__pool.require_started()

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Inngest access",
        )
        inner = self.__pool.peek(tenant_id)

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

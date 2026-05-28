"""Inngest client that resolves app credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

import hashlib
from typing import Callable, Mapping, final
from uuid import UUID

import attrs
import inngest
from pydantic import SecretStr

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy import require_tenant_id
from forze.base.exceptions import exc
from forze.base.primitives.fingerprint import stable_fingerprint
from forze.base.primitives.lru_registry import SimpleLruRegistry

from .client import InngestClient
from .config import InngestConfig
from .port import InngestClientPort
from .routing_credentials import InngestRoutingCredentials

# ----------------------- #


async def _dispose_inngest_client(_client: InngestClient) -> None:
    return None


# ----------------------- #


def _secret_fingerprint(value: str | SecretStr | None) -> str:
    if value is None:
        return ""

    raw = value.get_secret_value() if isinstance(value, SecretStr) else value
    return hashlib.sha256(raw.encode()).hexdigest()


def _to_inngest_config(creds: InngestRoutingCredentials) -> InngestConfig:
    cfg: InngestConfig = {}

    if creds.event_key is not None:
        cfg["event_key"] = (
            creds.event_key.get_secret_value()
            if isinstance(creds.event_key, SecretStr)
            else creds.event_key
        )

    if creds.signing_key is not None:
        cfg["signing_key"] = (
            creds.signing_key.get_secret_value()
            if isinstance(creds.signing_key, SecretStr)
            else creds.signing_key
        )

    if creds.is_production is not None:
        cfg["is_production"] = creds.is_production

    if creds.request_timeout_ms is not None:
        cfg["request_timeout_ms"] = creds.request_timeout_ms

    return cfg


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

    _registry: SimpleLruRegistry[UUID, InngestClient] = attrs.field(init=False)
    _fingerprints: dict[UUID, str] = attrs.field(factory=dict, init=False, repr=False)
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise exc.internal("max_cached_tenants must be at least 1")

        self._registry = SimpleLruRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=_dispose_inngest_client,
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

    async def _resolve_creds(self, tenant_id: UUID) -> InngestRoutingCredentials:
        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tenant_id)

        try:
            return await resolve_structured(
                self.secrets,
                ref,
                InngestRoutingCredentials,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve Inngest secret for tenant {tenant_id}: {e}",
            ) from e

    # ....................... #

    async def _ensure_fingerprint(self, tenant_id: UUID) -> str:
        cached = self._fingerprints.get(tenant_id)

        if cached is not None:
            return cached

        creds = await self._resolve_creds(tenant_id)
        fingerprint = stable_fingerprint(
            creds.app_id,
            _secret_fingerprint(creds.event_key),
        )
        self._fingerprints[tenant_id] = fingerprint

        return fingerprint

    # ....................... #

    async def _create_client(self, tid: UUID) -> InngestClient:
        creds = await self._resolve_creds(tid)
        return InngestClient(app_id=creds.app_id, config=_to_inngest_config(creds))

    # ....................... #

    async def _get_client(self) -> InngestClient:
        if not self._started:
            raise exc.internal("Routed Inngest client is not started")

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Inngest access",
        )
        await self._ensure_fingerprint(tenant_id)

        return await self._registry.get_or_create(tenant_id)

    # ....................... #

    @property
    def native(self) -> inngest.Inngest:
        if not self._started:
            raise exc.internal("Routed Inngest client is not started")

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Inngest access",
        )
        inner = self._registry.peek(tenant_id)

        if inner is None:
            raise exc.internal(
                "Routed Inngest inner client is not initialized for this tenant; "
                "call send or warm the client first.",
            )

        return inner.native

    async def send(
        self,
        events: inngest.Event | list[inngest.Event],
    ) -> list[str]:
        inner = await self._get_client()
        return await inner.send(events)

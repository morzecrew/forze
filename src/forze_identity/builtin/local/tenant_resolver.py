"""Tenant resolver backed by a static local identity config (demo/MVP only)."""

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.tenancy import TenantIdentity, TenantResolverPort
from forze.base.exceptions import exc

from .config import LocalIdentityConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalTenantResolver(TenantResolverPort):
    """Resolve tenant from principal using :class:`LocalIdentityConfig`.

    At most one tenant per principal. For production multi-tenant membership use
    :class:`~forze_identity.tenancy.adapters.resolver.TenantResolverAdapter`.
    """

    config: LocalIdentityConfig
    """Static principal → tenant mapping."""

    # ....................... #

    async def resolve_from_principal(
        self,
        principal_id: UUID,
        *,
        requested_tenant_id: UUID | None = None,
    ) -> TenantIdentity | None:
        tid = self.config.principal_tenants.get(principal_id)

        if tid is None:
            tid = self.config.default_tenant_id

        if tid is None:
            return None

        if requested_tenant_id is not None and requested_tenant_id != tid:
            raise exc.authentication(
                "Requested tenant does not match principal membership",
                code="tenant_mismatch",
            )

        return TenantIdentity(tenant_id=tid)

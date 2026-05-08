from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import final

import attrs
from fastapi import Request

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError

from .ports import TenantIdentityResolverPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenantIdentityResolver(TenantIdentityResolverPort):
    """Resolve the tenant identity from a request."""

    required: bool = False
    """Whether missing credentials should raise :class:`AuthenticationError`."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
        identity: AuthnIdentity | None,
    ) -> TenantIdentity | None:
        ten = ctx.tenant_resolver()
        tenant_identity: TenantIdentity | None = None

        if ten is not None and identity is not None:
            tenant_identity = await ten.resolve_from_principal(identity.principal_id)

        if self.required and tenant_identity is None:
            raise AuthenticationError(
                "Tenant identity is required",
                code="tenant_required",
            )

        return tenant_identity

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import final
from uuid import UUID

import attrs
from fastapi import Request

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.errors import AuthenticationError

from .ports import TenantIdentityCodecPort, TenantIdentityResolverPort

# ----------------------- #
#! TODO: review


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderTenantIdentityCodec(TenantIdentityCodecPort):
    """Decode tenant UUID from a header (optional request hint)."""

    header_name: str = "X-Tenant-Id"
    """HTTP header carrying tenant id as UUID string."""

    # ....................... #

    def decode(self, request: Request) -> TenantIdentity | None:
        raw = request.headers.get(self.header_name)

        if raw is None or not raw.strip():
            return None

        return TenantIdentity(tenant_id=UUID(raw.strip()))


# ....................... #
#! TODO: review


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenantIdentityResolver(TenantIdentityResolverPort):
    """Resolve tenant from authn-bound id, optional header hint, and resolver fallback."""

    required: bool = False
    """Whether missing tenant should raise :class:`AuthenticationError`."""

    hint_codec: TenantIdentityCodecPort | None = None
    """Optional codec for tenant hints on the request (e.g. ``X-Tenant-Id``)."""

    strict_tenant_sources: bool = False
    """When ``True``, disagreeing non-null tenant ids from sources raise."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
        identity: AuthnIdentity | None,
    ) -> TenantIdentity | None:
        hint = self.hint_codec.decode(request) if self.hint_codec is not None else None

        from_authn = (
            TenantIdentity(tenant_id=identity.tenant_id)
            if identity is not None and identity.tenant_id is not None
            else None
        )

        resolved = None
        ten = ctx.tenant_resolver()

        if ten is not None and identity is not None:
            resolved = await ten.resolve_from_principal(identity.principal_id)

        sources = [s for s in (from_authn, hint, resolved) if s is not None]

        if self.strict_tenant_sources and sources:
            ids = {s.tenant_id for s in sources}

            if len(ids) > 1:
                raise AuthenticationError(
                    "Conflicting tenant identities from credential, hint, and resolver",
                    code="tenant_conflict",
                )

        chosen = from_authn or hint or resolved

        if self.required and chosen is None:
            raise AuthenticationError(
                "Tenant identity is required",
                code="tenant_required",
            )

        return chosen

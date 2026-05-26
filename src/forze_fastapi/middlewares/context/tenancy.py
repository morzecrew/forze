from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import final
from uuid import UUID

import attrs
from fastapi import Request

from forze.application.contracts.authn import AuthnResult
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

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

        return TenantIdentity(
            tenant_id=_coerce_tenant_hint(raw, source=f"header {self.header_name!r}")
        )


# ....................... #
#! TODO: review


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenantIdentityResolver(TenantIdentityResolverPort):
    """Resolve tenant from an authenticated principal plus optional tenant hints.

    Request and issuer hints can only request or validate tenant scope. The
    authoritative tenant identity, when available, comes from the configured
    :class:`~forze.application.contracts.tenancy.TenantResolverPort`.
    """

    required: bool = False
    """Whether missing tenant should raise :class:`AuthenticationError`."""

    hint_codec: TenantIdentityCodecPort | None = None
    """Optional codec for tenant hints on the request (e.g. ``X-Tenant-Id``)."""

    # ....................... #

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
        authn: AuthnResult | None,
    ) -> TenantIdentity | None:
        hint = self.hint_codec.decode(request) if self.hint_codec is not None else None
        issuer_hint_id = _coerce_issuer_tenant_hint(authn)

        if (
            hint is not None
            and issuer_hint_id is not None
            and hint.tenant_id != issuer_hint_id
        ):
            raise exc.authentication(
                "Conflicting tenant identities from issuer and request hint",
                code="tenant_conflict",
            )

        requested_tenant_id = hint.tenant_id if hint is not None else issuer_hint_id
        resolved: TenantIdentity | None = None
        ten = ctx.tenancy.resolver()

        if ten is not None and authn is not None:
            resolved = await ten.resolve_from_principal(
                authn.identity.principal_id,
                requested_tenant_id=requested_tenant_id,
            )

            if requested_tenant_id is not None and resolved is None:
                raise exc.authentication(
                    "Requested tenant is not available for the authenticated principal",
                    code="tenant_conflict",
                )

        if self.required and resolved is None:
            raise exc.authentication(
                "Tenant identity is required",
                code="tenant_required",
            )

        return resolved


# ....................... #


def _coerce_tenant_hint(raw: str, *, source: str) -> UUID:
    try:
        return UUID(raw.strip())

    except ValueError as e:
        raise exc.authentication(
            f"Invalid tenant hint from {source}",
            code="invalid_tenant_hint",
        ) from e


def _coerce_issuer_tenant_hint(authn: AuthnResult | None) -> UUID | None:
    if authn is None or authn.issuer_tenant_hint is None:
        return None

    return _coerce_tenant_hint(authn.issuer_tenant_hint, source="credential issuer")

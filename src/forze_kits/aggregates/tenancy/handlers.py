"""Self-service handlers for the tenant selector (list memberships, switch active tenant)."""

from typing import Callable

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    IssuedTokens,
    TokenLifecyclePort,
)
from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import (
    TenantIdentity,
    TenantManagementPort,
    TenantResolverPort,
)
from forze.base.exceptions import exc

from ..authn.dto import AuthnTokenResponseDTO
from ..authn.handlers._utils import token_response_from_issued_tokens
from .dto import TenantListDTO, TenantListItemDTO, TenantSwitchRequestDTO

# ----------------------- #


def _require_identity(
    resolver: "Callable[[], AuthnIdentity | None]",
) -> AuthnIdentity:
    """Pull the bound identity or raise the uniform 401 (self-service guard)."""

    identity = resolver()

    if identity is None:
        raise exc.authentication("Authentication required", code="auth_required")

    return identity


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListTenants(Handler[None, TenantListDTO]):
    """Self-service: list the current principal's active tenant memberships."""

    resolver: Callable[[], AuthnIdentity | None]
    """Resolve the current authenticated identity."""

    current_tenant: Callable[[], TenantIdentity | None]
    """Resolve the tenant currently bound to the request (to flag ``is_current``)."""

    tenant_management: TenantManagementPort
    """Tenant management port (membership listing)."""

    async def __call__(self, args: None) -> TenantListDTO:
        _ = args

        identity = _require_identity(self.resolver)
        current = self.current_tenant()
        current_id = current.tenant_id if current is not None else None

        tenants = await self.tenant_management.list_principal_tenants(
            identity.principal_id
        )

        return TenantListDTO(
            tenants=[
                TenantListItemDTO(
                    tenant_id=t.tenant_id,
                    tenant_key=t.tenant_key,
                    is_current=t.tenant_id == current_id,
                )
                for t in tenants
            ]
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SwitchTenant(Handler[TenantSwitchRequestDTO, AuthnTokenResponseDTO]):
    """Self-service: activate a tenant — validate membership, then re-mint a scoped token."""

    resolver: Callable[[], AuthnIdentity | None]
    """Resolve the current authenticated identity."""

    tenant_resolver: TenantResolverPort
    """Authoritative tenant resolver (validates the request against membership)."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle used to re-mint a token scoped to the selected tenant."""

    async def __call__(self, args: TenantSwitchRequestDTO) -> AuthnTokenResponseDTO:
        identity = _require_identity(self.resolver)

        # The selection is a *request*; the resolver is the authority. This raises
        # ``tenant_mismatch`` (not a member) / ``tenant_inactive`` — never trust the id alone.
        await self.tenant_resolver.resolve_from_principal(
            identity.principal_id,
            requested_tenant_id=args.id,
        )

        tokens: IssuedTokens = await self.token_lifecycle.issue_tokens(
            identity,
            tenant_id=args.id,
        )

        return token_response_from_issued_tokens(tokens)

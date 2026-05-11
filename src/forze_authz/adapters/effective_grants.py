from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authz import (
    EffectiveGrants,
    EffectiveGrantsPort,
    PrincipalRef,
    coalesce_authz_tenant_id,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.errors import CoreError

from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import AuthzGrantResolver
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


def _principal_id(principal: PrincipalRef | UUID) -> UUID:
    return principal.principal_id if isinstance(principal, PrincipalRef) else principal


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveGrantsAdapter(EffectiveGrantsPort):
    """Resolve effective grants from catalog documents and binding edges."""

    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    resolver: AuthzGrantResolver

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_secure_authz_document_spec(self.principal_qry.spec)

        for qry in (
            self.resolver.deps.permission_qry,
            self.resolver.deps.role_qry,
            self.resolver.deps.group_qry,
            self.resolver.deps.rp_binding_qry,
            self.resolver.deps.pr_binding_qry,
            self.resolver.deps.pp_binding_qry,
            self.resolver.deps.gp_binding_qry,
            self.resolver.deps.gr_binding_qry,
            self.resolver.deps.gperm_binding_qry,
        ):
            validate_secure_authz_document_spec(qry.spec)

    # ....................... #

    async def resolve_effective_grants(
        self,
        principal: PrincipalRef | UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> EffectiveGrants:
        scope_tid = coalesce_authz_tenant_id(principal, tenant_id=tenant_id)
        pid = _principal_id(principal)
        row = await find_policy_principal_by_id(self.principal_qry, pid)

        if row is None:
            raise CoreError("Policy principal not found when resolving grants")

        return await self.resolver.resolve_effective_grants(pid, tenant_id=scope_tid)

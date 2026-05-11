from collections.abc import Mapping
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authz import (
    AuthzPort,
    PrincipalRef,
    coalesce_authz_tenant_id,
)
from forze.application.contracts.document import DocumentQueryPort

from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import AuthzGrantResolver
from ..services.policy import AuthzPolicyService
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


def _principal_id(principal: PrincipalRef | UUID) -> UUID:
    return principal.principal_id if isinstance(principal, PrincipalRef) else principal


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzAdapter(AuthzPort):
    """Permission checks using catalog-backed effective grants."""

    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    resolver: AuthzGrantResolver
    policy: AuthzPolicyService

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

    async def permits(
        self,
        principal: PrincipalRef | UUID,
        permission_key: str,
        *,
        tenant_id: UUID | None = None,
        resource: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        scope_tid = coalesce_authz_tenant_id(principal, tenant_id=tenant_id)
        pid = _principal_id(principal)
        row = await find_policy_principal_by_id(self.principal_qry, pid)

        if row is None or not row.is_active:
            return False

        grants = await self.resolver.resolve_effective_grants(pid, tenant_id=scope_tid)

        return self.policy.permits(
            grants,
            permission_key,
            resource=resource,
            context=context,
        )

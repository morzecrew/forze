from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzScope,
    AuthzSubject,
    EffectiveGrants,
    GrantQueryPort,
    PrincipalRef,
    resolve_policy_scope,
    subject_for_grant_query,
)
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import exc

from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import AuthzGrantResolver
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GrantQueryAdapter(GrantQueryPort):
    """Resolve effective grants from catalog documents and binding edges."""

    spec: AuthzSpec
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
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> EffectiveGrants:
        resolved_scope = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=scope.tenant_id if scope is not None else None,
        )
        pid = subject_for_grant_query(subject)
        row = await find_policy_principal_by_id(self.principal_qry, pid)

        if row is None:
            raise exc.internal("Policy principal not found when resolving grants")

        return await self.resolver.resolve_effective_grants(pid, scope=resolved_scope)


EffectiveGrantsAdapter = GrantQueryAdapter

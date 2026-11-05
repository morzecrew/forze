from collections.abc import Callable
from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authz import (
    AuthzDecision,
    AuthzDecisionPort,
    AuthzRequest,
    AuthzSpec,
    resolve_policy_scope,
)
from forze.application.contracts.document import DocumentQueryPort
from forze_identity.authn.services import admitted_on_epoch

from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import AuthzGrantResolver
from ..services.policy import AuthzPolicyService
from ._utils import find_policy_principal_by_id, validate_authz_query_ports

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzDecisionAdapter(AuthzDecisionPort):
    """Authorization decisions using catalog-backed effective grants.

    With :attr:`admission_window`, a decision about the principal whose eligibility read authn
    skipped after a fast-path admission, in the same task and outside a transaction, treats it
    as present and active without reading it: every write that may deactivate or remove it
    raises the epoch that admission was checked against.
    """

    spec: AuthzSpec
    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    resolver: AuthzGrantResolver
    policy: AuthzPolicyService

    admission_window: timedelta | None = None
    """How long after that admission the read is skipped: the revocation states' ``ttl``.
    ``None`` reads the principal on every decision."""

    in_transaction: Callable[[], bool] | None = None
    """Whether a transaction is open: it may have raised the epoch itself, which the cache sees
    only after commit, so a decision inside one reads. ``None`` reads on every decision."""

    def __attrs_post_init__(self) -> None:
        validate_authz_query_ports(
            self.spec,
            (
                self.principal_qry,
                self.resolver.deps.permission_qry,
                self.resolver.deps.role_qry,
                self.resolver.deps.group_qry,
                self.resolver.deps.rp_binding_qry,
                self.resolver.deps.pr_binding_qry,
                self.resolver.deps.pp_binding_qry,
                self.resolver.deps.gp_binding_qry,
                self.resolver.deps.gr_binding_qry,
                self.resolver.deps.gperm_binding_qry,
            ),
        )

    async def authorize(self, request: AuthzRequest) -> AuthzDecision:
        scope = resolve_policy_scope(
            spec=self.spec,
            explicit=request.scope,
            invocation_tenant_id=request.scope.tenant_id,
        )
        pid = request.subject.principal_id

        if self._admitted_on_epoch(pid):
            active = True

        else:
            row = await find_policy_principal_by_id(self.principal_qry, pid)

            if row is None:
                return AuthzDecision(allowed=False, reason="Policy principal not found")

            active = row.is_active if self.spec.enforce_principal_active else True

        grants = await self.resolver.resolve_effective_grants(
            pid,
            scope=scope,
        )

        ctx = dict(request.context)
        ctx.setdefault("subject_id", str(request.subject.principal_id))

        enriched = attrs.evolve(request, scope=scope, context=ctx)

        return self.policy.decide(grants, enriched, principal_active=active)

    # ....................... #

    def _admitted_on_epoch(self, principal_id: UUID) -> bool:
        return (
            self.admission_window is not None
            and self.in_transaction is not None
            and not self.in_transaction()
            and admitted_on_epoch(principal_id, within=self.admission_window)
        )

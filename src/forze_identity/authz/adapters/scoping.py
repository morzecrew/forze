"""Data-scoping adapter for document-backed authorization."""

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authz import (
    AuthzDecision,
    AuthzDocumentScope,
    AuthzDocumentScopeRequest,
    AuthzRequest,
    AuthzResource,
    AuthzScopePort,
    AuthzSensitiveAccessRequest,
    AuthzSpec,
    resolve_policy_scope,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.querying import QueryFilterExpression

from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import AuthzGrantResolver
from ..services.policy import AuthzPolicyService
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


def _tenant_filter(tenant_id: UUID) -> QueryFilterExpression:
    return {"$values": {"tenant_id": tenant_id}}


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzScopeAdapter(AuthzScopePort):
    """Derive query constraints and sensitive-resource checks from grants + scope."""

    spec: AuthzSpec
    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    resolver: AuthzGrantResolver
    policy: AuthzPolicyService

    def __attrs_post_init__(self) -> None:
        validate_secure_authz_document_spec(self.principal_qry.spec)

    async def _decide_operation(
        self,
        request: AuthzDocumentScopeRequest,
        *,
        action: str,
    ) -> AuthzDecision:
        scope = resolve_policy_scope(
            spec=self.spec,
            explicit=request.scope,
            invocation_tenant_id=request.scope.tenant_id,
        )
        pid = request.subject.principal_id
        row = await find_policy_principal_by_id(self.principal_qry, pid)

        if row is None:
            return AuthzDecision(allowed=False, reason="Policy principal not found")

        grants = await self.resolver.resolve_effective_grants(pid, scope=scope)

        auth_request = AuthzRequest(
            subject=request.subject,
            action=action,
            scope=scope,
            context={
                "document_name": request.document_name,
                "operation": request.operation,
                "subject_id": str(request.subject.principal_id),
            },
        )

        return self.policy.decide(
            grants,
            auth_request,
            principal_active=row.is_active if self.spec.enforce_principal_active else True,
        )

    async def scope_document(
        self,
        request: AuthzDocumentScopeRequest,
    ) -> AuthzDocumentScope:
        action = request.action or f"{request.document_name}.{request.operation}"
        decision = await self._decide_operation(request, action=action)

        if not decision.allowed:
            return AuthzDocumentScope(
                filters=None,
                deny_all=True,
                reason=decision.reason,
            )

        scope = resolve_policy_scope(
            spec=self.spec,
            explicit=request.scope,
            invocation_tenant_id=request.scope.tenant_id,
        )

        extra: QueryFilterExpression | None = None

        if scope.tenant_id is not None:
            extra = _tenant_filter(scope.tenant_id)

        if request.base_filters is None:
            return AuthzDocumentScope(filters=extra)

        if extra is None:
            return AuthzDocumentScope(filters=request.base_filters)

        return AuthzDocumentScope(
            filters={"$and": [request.base_filters, extra]},
        )

    async def authorize_sensitive_resource(
        self,
        request: AuthzSensitiveAccessRequest,
    ) -> bool:
        scope = resolve_policy_scope(
            spec=self.spec,
            explicit=request.scope,
            invocation_tenant_id=request.scope.tenant_id,
        )
        pid = request.subject.principal_id
        row = await find_policy_principal_by_id(self.principal_qry, pid)

        if row is None:
            return False

        grants = await self.resolver.resolve_effective_grants(pid, scope=scope)

        auth_request = AuthzRequest(
            subject=request.subject,
            action=request.action,
            scope=scope,
            resource=AuthzResource(
                resource_type=request.resource_type,
                resource_id=request.resource_id,
            ),
            context={"subject_id": str(request.subject.principal_id)},
        )

        return self.policy.decide(
            grants,
            auth_request,
            principal_active=row.is_active if self.spec.enforce_principal_active else True,
        ).allowed

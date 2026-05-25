"""Wire authz into :class:`~forze.application.execution.registry.OperationRegistry` plans."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, final

import attrs

from forze.application.contracts.authz import (
    AuthzDocumentScopeRequest,
    AuthzRequest,
    AuthzResource,
    AuthzScope,
    AuthzSpec,
    subject_from_authn,
)
from forze.application.contracts.execution import BeforeStep, MiddlewareStep
from forze.application.contracts.execution.protocols import (
    Before,
    BeforeFactory,
    Middleware,
    MiddlewareFactory,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution.context import ExecutionContext
from forze.base.errors import AuthorizationError
from forze.domain.models import BaseDTO

# ----------------------- #


def policy_scope_from_invocation(ctx: ExecutionContext) -> AuthzScope:
    """Build :class:`AuthzScope` from the bound invocation tenant, if any."""

    tenant = ctx.inv.get_tenant()

    if tenant is None:
        return AuthzScope()

    return AuthzScope(tenant_id=tenant.tenant_id)


# ....................... #


def merge_query_filters(
    base: QueryFilterExpression | None,  # type: ignore[valid-type]
    extra: QueryFilterExpression | None,  # type: ignore[valid-type]
) -> QueryFilterExpression | None:  # type: ignore[valid-type]
    """Merge two query filter expressions with ``$and`` when both are present."""

    if base is None:
        return extra

    if extra is None:
        return base

    return {"$and": [base, extra]}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class AuthzBeforeAuthorize(BeforeFactory):
    """Before-hook factory that calls :meth:`AuthzDecisionPort.authorize`."""

    spec: AuthzSpec
    action: str
    resource_factory: Callable[[ExecutionContext, Any], AuthzResource | None] | None = (
        None
    )
    context_factory: Callable[[ExecutionContext, Any], dict[str, Any]] | None = None

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        decision_port = ctx.authz.decision(self.spec)

        async def _before(args: Any) -> None:
            identity = ctx.inv.get_authn()

            if identity is None:
                raise AuthorizationError(
                    "Authentication required",
                    code="principal_required",
                )

            resource = (
                self.resource_factory(ctx, args) if self.resource_factory else None
            )
            context = self.context_factory(ctx, args) if self.context_factory else {}

            request = AuthzRequest(
                subject=subject_from_authn(identity),
                action=self.action,
                scope=policy_scope_from_invocation(ctx),
                resource=resource,
                context=context,
            )
            result = await decision_port.authorize(request)

            if not result.allowed:
                raise AuthorizationError(
                    result.reason or f"Permission denied: {self.action!r}",
                    code="permission_denied",
                )

        return _before

    # ....................... #

    def to_before_step(
        self,
        *,
        step_id: str,
        requires: tuple[str, ...] = ("authn.principal",),
        priority: int = 50,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            priority=priority,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class AuthzDocumentScopeWrap(MiddlewareFactory):
    """Wrap middleware factory that injects policy filters into list-style request DTOs."""

    spec: AuthzSpec
    document_name: str
    operation: str
    action: str | None = None
    args_filter_attr: str = "filters"

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        scope_port = ctx.authz.scope(self.spec)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            identity = ctx.inv.get_authn()

            if identity is None:
                raise AuthorizationError(
                    "Authentication required",
                    code="principal_required",
                )

            base_filters = getattr(args, self.args_filter_attr, None)
            scope_req = AuthzDocumentScopeRequest(
                subject=subject_from_authn(identity),
                scope=policy_scope_from_invocation(ctx),
                document_name=self.document_name,
                operation=self.operation,
                action=self.action,
            )
            doc_scope = await scope_port.scope_document(scope_req)

            if doc_scope.deny_all:
                raise AuthorizationError(
                    doc_scope.reason or "Access denied by policy scope",
                    code="scope_denied",
                )

            if doc_scope.filters is not None and hasattr(args, self.args_filter_attr):
                merged = merge_query_filters(base_filters, doc_scope.filters)

                if isinstance(args, BaseDTO):
                    args = args.model_copy(update={self.args_filter_attr: merged})
                else:
                    args = attrs.evolve(args, **{self.args_filter_attr: merged})  # type: ignore[arg-type]

            return await next(args)

        return _wrap

    # ....................... #

    def to_middleware_step(
        self,
        *,
        step_id: str,
        priority: int = 40,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep` using this factory."""

        return MiddlewareStep(
            id=step_id,
            factory=self,
            priority=priority,
        )


# ....................... #
# Convenience assemblers


def authorize_before_step(
    *,
    step_id: str,
    spec: AuthzSpec,
    action: str,
    requires: tuple[str, ...] = ("authn.principal",),
    priority: int = 50,
    resource_factory: (
        Callable[[ExecutionContext, Any], AuthzResource | None] | None
    ) = None,
    context_factory: Callable[[ExecutionContext, Any], dict[str, Any]] | None = None,
) -> BeforeStep:
    """Ready-made :class:`BeforeStep` for operation-level permission checks."""

    return AuthzBeforeAuthorize(
        spec=spec,
        action=action,
        resource_factory=resource_factory,
        context_factory=context_factory,
    ).to_before_step(
        step_id=step_id,
        requires=requires,
        priority=priority,
    )


def document_scope_wrap_step(
    *,
    step_id: str,
    spec: AuthzSpec,
    document_name: str,
    operation: str,
    action: str | None = None,
    priority: int = 40,
) -> MiddlewareStep:
    """Ready-made :class:`MiddlewareStep` for document list/search scope injection."""

    return AuthzDocumentScopeWrap(
        spec=spec,
        document_name=document_name,
        operation=operation,
        action=action,
    ).to_middleware_step(step_id=step_id, priority=priority)

"""Wire authz into :class:`~forze.application.execution.registry.OperationRegistry` plans."""

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
from forze.application.contracts.execution import (
    Before,
    BeforeFactory,
    BeforeStep,
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.domain.models import BaseDTO

# ----------------------- #


def policy_scope_from_invocation(ctx: ExecutionContext) -> AuthzScope:
    """Build :class:`AuthzScope` from the bound invocation tenant, if any."""

    tenant = ctx.inv_ctx.get_tenant()

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
@attrs.define(slots=True, kw_only=True, frozen=True)
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
            identity = ctx.inv_ctx.get_authn()

            if identity is None:
                raise exc.authorization(
                    "Authentication required",
                    code="auth_required",
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
                raise exc.authorization(
                    result.reason or f"Permission denied: {self.action!r}",
                    code="permission_denied",
                )

        return _before

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey,
        requires: tuple[StrKey, ...] = ("authn.principal",),
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 50,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return BeforeStep(
            id=step_id,
            factory=self,
            requires=requires,
            depends_on=depends_on,
            priority=priority,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
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
            identity = ctx.inv_ctx.get_authn()

            if identity is None:
                raise exc.authorization(
                    "Authentication required",
                    code="auth_required",
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
                raise exc.authorization(
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

    def to_step(
        self,
        *,
        step_id: StrKey,
        priority: int = 40,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep` using this factory."""

        return MiddlewareStep(id=step_id, factory=self, priority=priority)

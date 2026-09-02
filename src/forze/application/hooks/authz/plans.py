"""Wire authz into :class:`~forze.application.execution.operations.registry.OperationRegistry` plans."""

from collections.abc import Awaitable, Callable, Collection
from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application._logger import logger
from forze.application.contracts.authz import (
    AuthzDocumentScopeRequest,
    AuthzRequest,
    AuthzResource,
    AuthzScope,
    AuthzSpec,
    subject_from_authn,
)
from forze.application.contracts.base import AbstentionReason
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

from .._base import required_guard_step

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


def _empty_unexplained_page(result: Any) -> bool:
    """A page-like result (has ``hits`` and ``abstention``) with no hits and no reason yet."""

    hits = getattr(result, "hits", None)

    return hits == [] and getattr(result, "abstention", ...) is None


def _stamp_abstention(result: Any, reason: AbstentionReason) -> Any:
    if isinstance(result, BaseModel):
        return result.model_copy(update={"abstention": reason})

    return attrs.evolve(result, abstention=reason)


def _existence_probe_args(args: Any, base_filters: Any, filter_attr: str) -> Any | None:
    """Rebuild ``args`` as a smallest-possible first-page read of the caller's own filters.

    Returns ``None`` when no safe probe can be built: the probe must be clamped to one
    row (a ``size`` field) or it would materialize the unscoped result set the policy
    filter exists to prevent.
    """

    if isinstance(args, BaseModel):
        fields: Collection[str] = type(args).model_fields.keys()

        def rebuild(update: dict[str, Any]) -> Any:
            return args.model_copy(update=update)

    elif attrs.has(type(args)):
        fields = {f.name for f in attrs.fields(type(args))}

        def rebuild(update: dict[str, Any]) -> Any:
            return attrs.evolve(args, **update)

    else:
        return None

    if "size" not in fields:
        return None

    update: dict[str, Any] = {filter_attr: base_filters, "size": 1}

    for field, first_page_value in (("page", 1), ("after", None), ("before", None)):
        if field in fields:
            update[field] = first_page_value

    return rebuild(update)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzBeforeAuthorize(BeforeFactory):
    """Before-hook factory that calls :meth:`AuthzDecisionPort.authorize`."""

    spec: AuthzSpec
    action: str
    resource_factory: Callable[[ExecutionContext, Any], AuthzResource | None] | None = None
    context_factory: Callable[[ExecutionContext, Any], dict[str, Any]] | None = None

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        decision_port = ctx.authz.decision(self.spec)
        # Resolve eagerly when enforcement is on so a missing wiring fails loud at hook
        # build (per execution) instead of silently skipping the may_act check at runtime.
        delegation_port = (
            ctx.authz.delegation(self.spec) if self.spec.enforce_delegation_grant else None
        )

        async def _before(args: Any) -> None:
            identity = ctx.inv_ctx.get_authn()

            if identity is None:
                raise exc.authentication(
                    "Authentication required",
                    code="auth_required",
                )

            resource = self.resource_factory(ctx, args) if self.resource_factory else None
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

            # Delegation (on-behalf-of): walk the actor chain. Each actor must be
            # *independently* permitted the same action, so a delegated call can never exceed
            # intersect(subject grants, actor grants) — the confused-deputy defense. When the
            # route enforces delegation grants, each actor must additionally hold an explicit
            # may_act grant for the principal it acts for.
            node = request.subject

            while node.actor is not None:
                actor = node.actor
                actor_result = await decision_port.authorize(attrs.evolve(request, subject=actor))

                if not actor_result.allowed:
                    raise exc.authorization(
                        actor_result.reason or f"Delegate not permitted: {self.action!r}",
                        code="delegate_denied",
                    )

                if delegation_port is not None:
                    granted = await delegation_port.may_act(
                        actor.principal_id,
                        node.principal_id,
                        scope=request.scope,
                    )

                    if not granted:
                        raise exc.authorization(
                            f"Delegation not granted: {actor.principal_id} may not act "
                            f"on behalf of {node.principal_id}",
                            code="delegation_not_granted",
                        )

                node = actor

        return _before

    # ....................... #

    def permission_keys(self) -> tuple[str, ...]:
        """Marker (``DeclaresAuthz``): the permission key this hook enforces.

        Surfaced on the operation catalog as ``required_permissions``. Declared-hook
        introspection only — handlers may enforce further checks invisibly.
        """

        return (self.action,)

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

        return required_guard_step(
            self,
            step_id=step_id,
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

    explain_empty: bool = False
    """When on, an empty page result carries an abstention reason instead of a bare
    empty page. If the policy added no filters, empty means ``no_match`` outright.
    If it did, one probe re-invokes the inner chain with the caller's own filters,
    clamped to a single first-page row — rows there mean ``not_permitted``, none mean
    ``no_match``. The probe's rows never reach the caller, only whether any exist; it
    runs the inner middlewares and handler a second time, so it fires only when the
    invocation is bound read-only (a ``QUERY``-classified operation) — on any other
    operation, and when the args shape offers no ``size`` field to clamp or the probe
    itself fails, the page is returned without a reason rather than with a guess."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        scope_port = ctx.authz.scope(self.spec)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            identity = ctx.inv_ctx.get_authn()

            if identity is None:
                raise exc.authentication(
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

            if doc_scope.filters is not None:
                if not hasattr(args, self.args_filter_attr):
                    raise exc.configuration(
                        f"AuthzDocumentScopeWrap for document "
                        f"{self.document_name!r} (operation {self.operation!r}) "
                        f"received policy filters, but args of type "
                        f"{type(args).__name__!r} has no "
                        f"{self.args_filter_attr!r} attribute to apply them to",
                    )

                merged = merge_query_filters(base_filters, doc_scope.filters)

                if isinstance(args, BaseDTO):
                    args = args.model_copy(update={self.args_filter_attr: merged})
                else:
                    args = attrs.evolve(args, **{self.args_filter_attr: merged})  # type: ignore[arg-type]

            result = await next(args)

            if not self.explain_empty or not _empty_unexplained_page(result):
                return result

            if doc_scope.filters is None:
                # The policy restricted nothing, so the caller's own query matched nothing.
                return _stamp_abstention(result, "no_match")

            if not ctx.inv_ctx.is_read_only():
                # The probe re-invokes the inner chain, and only a QUERY invocation is
                # known read-only — never replay a handler that may write.
                return result

            probe_args = _existence_probe_args(args, base_filters, self.args_filter_attr)

            if probe_args is None:
                return result

            try:
                probe = await next(probe_args)
            except Exception:
                # The reason is advisory; a probe failure must not outrank the real result.
                logger.exception(
                    "Abstention existence probe failed for document %s (operation %s); "
                    "returning the empty page without a reason",
                    self.document_name,
                    self.operation,
                )
                return result

            reason: AbstentionReason = "not_permitted" if getattr(probe, "hits", []) else "no_match"

            return _stamp_abstention(result, reason)

        return _wrap

    # ....................... #

    def permission_keys(self) -> tuple[str, ...]:
        """Marker (``DeclaresAuthz``): the explicit action key, when one is set.

        Empty when no ``action`` is configured — the wrap still scopes (and may deny)
        access via the policy's document scoping, it just declares no named permission.
        Declared-hook introspection only, not a security statement.
        """

        return (self.action,) if self.action is not None else ()

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey,
        priority: int = 40,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep` using this factory."""

        return MiddlewareStep(id=step_id, factory=self, priority=priority)

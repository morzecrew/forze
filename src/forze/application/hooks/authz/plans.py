"""Wire authz into :class:`~forze.application.execution.operations.registry.OperationRegistry` plans."""

from collections.abc import Awaitable, Callable, Collection, Mapping
from typing import Any, cast, final

import attrs
from pydantic import BaseModel

from forze.application._logger import logger
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzDecisionPort,
    AuthzDocumentScopeRequest,
    AuthzRequest,
    AuthzResource,
    AuthzScope,
    AuthzScopePort,
    AuthzSpec,
    DelegationPort,
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
from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchMemberGrant,
    bind_search_member_grant,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import MappingConverter, StrKey
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

    elif attrs.has(type(args)):  # pyright: ignore[reportUnknownArgumentType]
        fields = {f.name for f in attrs.fields(type(args))}  # pyright: ignore[reportUnknownArgumentType]

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


def _authenticated(ctx: ExecutionContext) -> AuthnIdentity:
    identity = ctx.inv_ctx.get_authn()

    if identity is None:
        raise exc.authentication(
            "Authentication required",
            code="auth_required",
        )

    return identity


async def authorize_action(
    ctx: ExecutionContext,
    decision_port: AuthzDecisionPort,
    action: str,
    *,
    delegation_port: DelegationPort | None,
    resource: AuthzResource | None = None,
    context: Mapping[str, Any] | None = None,
) -> None:
    """Authorize *action* for the invocation's identity, and every actor it is delegated to.

    The check :class:`AuthzBeforeAuthorize` makes, callable where no operation hook guards the
    call (a transport dependency, say), so both refuse the same callers with the same denial.

    :raises CoreException: ``authentication`` (``auth_required``) without an identity;
        ``authorization`` — ``permission_denied`` for the subject, ``delegate_denied`` for an actor,
        ``delegation_not_granted`` when *delegation_port* holds no ``may_act`` grant. Pass the
        spec's delegation port when it enforces delegation grants; ``None`` asks no ``may_act``.
        A denial about *resource* names its type, so a non-disclosing posture can render it as
        not-found.
    """

    request = AuthzRequest(
        subject=subject_from_authn(_authenticated(ctx)),
        action=action,
        scope=policy_scope_from_invocation(ctx),
        resource=resource,
        context=dict(context) if context is not None else {},
    )
    result = await decision_port.authorize(request)
    # A denial about a resource names its type, so a non-disclosing posture can render it as
    # that type's not-found; an action-level denial (no resource) stays a 403.
    resource_type = resource.resource_type if resource is not None else None

    if not result.allowed:
        raise exc.authorization(
            result.reason or f"Permission denied: {action!r}",
            code="permission_denied",
            resource_type=resource_type,
        )

    # Delegation (on-behalf-of): walk the actor chain. Each actor must be *independently*
    # permitted the same action, so a delegated call can never exceed intersect(subject grants,
    # actor grants) — the confused-deputy defense. When the route enforces delegation grants,
    # each actor must additionally hold an explicit may_act grant for the principal it acts for.
    node = request.subject

    while node.actor is not None:
        actor = node.actor
        actor_result = await decision_port.authorize(attrs.evolve(request, subject=actor))

        if not actor_result.allowed:
            raise exc.authorization(
                actor_result.reason or f"Delegate not permitted: {action!r}",
                code="delegate_denied",
                resource_type=resource_type,
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
            # Before the factories run: an anonymous caller is refused before anything a factory
            # reads or raises can answer it.
            _authenticated(ctx)

            await authorize_action(
                ctx,
                decision_port,
                self.action,
                delegation_port=delegation_port,
                resource=self.resource_factory(ctx, args) if self.resource_factory else None,
                context=self.context_factory(ctx, args) if self.context_factory else None,
            )

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
        step_id: StrKey = "authz.authorize",
        requires: tuple[StrKey, ...] = ("authn.principal",),
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 50,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory.

        It requires the ``authn.principal`` capability that
        :meth:`~forze.application.hooks.authn.AuthnRequired.to_step` provides, so it runs after
        authentication; a plan with no such step refuses to freeze. Pass ``requires=()`` to
        authorize without one.
        """

        return required_guard_step(
            self,
            step_id=step_id,
            requires=requires,
            depends_on=depends_on,
            priority=priority,
        )


# ....................... #


async def _delegated_scope_filters(
    scope_port: AuthzScopePort,
    delegation_port: DelegationPort | None,
    request: AuthzDocumentScopeRequest,
    subject_filters: QueryFilterExpression | None,  # type: ignore[valid-type]
) -> QueryFilterExpression | None:  # type: ignore[valid-type]
    """The row filters for a delegated call: every principal in the chain, conjoined.

    As the before-hook guard does for the action, each actor is scoped *independently*, so
    a delegation never sees more than every principal in it could: an actor the policy
    denies refuses the call, and an actor's row filters narrow the subject's. When the spec
    enforces delegation grants, each actor must also hold a ``may_act`` grant for the
    principal it acts for.
    """

    parts = [subject_filters] if subject_filters is not None else []
    node = request.subject

    while node.actor is not None:
        actor = node.actor
        actor_scope = await scope_port.scope_document(attrs.evolve(request, subject=actor))

        if actor_scope.deny_all:
            raise exc.authorization(
                actor_scope.reason or "Delegate not permitted by policy scope",
                code="delegate_denied",
            )

        if delegation_port is not None and not await delegation_port.may_act(
            actor.principal_id, node.principal_id, scope=request.scope
        ):
            raise exc.authorization(
                f"Delegation not granted: {actor.principal_id} may not act on behalf of "
                f"{node.principal_id}",
                code="delegation_not_granted",
            )

        if actor_scope.filters is not None:
            parts.append(actor_scope.filters)

        node = actor

    if not parts:
        return None

    return parts[0] if len(parts) == 1 else {"$and": parts}


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
        # Resolved eagerly, as the before-hook guard does: a route enforcing delegation grants
        # with no delegation port wired fails when the hook is built, never open at runtime.
        delegation_port = (
            ctx.authz.delegation(self.spec) if self.spec.enforce_delegation_grant else None
        )

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

            policy_filters = await self._delegated_filters(
                scope_port, delegation_port, scope_req, doc_scope.filters
            )
            doc_scope = attrs.evolve(doc_scope, filters=policy_filters)

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

            # The reason is advisory, so nothing on the probe path — building the args
            # (attrs.evolve re-runs validators, which may reject the clamped values) or
            # running the read — may outrank the real result.
            try:
                probe_args = _existence_probe_args(args, base_filters, self.args_filter_attr)

                if probe_args is None:
                    return result

                probe = await next(probe_args)
            except Exception:
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

    _delegated_filters = staticmethod(_delegated_scope_filters)

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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzSearchMembers(MiddlewareFactory):
    """Wrap middleware factory that authorizes each member of a hub or federated search.

    Each member must pass its own read action, the check :class:`AuthzBeforeAuthorize` makes
    for one; a caller who may read none is refused. The result is bound as a
    :class:`~forze.application.contracts.search.SearchMemberGrant`, which the adapter applies
    to the request it receives, after any request mapper: a request naming a member the caller
    may not read (in ``members``, or in ``member_weights`` above zero) is refused with that
    member's denial, and one naming none skips such members, refused only when none remains.

    With :attr:`scope_operation` set (federated search only), each member is also scoped as
    :class:`AuthzDocumentScopeWrap` scopes one document, and its row filters reach its own leg
    alone. The scope request names the member's document from :attr:`scope_documents`, or the
    member's own name. A member the policy scope denies counts as one the caller may not read.

    Any authz-guarded hub or federated search operation needs this hook (or another
    :class:`~forze.application.contracts.execution.AuthorizesMembers` one): without it the
    registry refuses to freeze. Two such hooks on one operation, or on nested ones, both
    apply: their grants intersect.
    """

    spec: AuthzSpec
    search: HubSearchSpec[Any] | FederatedSearchSpec[Any]
    """The hub or federated search the operation runs."""

    actions: Mapping[str, str] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """The read action for each member, by member name. Names every member of :attr:`search`
    and nothing else. Map every member to the operation's own action to let that one
    permission read them all."""

    scope_operation: str | None = None
    """The ``operation`` of each member's policy scope request, or ``None`` to scope none."""

    scope_documents: Mapping[str, str] = attrs.field(
        factory=dict,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """The ``document_name`` of a member's policy scope request, by member name, where it is
    not the member's own name."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = {member.name for member in self.search.members}

        if missing := sorted(names - self.actions.keys()):
            raise exc.configuration(
                f"AuthzSearchMembers for {self.search.name!r} names no read action for "
                f"member(s) {missing}.",
            )

        if unknown := sorted(self.actions.keys() - names):
            raise exc.configuration(
                f"AuthzSearchMembers for {self.search.name!r} names action(s) for {unknown}, "
                "which it has no member for.",
            )

        if self.scope_operation is not None and isinstance(self.search, HubSearchSpec):
            raise exc.configuration(
                f"AuthzSearchMembers for hub {self.search.name!r} cannot scope its members: a "
                "hub filter applies to the hub rows, so scope those with AuthzDocumentScopeWrap.",
            )

        if self.scope_documents and self.scope_operation is None:
            raise exc.configuration(
                f"AuthzSearchMembers for {self.search.name!r} names scope documents but no "
                "scope_operation to scope them with.",
            )

        if unknown := sorted(self.scope_documents.keys() - names):
            raise exc.configuration(
                f"AuthzSearchMembers for {self.search.name!r} names scope documents for "
                f"{unknown}, which it has no member for.",
            )

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        decision_port = ctx.authz.decision(self.spec)
        # Resolved eagerly, as the other guards do: missing wiring fails when the hook is built.
        delegation_port = (
            ctx.authz.delegation(self.spec) if self.spec.enforce_delegation_grant else None
        )
        scope_port = ctx.authz.scope(self.spec) if self.scope_operation is not None else None

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            identity = _authenticated(ctx)
            allowed: set[str] = set()
            denials: dict[str, CoreException] = {}
            policy_filters: dict[str, Any] = {}

            # One decision per action, which the port answers alike for every member mapped to it.
            verdicts: dict[str, CoreException | None] = {}

            # Every member, not only those the request reaches: a request mapper runs after
            # this hook and may reach others, which the bound grant must answer for too.
            for member in self.search.members:
                name = member.name
                action = self.actions[name]

                try:
                    if action not in verdicts:
                        verdicts[action] = await self._decide(
                            ctx, decision_port, delegation_port, action
                        )

                    if (denial := verdicts[action]) is not None:
                        raise denial

                    if scope_port is not None:
                        filters = await self._member_filters(
                            ctx, scope_port, delegation_port, identity, name
                        )

                        if filters is not None:
                            policy_filters[name] = filters

                except CoreException as error:
                    if error.kind is not ExceptionKind.AUTHORIZATION:
                        raise

                    denials[name] = error

                else:
                    allowed.add(name)

            if not allowed:
                raise denials[self.search.members[0].name]

            grant = SearchMemberGrant(
                allowed=frozenset(allowed), denials=denials, filters=policy_filters
            )

            with bind_search_member_grant(self.search.name, grant):
                return await next(args)

        return _wrap

    # ....................... #

    @staticmethod
    async def _decide(
        ctx: ExecutionContext,
        decision_port: AuthzDecisionPort,
        delegation_port: DelegationPort | None,
        action: str,
    ) -> CoreException | None:
        """The denial of *action* for the caller, or ``None`` when it is allowed."""

        try:
            await authorize_action(ctx, decision_port, action, delegation_port=delegation_port)

        except CoreException as error:
            if error.kind is not ExceptionKind.AUTHORIZATION:
                raise

            return error

        return None

    # ....................... #

    async def _member_filters(
        self,
        ctx: ExecutionContext,
        scope_port: AuthzScopePort,
        delegation_port: DelegationPort | None,
        identity: AuthnIdentity,
        name: str,
    ) -> QueryFilterExpression | None:  # type: ignore[valid-type]
        """The policy's row filters for member *name*, or ``None`` when it restricts none.

        :raises CoreException: ``authorization`` (``scope_denied``) when the policy denies the
            member outright, or a delegated actor's denial.
        """

        request = AuthzDocumentScopeRequest(
            subject=subject_from_authn(identity),
            scope=policy_scope_from_invocation(ctx),
            document_name=self.scope_documents.get(name, name),
            operation=cast(str, self.scope_operation),
            action=self.actions[name],
        )
        doc_scope = await scope_port.scope_document(request)

        if doc_scope.deny_all:
            raise exc.authorization(
                doc_scope.reason or "Access denied by policy scope",
                code="scope_denied",
            )

        return await _delegated_scope_filters(
            scope_port, delegation_port, request, doc_scope.filters
        )

    # ....................... #

    def permission_keys(self) -> tuple[str, ...]:
        """Marker (``DeclaresAuthz``): every member's read action.

        A caller needs the action of each member it searches, not all of them. Declared-hook
        introspection only, not a security statement.
        """

        return tuple(sorted(set(self.actions.values())))

    # ....................... #

    def authorized_members(self) -> frozenset[str]:
        """Marker (``AuthorizesMembers``): every member of :attr:`search`."""

        return frozenset(self.actions)

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "authz.search_members",
        priority: int = 45,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep` using this factory."""

        return MiddlewareStep(id=step_id, factory=self, priority=priority)

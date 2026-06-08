from typing import Final
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import exc

from .specs import AuthzSpec
from .value_objects import AuthzScope, AuthzSubject, PrincipalRef

# ----------------------- #

MAX_DELEGATION_DEPTH: Final = 16
"""Backstop on the delegation (actor) chain length when converting or traversing it. Sits
above the token-derived cap (the orchestrator bounds RFC 8693 ``act`` chains lower); exceeding
it signals a malformed or abusive chain. No cycle check is needed — the identity value objects
are immutable, so an actor chain cannot reference itself."""


def resolve_policy_scope(
    *,
    spec: AuthzSpec,
    explicit: AuthzScope | None = None,
    invocation_tenant_id: UUID | None = None,
) -> AuthzScope:
    """Resolve effective policy scope for a decision or scoping call."""

    if explicit is not None and explicit.tenant_id is not None:
        scope = explicit

    elif invocation_tenant_id is not None:
        scope = AuthzScope(tenant_id=invocation_tenant_id)

    else:
        scope = AuthzScope()

    if spec.tenancy_mode == "require_invocation_tenant":
        if invocation_tenant_id is None:
            raise exc.internal(
                "AuthzSpec requires a bound tenant on the invocation context",
            )

        if scope.tenant_id is None:
            raise exc.internal(
                "AuthzSpec requires tenant_id on AuthzScope for this route",
            )

        if scope.tenant_id != invocation_tenant_id:
            raise exc.internal(
                "AuthzScope.tenant_id disagrees with invocation tenant",
            )

    return scope


# ....................... #


def subject_for_grant_query(
    principal: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
) -> UUID:
    """Normalize grant-query subject input to a principal id."""

    if isinstance(principal, UUID):
        return principal

    return principal.principal_id


# ....................... #


def subject_from_authn(identity: AuthnIdentity) -> AuthzSubject:
    """Build an :class:`AuthzSubject` from a bound :class:`AuthnIdentity`.

    Carries the delegation chain: a bound ``actor`` (the agent acting on behalf of the
    subject) becomes the subject's :attr:`AuthzSubject.actor`. Built iteratively and bounded
    by :data:`MAX_DELEGATION_DEPTH` so a pathologically deep chain is rejected rather than
    overflowing.
    """

    chain: list[AuthnIdentity] = []
    node: AuthnIdentity | None = identity

    while node is not None:
        if len(chain) >= MAX_DELEGATION_DEPTH:
            raise exc.precondition(
                f"Delegation chain exceeds the maximum depth ({MAX_DELEGATION_DEPTH}).",
                code="delegation_chain_too_deep",
            )

        chain.append(node)
        node = node.actor

    # Build from the innermost actor outward so each link's ``actor`` is already built.
    subject = AuthzSubject(principal_id=chain[-1].principal_id)

    for hop in reversed(chain[:-1]):
        subject = AuthzSubject(principal_id=hop.principal_id, actor=subject)

    return subject


def subject_from_principal_ref(principal: PrincipalRef) -> AuthzSubject:
    """Build an :class:`AuthzSubject` from a :class:`PrincipalRef`."""

    return AuthzSubject(
        principal_id=principal.principal_id,
        kind=principal.kind,
    )

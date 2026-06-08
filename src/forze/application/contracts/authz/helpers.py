from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import exc

from .specs import AuthzSpec
from .value_objects import AuthzScope, AuthzSubject, PrincipalRef

# ----------------------- #


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
    subject) becomes the subject's :attr:`AuthzSubject.actor`.
    """

    return AuthzSubject(
        principal_id=identity.principal_id,
        actor=subject_from_authn(identity.actor) if identity.actor is not None else None,
    )


def subject_from_principal_ref(principal: PrincipalRef) -> AuthzSubject:
    """Build an :class:`AuthzSubject` from a :class:`PrincipalRef`."""

    return AuthzSubject(
        principal_id=principal.principal_id,
        kind=principal.kind,
    )

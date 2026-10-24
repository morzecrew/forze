"""API-key authentication and principal → tenant resolution at the ingress.

Both mirror the HTTP edge, so a key authenticates identically on every surface: the key
runs through the same :class:`~forze.application.contracts.authn.AuthnPort` pipeline
(verifier, resolver, eligibility), and the tenant resolves from the principal, never from
an unverified hint.
"""

from uuid import UUID

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.tenancy import (
    TenantIdentity,
    coalesce_tenant_request_hints,
)
from forze.base.exceptions import exc

from ..context import ExecutionContext

# ----------------------- #


def api_key_credentials(raw: str) -> ApiKeyCredentials:
    """Split a ``prefix:secret`` key into credentials (mirrors the HTTP edge).

    A key with no ``:`` separator is taken whole as the secret (no prefix), so the same
    key authenticates over every surface.
    """

    head, _, tail = raw.strip().partition(":")

    if not tail:
        return ApiKeyCredentials(key=head)

    return ApiKeyCredentials(key=tail, prefix=head)


# ....................... #


async def authenticate_api_key(
    ctx: ExecutionContext,
    spec: AuthnSpec,
    raw: str,
) -> AuthnResult:
    """Authenticate the raw key *raw* through the authn port *spec* selects.

    The port is resolved before the key is checked, so a misconfigured spec or route
    raises as the deployment error it is, never as a bad credential.

    :raises CoreException: ``authentication`` for an unknown, expired or inactive key; any
        other kind for an infrastructure or configuration failure.
    """

    authn = ctx.authn.authn(spec)

    return await authn.authenticate_with_api_key(api_key_credentials(raw))


# ....................... #


async def resolve_principal_tenant(
    ctx: ExecutionContext,
    result: AuthnResult,
    *,
    route: str | None = None,
    requested: UUID | None = None,
) -> TenantIdentity | None:
    """Resolve the tenant for an authenticated principal (mirrors the HTTP edge).

    Prefers the tenancy resolver on *route* (principal → tenant, validating the request);
    without one, falls back to a tenant carried on the verified credential.

    :param ctx: The scope's execution context.
    :param result: The verified authentication result.
    :param route: The tenancy route the credential authenticated on.
    :param requested: A tenant the caller asks for. It is validated against the
        principal's membership, never trusted raw.
    :raises CoreException: ``authentication`` (``tenant_conflict``) when *requested*
        differs from the credential's tenant, or (``tenant_mismatch``) when the resolver
        finds no tenant for a principal *requested* names one; ``precondition``
        (``tenant_request_unverifiable``) when *requested* is given and no resolver can
        validate it; and whatever the resolver raises for a tenant outside the principal's
        membership.
    """

    hint = coalesce_tenant_request_hints(
        issuer_hint=result.issuer_tenant_hint,
        header_hint=str(requested) if requested is not None else None,
    )
    resolver = ctx.tenancy.resolver(route)

    if resolver is not None:
        resolved = await resolver.resolve_from_principal(
            result.identity.principal_id,
            requested_tenant_id=hint,
        )

        # A resolver with no binding for the principal returns None; a tenant the caller
        # asked for must then be refused, not dropped into a tenant-less call.
        if resolved is None and requested is not None:
            raise exc.authentication(
                "Requested tenant does not match principal membership",
                code="tenant_mismatch",
            )

        return resolved

    if requested is not None:
        raise exc.precondition(
            "A requested tenant needs a tenancy resolver to validate it against",
            code="tenant_request_unverifiable",
        )

    return TenantIdentity(tenant_id=hint) if hint is not None else None

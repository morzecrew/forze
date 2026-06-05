from typing import Sequence

from fastapi import Request

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnPort,
    AuthnResult,
)
from forze.application.contracts.tenancy import (
    TENANT_ID_HEADER,
    TenantIdentity,
    coalesce_tenant_request_hints,
    parse_tenant_hint,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc

from .value_objects import (
    AuthnIngress,
    CookieTokenAuthn,
    HeaderApiKeyAuthn,
    HeaderTokenAuthn,
)

# ----------------------- #


def _split_authorization(raw: str, sep: str = " ") -> tuple[str, str | None]:
    """Split an authorization-style header into ``(scheme, value)`` (or ``(value, None)``)."""

    parts: Sequence[str] = raw.strip(sep).split(maxsplit=1)

    if len(parts) == 1:
        return parts[0], None

    return parts[0], parts[1]


# ....................... #


async def _resolve_cookie_token_authn(
    ingress: CookieTokenAuthn,
    *,
    request: Request,
    ctx: ExecutionContext,
) -> AuthnResult | None:
    raw = request.cookies.get(ingress.cookie_name)

    if raw is None or not str(raw).strip():
        if ingress.required:
            raise exc.authentication("Authentication credentials are required")

        return None

    creds = AccessTokenCredentials(
        token=str(raw).strip(),
        scheme=ingress.scheme,
    )

    authn: AuthnPort = ctx.deps.resolve_configurable(
        ctx,
        AuthnDepKey,
        ingress.authn_spec,
        route=ingress.authn_spec.name,
    )

    return await authn.authenticate_with_token(creds)


# ....................... #


async def _resolve_header_token_authn(
    ingress: HeaderTokenAuthn,
    *,
    request: Request,
    ctx: ExecutionContext,
) -> AuthnResult | None:
    raw = request.headers.get(ingress.header_name)

    if raw is None or not str(raw).strip():
        if ingress.required:
            raise exc.authentication("Authentication credentials are required")

        return None

    scheme, token = _split_authorization(raw)

    if token is None:
        creds = AccessTokenCredentials(token=scheme)

    else:
        creds = AccessTokenCredentials(token=token, scheme=scheme)

    #! TODO: add authn convenient deps
    authn: AuthnPort = ctx.deps.resolve_configurable(
        ctx,
        AuthnDepKey,
        ingress.authn_spec,
        route=ingress.authn_spec.name,
    )

    return await authn.authenticate_with_token(creds)


# ....................... #


async def _resolve_header_api_key_authn(
    ingress: HeaderApiKeyAuthn,
    *,
    request: Request,
    ctx: ExecutionContext,
) -> AuthnResult | None:
    raw = request.headers.get(ingress.header_name)

    if raw is None or not str(raw).strip():
        if ingress.required:
            raise exc.authentication("Authentication credentials are required")

        return None

    prefix, key = _split_authorization(raw, sep=":")

    if key is None:
        creds = ApiKeyCredentials(key=prefix)

    else:
        creds = ApiKeyCredentials(key=key, prefix=prefix)

    #! TODO: add authn convenient deps
    authn: AuthnPort = ctx.deps.resolve_configurable(
        ctx,
        AuthnDepKey,
        ingress.authn_spec,
        route=ingress.authn_spec.name,
    )

    return await authn.authenticate_with_api_key(creds)


# ....................... #


async def resolve_authn_ingress(
    ingress: AuthnIngress,
    *,
    request: Request,
    ctx: ExecutionContext,
) -> AuthnResult | None:
    match ingress:
        case CookieTokenAuthn():
            return await _resolve_cookie_token_authn(
                ingress,
                request=request,
                ctx=ctx,
            )

        case HeaderTokenAuthn():
            return await _resolve_header_token_authn(
                ingress,
                request=request,
                ctx=ctx,
            )

        case HeaderApiKeyAuthn():
            return await _resolve_header_api_key_authn(
                ingress,
                request=request,
                ctx=ctx,
            )


# ....................... #


async def resolve_tenant_identity(
    authn: AuthnResult | None,
    *,
    request: Request,
    ctx: ExecutionContext,
    trust_tenant_header: bool = False,
) -> TenantIdentity | None:
    issuer_hint = authn.issuer_tenant_hint if authn is not None else None
    header_hint = request.headers.get(TENANT_ID_HEADER)
    requested = coalesce_tenant_request_hints(
        issuer_hint=issuer_hint,
        header_hint=header_hint,
    )

    ten = ctx.tenancy.resolver()

    if ten is not None and authn is not None:
        return await ten.resolve_from_principal(
            authn.identity.principal_id,
            requested_tenant_id=requested,
        )

    if requested is None:
        return None

    # No tenancy resolver validated the request. A tenant derived from a verified
    # credential (issuer hint) is trustworthy, but a tenant taken from the raw
    # ``X-Tenant-Id`` header is unauthenticated client input: an attacker could set
    # it to any tenant. Honor the header-only path only when the deployment has
    # explicitly opted in (e.g. it sits behind a gateway that sets the header).
    from_verified_credential = parse_tenant_hint(issuer_hint) is not None

    if from_verified_credential or trust_tenant_header:
        return TenantIdentity(tenant_id=requested)

    return None

"""Require a bound principal on the execution context."""

from collections.abc import Callable, Sequence
from typing import Any, cast, final

import attrs
from fastapi import Depends, HTTPException, Security
from fastapi.routing import APIRoute
from fastapi.params import Depends as DependsParam
from fastapi.security import APIKeyCookie, APIKeyHeader, HTTPBearer
from fastapi.security.base import SecurityBase

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.openapi.security import (
    openapi_api_key_cookie_scheme,
    openapi_api_key_header_scheme,
    openapi_http_bearer_scheme,
    openapi_operation_security,
)
from forze_fastapi.transport.http.auth import AuthnRequirement

from .base import Policy

# ----------------------- #


def _security_scheme_for_requirement(req: AuthnRequirement) -> SecurityBase:
    if req.token_header is not None:
        return HTTPBearer(
            scheme_name=req.scheme_name,
            bearerFormat=req.bearer_format,
            description=req.description,
            auto_error=False,
        )

    if req.token_cookie is not None:
        return APIKeyCookie(
            name=req.token_cookie,
            scheme_name=req.scheme_name,
            description=req.description,
            auto_error=False,
        )

    api_key_header = req.api_key_header
    if api_key_header is None:
        raise CoreError(
            "AuthnRequirement.api_key_header must be set when token transports are absent",
        )
    return APIKeyHeader(
        name=api_key_header,
        scheme_name=req.scheme_name,
        description=req.description,
        auto_error=False,
    )


def _security_scheme_fragment(req: AuthnRequirement) -> dict[str, dict[str, Any]]:
    if req.token_header is not None:
        return openapi_http_bearer_scheme(
            scheme_name=req.scheme_name,
            bearer_format=req.bearer_format,
            description=req.description,
        )

    if req.token_cookie is not None:
        return openapi_api_key_cookie_scheme(
            scheme_name=req.scheme_name,
            cookie_name=req.token_cookie,
            description=req.description,
        )

    api_key_header = req.api_key_header
    if api_key_header is None:
        raise CoreError(
            "AuthnRequirement.api_key_header must be set when token transports are absent",
        )
    return openapi_api_key_header_scheme(
        scheme_name=req.scheme_name,
        header_name=api_key_header,
        description=req.description,
    )


def _openapi_extra_for_requirement(req: AuthnRequirement) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    components: dict[str, Any] = {}
    security_schemes = dict(_security_scheme_fragment(req))
    components["securitySchemes"] = security_schemes
    extra["components"] = components
    op_security = openapi_operation_security(req.scheme_name)["security"]
    extra["security"] = list(op_security)
    return extra


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RequirePrincipal(Policy):
    """HTTP gate: require ``ctx.inv.get_authn()`` and document OpenAPI security.

    Credential extraction happens in
    :class:`~forze_fastapi.middlewares.context.ContextBindingMiddleware`;
    this policy only enforces presence and exports the matching security scheme.
    """

    requirement: AuthnRequirement
    ctx_dep: Callable[[], ExecutionContext]

    # ....................... #

    def route_dependencies(self) -> Sequence[Any]:
        security_scheme = cast(
            Callable[..., Any],
            _security_scheme_for_requirement(self.requirement),
        )

        ctx_dep = self.ctx_dep

        async def _enforce(
            _credentials: Any = Security(security_scheme),
            ctx: ExecutionContext = Depends(ctx_dep),
        ) -> None:
            if ctx.inv.get_authn() is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required",
                )

        return [Depends(_enforce)]

    # ....................... #

    def openapi_extra(self) -> dict[str, Any] | None:
        return _openapi_extra_for_requirement(self.requirement)

    # ....................... #

    def route_class(self) -> type[APIRoute] | None:
        return None


# ....................... #


def build_require_principal_dependency(
    requirement: AuthnRequirement,
    *,
    ctx_dep: Callable[[], ExecutionContext],
) -> DependsParam:
    """Build a FastAPI dependency equivalent to :class:`RequirePrincipal`."""

    policy = RequirePrincipal(requirement=requirement, ctx_dep=ctx_dep)
    deps = policy.route_dependencies()
    if len(deps) != 1:
        raise CoreError("RequirePrincipal must expose exactly one route dependency")
    return deps[0]

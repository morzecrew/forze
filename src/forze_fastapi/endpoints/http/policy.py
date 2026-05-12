"""Helpers for composing HTTP endpoint feature chains (auth guards, defaults)."""

from typing import Any, Callable, Sequence, TypeVar, cast

import attrs
from fastapi import Depends, HTTPException, Security
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

from .contracts.authn import AuthnRequirement
from .contracts.ports import HttpEndpointFeaturePort
from .contracts.specs import HttpEndpointSpec, HttpMetadataSpec
from .features.security import RequireAuthnFeature

# ----------------------- #

AnyFeature = HttpEndpointFeaturePort[Any, Any, Any, Any, Any, Any, Any, Any, Any]
"""Loosely typed feature (all type parameters free) for default feature bundles."""

_FSpec = TypeVar(
    "_FSpec", bound=HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any, Any]
)


# ....................... #


def merge_http_endpoint_features(
    *parts: Sequence[AnyFeature] | None,
) -> tuple[AnyFeature, ...] | None:
    """Concatenate feature sequences (``None`` or empty parts are skipped)."""

    out: list[AnyFeature] = []

    for p in parts:
        if p:
            out.extend(p)

    return tuple(out) if out else None


# ....................... #


def with_default_http_features(
    spec: _FSpec, default_features: Sequence[AnyFeature] | None
) -> _FSpec:
    """Return ``spec`` with ``default_features`` prepended to ``spec.features``."""

    if not default_features:
        return spec

    merged = merge_http_endpoint_features(tuple(default_features), spec.features)

    return attrs.evolve(spec, features=cast(Any, merged))


# ....................... #


def _security_scheme_fragment(req: AuthnRequirement) -> dict[str, dict[str, Any]]:
    """Build the matching ``components.securitySchemes`` fragment for ``req``."""

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


# ....................... #


def _merge_openapi_extra(
    metadata: HttpMetadataSpec | None,
    requirement: AuthnRequirement,
) -> HttpMetadataSpec:
    """Return ``metadata`` augmented with security scheme + operation security entries."""

    base: dict[str, Any] = dict(metadata) if metadata else {}
    extra = dict(base.get("openapi_extra", {}))

    components = dict(extra.get("components", {}))
    security_schemes = dict(components.get("securitySchemes", {}))
    security_schemes.update(_security_scheme_fragment(requirement))
    components["securitySchemes"] = security_schemes
    extra["components"] = components

    op_security = openapi_operation_security(requirement.scheme_name)["security"]
    existing = list(extra.get("security", []))
    existing.extend(op_security)
    extra["security"] = existing

    base["openapi_extra"] = extra
    return cast(HttpMetadataSpec, base)


# ....................... #


def apply_authn_requirement(
    spec: _FSpec,
    requirement: AuthnRequirement | None,
) -> _FSpec:
    """Augment an :class:`HttpEndpointSpec` with auth runtime gate and OpenAPI security.

    When ``requirement`` is ``None``, ``spec`` is returned unchanged. Otherwise:

    1. A :class:`RequireAuthnFeature` is prepended to ``spec.features``.
    2. ``spec.metadata.openapi_extra`` is augmented with both a
       ``components.securitySchemes`` entry and an operation-level ``security``
       requirement that references it.

    The helper is idempotent over feature lists (it does *not* deduplicate
    existing :class:`RequireAuthnFeature` entries — call once per requirement).
    """

    if requirement is None:
        return spec

    require_feature: AnyFeature = RequireAuthnFeature()
    merged_features = merge_http_endpoint_features((require_feature,), spec.features)

    new_metadata = _merge_openapi_extra(spec.metadata, requirement)

    return attrs.evolve(
        spec,
        features=cast(Any, merged_features),
        metadata=new_metadata,
    )


# ....................... #


def _security_scheme_for_requirement(req: AuthnRequirement) -> SecurityBase:
    """Build the matching FastAPI :class:`SecurityBase` instance for ``req``.

    The returned scheme is used purely for OpenAPI documentation (the operation
    is labeled with the matching padlock); credential extraction happens in
    :class:`~forze_fastapi.middlewares.context.ContextBindingMiddleware`, so
    ``auto_error`` is forced to :obj:`False` to keep the security class from
    short-circuiting the request before the resolver runs.
    """

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


# ....................... #


def build_authn_requirement_dependency(
    requirement: AuthnRequirement,
    *,
    ctx_dep: Callable[[], ExecutionContext],
) -> DependsParam:
    """Build a FastAPI :class:`Depends` from an :class:`AuthnRequirement`.

    Use this when wiring custom FastAPI routes/routers that should mirror the
    authentication surface produced by :func:`apply_authn_requirement` on
    Forze-built endpoints:

    .. code-block:: python

        router = APIRouter(
            dependencies=[
                build_authn_requirement_dependency(req, ctx_dep=ctx_dep),
            ],
        )

    The returned dependency:

    1. Declares the matching FastAPI security scheme (``HTTPBearer``,
       ``APIKeyCookie`` or ``APIKeyHeader``) so the operation is documented
       under ``components.securitySchemes`` with ``requirement.scheme_name``.
    2. Reads :class:`ExecutionContext` via ``ctx_dep`` and raises ``HTTP 401``
       when no :class:`~forze.application.contracts.authn.AuthnIdentity` is
       bound (i.e. when
       :class:`~forze_fastapi.middlewares.context.ContextBindingMiddleware`
       could not resolve credentials on the current request).

    .. note::

        Credential extraction itself happens in the binding middleware; the
        FastAPI security class attached here is purely for OpenAPI rendering.
        Routes guarded by this dependency therefore behave identically to
        Forze-built endpoints decorated via :func:`apply_authn_requirement`.

    :param requirement: Authentication contract the route advertises and enforces.
    :param ctx_dep: Dependency yielding the per-request :class:`ExecutionContext`.
    :returns: A :class:`fastapi.params.Depends` instance ready to plug into
        ``APIRouter(dependencies=[...])`` or per-route ``dependencies``.
    """

    security_scheme = cast(
        Callable[..., Any],
        _security_scheme_for_requirement(requirement),
    )

    async def _enforce(
        _credentials: Any = Security(security_scheme),
        ctx: ExecutionContext = Depends(ctx_dep),
    ) -> None:
        if ctx.get_authn_identity() is None:
            raise HTTPException(
                status_code=401,
                detail="Authentication required",
            )

    return Depends(_enforce)

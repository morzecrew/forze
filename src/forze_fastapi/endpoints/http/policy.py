"""Helpers for composing HTTP endpoint feature chains (auth guards, defaults)."""

from typing import Any, Sequence, TypeVar, cast

import attrs

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

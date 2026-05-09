"""Helpers for composing HTTP endpoint feature chains (auth guards, defaults)."""

from typing import Any, Sequence, TypeVar, cast

import attrs

from .contracts.ports import HttpEndpointFeaturePort
from .contracts.specs import HttpEndpointSpec

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

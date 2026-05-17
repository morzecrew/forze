from typing import Sequence

import attrs

from forze.base.primitives import StrKey

from ..middlewares import MiddlewareFactory

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to a middleware plan."""

    factory: MiddlewareFactory
    """Factory that builds the middleware."""

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    """Priority of the middleware."""

    requires: Sequence[StrKey] = attrs.field(factory=tuple)
    """Capabilities required by the middleware."""

    provides: Sequence[StrKey] = attrs.field(factory=tuple)
    """Capabilities provided by the middleware."""

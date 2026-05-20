from typing import Any, Callable, final

import attrs

from forze.base.primitives import StrKey

from ..core.factories import (
    BeforeFactory,
    FinallyFactory,
    MiddlewareFactory,
    OnFailureFactory,
    OnSuccessFactory,
)
from ..core.value_objects import GraphStep, Step

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareStep(Step):
    """Middleware step."""

    factory: MiddlewareFactory
    """Factory that builds the middleware."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FinallyStep(Step):
    """Finally step."""

    factory: FinallyFactory
    """Factory that builds the finally hook."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OnFailureStep(Step):
    """On failure step."""

    factory: OnFailureFactory
    """Factory that builds the on failure hook."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BeforeStep(GraphStep):
    """Before step."""

    factory: BeforeFactory
    """Factory that builds the before hook."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OnSuccessStep(GraphStep):
    """On success step."""

    factory: OnSuccessFactory
    """Factory that builds the on success hook."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchStep(Step):
    """Dispatch step."""

    target: StrKey
    """Target operation key."""

    mapper: Callable[[Any, Any], Any]
    """Mapper function to transform the result of the target operation."""

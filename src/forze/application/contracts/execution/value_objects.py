from typing import Any, Callable, final

import attrs

from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from .defaults import noop_lifecycle_hook
from .protocols import (
    BeforeFactory,
    FinallyFactory,
    LifecycleHook,
    MiddlewareFactory,
    OnFailureFactory,
    OnSuccessFactory,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Success[R]:
    """Base value object for result of successful execution."""

    value: R
    """Result of the successful execution."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Failure:
    """Base value object for result of failed execution."""

    exc: Exception
    """Exception that caused the failure."""


# ....................... #

type Outcome[R] = Success[R] | Failure
"""Union type for the result of execution."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionGraph[G]:
    """Execution graph."""

    steps: StrKeyMapping[G] = attrs.field(
        factory=dict[StrKey, G],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Steps for this graph."""

    waves: tuple[tuple[StrKey, ...], ...] = attrs.field(factory=tuple)
    """Waves for this graph."""

    # ....................... #

    def is_empty(self) -> bool:
        return not self.steps and not self.waves


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionPipeline[P]:
    """Execution pipeline."""

    steps: tuple[P, ...] = attrs.field(factory=tuple)
    """Steps for this pipeline."""

    # ....................... #

    def is_empty(self) -> bool:
        return not self.steps


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Step:
    """Basic step."""

    id: StrKey
    """Unique identifier for the step."""

    priority: int = 0
    """Priority of the step."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphStep(Step):
    """Basic graph step."""

    requires: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Capabilities required by this step."""

    provides: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Capabilities provided by this step."""

    depends_on: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Other steps IDs this step depends on."""


# ....................... #


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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LifecycleStep(GraphStep):
    """Lifecycle step.

    ``shutdown`` may be invoked at most once per successful ``startup`` and never
    without it (guaranteed by the runtime).
    """

    startup: LifecycleHook = noop_lifecycle_hook
    """Startup hook."""

    shutdown: LifecycleHook = noop_lifecycle_hook
    """Shutdown hook."""

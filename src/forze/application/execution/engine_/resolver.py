from typing import TYPE_CHECKING, Any, Sequence

import attrs

from .scheduler import CapabilityScheduler

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

from forze.base.errors import CoreError

from ..middlewares import Guard, GuardMiddleware, OnSuccess, OnSuccessMiddleware
from .specs import MiddlewareSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedCapabilityStep:
    """A resolved capability step."""

    spec: MiddlewareSpec
    """Specification for the step."""

    runnable: OnSuccess[Any, Any] | Guard[Any]
    """Runnable middleware for the step."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityResolver:
    """Resolves capabilities for a given slot."""

    scheduler: CapabilityScheduler
    """Scheduler to use."""

    # ....................... #

    def resolve(self, ctx: "ExecutionContext") -> Sequence[ResolvedCapabilityStep]:
        """Resolve capabilities for the given slot."""

        out: list[ResolvedCapabilityStep] = []
        scheduled = self.scheduler.schedule()

        for spec in scheduled:
            mw = spec.factory(ctx)

            if not isinstance(mw, GuardMiddleware | OnSuccessMiddleware):
                raise CoreError(
                    f"Expected 'GuardMiddleware' or 'OnSuccessMiddleware' in capability slot {self.scheduler.slot!r}, got {type(mw)}",
                )

            out.append(ResolvedCapabilityStep(spec=spec, runnable=mw.inner))

        return out

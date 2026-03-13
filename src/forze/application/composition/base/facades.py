from typing import Any

import attrs

from forze.application.execution import (
    ExecutionContext,
    Usecase,
    UsecasePlan,
    UsecaseRegistry,
)
from forze.application.execution.plan import OpKey
from forze.base.logging import getLogger, log_section

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseUsecasesFacade:
    """Base usecases facade."""

    ctx: ExecutionContext
    """Execution context for resolving usecases."""

    reg: UsecaseRegistry
    """Registry with plan merged; used to resolve usecases."""

    # ....................... #

    def resolve(self, op: OpKey) -> Usecase[Any, Any]:
        """Resolve a usecase for the given operation."""

        logger.trace("%s: resolving operation '%s'", type(self).__qualname__, op)

        with log_section():
            return self.reg.resolve(op, self.ctx)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseUsecasesFacadeProvider[F: BaseUsecasesFacade]:
    """Factory that produces a base usecases facade for a given context."""

    reg: UsecaseRegistry
    """Base usecase registry."""

    plan: UsecasePlan
    """Plan to merge into the registry when building the facade."""

    facade: type[F]
    """Facade type to produce."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> F:
        """Build a base usecases facade for a given context."""

        logger.trace(
            "%s: building facade %s",
            type(self).__qualname__,
            self.facade.__qualname__,
        )

        with log_section():
            reg = self.reg.extend_plan(self.plan)
            return self.facade(ctx=ctx, reg=reg)

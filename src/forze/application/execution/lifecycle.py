"""Lifecycle hooks and plans for startup and shutdown."""

from typing import TYPE_CHECKING, Self, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleStep

if TYPE_CHECKING:
    from .context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecyclePlan:
    """Declarative plan for application lifecycle.

    Collects :class:`LifecycleStep` instances. :meth:`startup` runs in order;
    :meth:`shutdown` runs in reverse. On startup failure, already-executed
    steps are shut down before re-raising.
    """

    steps: tuple[LifecycleStep, ...] = attrs.field(factory=tuple)
    """Ordered sequence of lifecycle steps."""

    # ....................... #

    @classmethod
    def from_steps(cls, *steps: LifecycleStep) -> Self:
        """Create a plan from steps.

        :param steps: Steps to include.
        :returns: New plan instance.
        :raises CoreError: If step names collide.
        """

        logger.trace("Creating lifecycle plan from %s step(s)", len(steps))
        logger.trace("Steps: %s", tuple(step.id for step in steps))

        return cls(steps=steps)

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        """Return a new plan with additional steps appended.

        :param steps: Steps to append.
        :returns: New plan instance.
        :raises CoreError: If step names collide.
        """

        logger.trace(
            "Appending %s lifecycle step(s) to existing plan with %s step(s)",
            len(steps),
            len(self.steps),
        )

        logger.trace("Existing steps: %s", tuple(step.id for step in self.steps))
        logger.trace("New steps: %s", tuple(step.id for step in steps))

        new_steps = (*self.steps, *steps)

        return attrs.evolve(self, steps=new_steps)

    # ....................... #

    async def startup(self, ctx: "ExecutionContext") -> None:
        """Run startup hooks in order.

        On failure, runs shutdown for already-executed steps in reverse, then
        re-raises.
        """

        logger.trace("Running lifecycle startup with %s step(s)", len(self.steps))

        executed: list[LifecycleStep] = []

        try:
            for step in self.steps:
                logger.trace("Executing '%s' startup hook", step.id)
                await step.startup(ctx)
                executed.append(step)

        except Exception:
            logger.exception("Lifecycle startup failed")

            for step in reversed(executed):
                try:
                    logger.trace("Rolling back '%s' via shutdown", step.id)
                    await step.shutdown(ctx)
                    logger.trace("Rolled back '%s' successfully", step.id)

                except Exception:
                    logger.exception(
                        "Lifecycle rollback shutdown failed for '%s'",
                        step.id,
                    )

            raise

    # ....................... #

    async def shutdown(self, ctx: "ExecutionContext") -> None:
        """Run shutdown hooks in reverse order.

        Exceptions are swallowed so all steps are attempted.
        """

        logger.trace("Running lifecycle shutdown with %s step(s)", len(self.steps))

        for step in reversed(self.steps):
            try:
                logger.trace("Executing '%s' shutdown hook", step.id)
                await step.shutdown(ctx)

            except Exception:
                logger.exception("Lifecycle shutdown failed for '%s'", step.id)

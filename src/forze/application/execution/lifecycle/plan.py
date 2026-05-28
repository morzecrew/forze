"""Lifecycle hooks and plans for startup and shutdown."""

from typing import TYPE_CHECKING, Self, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution.planning.builders import lifecycle_steps_from_sequence

from .module import LifecycleModule

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecyclePlan:
    """Declarative plan for application lifecycle.

    Collects :class:`LifecycleModule` callables and/or :class:`LifecycleStep`
    instances. :meth:`build` merges modules, resolves order via capability and
    dependency metadata, then :meth:`startup` runs hooks in order;
    :meth:`shutdown` runs in reverse. On startup failure, already-executed
    steps are shut down before re-raising.
    """

    modules: tuple[LifecycleModule, ...] = attrs.field(factory=tuple)
    """Modules to invoke when building."""

    steps: tuple[LifecycleStep, ...] = attrs.field(factory=tuple)
    """Lifecycle steps to include when building."""

    # ....................... #

    @classmethod
    def from_modules(cls, *modules: LifecycleModule) -> Self:
        """Create a plan from modules.

        :param modules: Modules to include.
        :returns: New plan instance.
        """

        logger.trace("Creating lifecycle plan from %s module(s)", len(modules))

        return cls(modules=modules)

    # ....................... #

    @classmethod
    def from_steps(cls, *steps: LifecycleStep) -> Self:
        """Create a plan from steps.

        :param steps: Steps to include.
        :returns: New plan instance.
        """

        logger.trace("Creating lifecycle plan from %s step(s)", len(steps))
        logger.trace("Steps: %s", tuple(step.id for step in steps))

        return cls(steps=steps)

    # ....................... #

    def with_modules(self, *modules: LifecycleModule) -> Self:
        """Return a new plan with additional modules appended.

        :param modules: Modules to append.
        :returns: New plan instance.
        """

        logger.trace(
            "Appending %s lifecycle module(s) to plan with %s existing module(s)",
            len(modules),
            len(self.modules),
        )

        return attrs.evolve(self, modules=(*self.modules, *modules))

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        """Return a new plan with additional steps appended.

        :param steps: Steps to append.
        :returns: New plan instance.
        """

        logger.trace(
            "Appending %s lifecycle step(s) to existing plan with %s step(s)",
            len(steps),
            len(self.steps),
        )

        logger.trace("Existing steps: %s", tuple(step.id for step in self.steps))
        logger.trace("New steps: %s", tuple(step.id for step in steps))

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    def build(self) -> Self:
        """Build a resolved plan with topologically ordered steps.

        Invokes each module, concatenates with :attr:`steps`, and orders the
        result. Returns a plan with only :attr:`steps` populated.

        :returns: Resolved lifecycle plan.
        """

        if not self.modules and not self.steps:
            logger.trace("Lifecycle plan is empty; returning empty plan")

            return self

        collected: list[LifecycleStep] = []

        for i, module in enumerate(self.modules, 1):
            module_steps = module()
            logger.trace(
                "Built lifecycle module #%s with %s step(s): %s",
                i,
                len(module_steps),
                tuple(s.id for s in module_steps),
            )
            collected.extend(module_steps)

        collected.extend(self.steps)

        if not collected:
            return attrs.evolve(self, modules=(), steps=())

        ordered = lifecycle_steps_from_sequence(collected)

        logger.trace(
            "Resolved lifecycle plan with %s step(s): %s",
            len(ordered),
            tuple(s.id for s in ordered),
        )

        return attrs.evolve(self, modules=(), steps=ordered)

    # ....................... #

    def _resolved_steps(self) -> tuple[LifecycleStep, ...]:
        if self.modules:
            return self.build().steps

        if self.steps:
            return lifecycle_steps_from_sequence(self.steps)

        return ()

    # ....................... #

    async def startup(self, ctx: "ExecutionContext") -> None:
        """Run startup hooks in order.

        On failure, runs shutdown for already-executed steps in reverse, then
        re-raises.
        """

        resolved = self._resolved_steps()

        logger.trace("Running lifecycle startup with %s step(s)", len(resolved))

        executed: list[LifecycleStep] = []

        try:
            for step in resolved:
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

        resolved = self._resolved_steps()

        logger.trace("Running lifecycle shutdown with %s step(s)", len(resolved))

        for step in reversed(resolved):
            try:
                logger.trace("Executing '%s' shutdown hook", step.id)
                await step.shutdown(ctx)

            except Exception:
                logger.exception("Lifecycle shutdown failed for '%s'", step.id)

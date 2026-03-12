"""Lifecycle hooks and plans for startup and shutdown.

Provides :class:`LifecycleHook` protocol, :class:`LifecycleStep` (named
startup/shutdown pair), and :class:`LifecyclePlan` (ordered sequence of steps).
Startup runs in order; shutdown runs in reverse. On startup failure, already-
executed steps are shut down in reverse before re-raising.
"""

import logging
from typing import Protocol, Self, final

import attrs

from forze.base.errors import CoreError
from forze.base.logging import log_section

from .context import ExecutionContext

# ----------------------- #

logger = logging.getLogger(__name__)

# ....................... #


class LifecycleHook(Protocol):
    """Protocol for a startup or shutdown hook.

    Receives the execution context. May perform setup (startup) or teardown
    (shutdown). Exceptions propagate unless swallowed by the plan.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        """Execute the hook during startup or shutdown."""
        ...


# ....................... #


async def noop_hook(ctx: ExecutionContext) -> None:
    """No-op startup/shutdown hook."""

    return


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecycleStep:
    """Named pair of startup and shutdown hooks.

    Steps are executed in order at startup; shutdown runs in reverse order.
    Name must be unique within a plan for collision detection.
    """

    name: str
    """Unique name for the step (used for collision detection)."""

    startup: LifecycleHook = noop_hook
    """Hook to run on startup."""

    shutdown: LifecycleHook = noop_hook
    """Hook to run on shutdown."""


# ....................... #


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

    @staticmethod
    def _check_name_collision(*steps: LifecycleStep) -> None:
        used: set[str] = set()

        logger.debug(
            "Checking lifecycle step name collisions for %d step(s)", len(steps)
        )

        for step in steps:
            if step.name in used:
                raise CoreError(f"Lifecycle step name collision: {step.name}")

            used.add(step.name)

    # ....................... #

    @classmethod
    def from_steps(cls, *steps: LifecycleStep) -> Self:
        """Create a plan from steps.

        :param steps: Steps to include.
        :returns: New plan instance.
        :raises CoreError: If step names collide.
        """

        logger.debug("Creating lifecycle plan from %d step(s)", len(steps))

        with log_section():
            logger.debug("Steps: %s", tuple(step.name for step in steps))
            cls._check_name_collision(*steps)

        return cls(steps=steps)

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        """Return a new plan with additional steps appended.

        :param steps: Steps to append.
        :returns: New plan instance.
        :raises CoreError: If step names collide.
        """

        logger.debug(
            "Appending %d lifecycle step(s) to existing plan with %d step(s)",
            len(steps),
            len(self.steps),
        )

        with log_section():
            logger.debug("Existing steps: %s", tuple(step.name for step in self.steps))
            logger.debug("New steps: %s", tuple(step.name for step in steps))

            self._check_name_collision(*self.steps, *steps)

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def startup(self, ctx: ExecutionContext) -> None:
        """Run startup hooks in order.

        On failure, runs shutdown for already-executed steps in reverse, then
        re-raises.
        """

        logger.debug(
            "Running lifecycle startup with %d step(s) for context %s",
            len(self.steps),
            type(ctx).__qualname__,
        )

        executed: list[LifecycleStep] = []

        with log_section():
            try:
                for step in self.steps:
                    logger.debug("Starting lifecycle step %s", step.name)

                    with log_section():
                        await step.startup(ctx)

                    executed.append(step)
                    logger.debug("Started lifecycle step %s", step.name)

            except Exception:
                logger.exception("Lifecycle startup failed")

                with log_section():
                    for step in reversed(executed):
                        try:
                            logger.debug(
                                "Rolling back lifecycle step %s via shutdown",
                                step.name,
                            )

                            with log_section():
                                await step.shutdown(ctx)

                            logger.debug(
                                "Rolled back lifecycle step %s successfully",
                                step.name,
                            )

                        except Exception:  # nosec: B110
                            logger.exception(
                                "Lifecycle rollback shutdown failed for step %s",
                                step.name,
                            )

                raise

    # ....................... #

    async def shutdown(self, ctx: ExecutionContext) -> None:
        """Run shutdown hooks in reverse order.

        Exceptions are swallowed so all steps are attempted.
        """

        logger.debug(
            "Running lifecycle shutdown with %d step(s) for context %s",
            len(self.steps),
            type(ctx).__qualname__,
        )

        with log_section():
            for step in reversed(self.steps):
                try:
                    logger.debug("Shutting down lifecycle step %s", step.name)

                    with log_section():
                        await step.shutdown(ctx)

                    logger.debug("Shut down lifecycle step %s", step.name)

                except Exception:  # nosec: B110
                    logger.exception(
                        "Lifecycle shutdown failed for step %s",
                        step.name,
                    )

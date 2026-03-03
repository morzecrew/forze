"""Lifecycle hooks and plans for startup and shutdown.

Provides :class:`LifecycleHook` protocol, :class:`LifecycleStep` (named
startup/shutdown pair), and :class:`LifecyclePlan` (ordered sequence of steps).
Startup runs in order; shutdown runs in reverse. On startup failure, already-
executed steps are shut down in reverse before re-raising.
"""

from typing import Protocol, Self, final

import attrs

from forze.base.errors import CoreError

from .context import ExecutionContext

# ----------------------- #


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
        cls._check_name_collision(*steps)

        return cls(steps=steps)

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        """Return a new plan with additional steps appended.

        :param steps: Steps to append.
        :returns: New plan instance.
        :raises CoreError: If step names collide.
        """
        self._check_name_collision(*self.steps, *steps)

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def startup(self, ctx: ExecutionContext) -> None:
        """Run startup hooks in order.

        On failure, runs shutdown for already-executed steps in reverse, then
        re-raises.
        """
        executed: list[LifecycleStep] = []

        try:
            for step in self.steps:
                await step.startup(ctx)
                executed.append(step)

        except Exception:
            for step in reversed(executed):
                try:
                    await step.shutdown(ctx)

                except Exception:  # nosec: B110
                    pass

            raise

    # ....................... #

    async def shutdown(self, ctx: ExecutionContext) -> None:
        """Run shutdown hooks in reverse order.

        Exceptions are swallowed so all steps are attempted.
        """
        for step in reversed(self.steps):
            try:
                await step.shutdown(ctx)

            except Exception:  # nosec: B110
                pass

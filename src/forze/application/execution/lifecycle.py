from typing import Optional, Protocol, Self, final

import attrs

from forze.base.errors import CoreError

from .context import ExecutionContext

# ----------------------- #


class LifecycleHook(Protocol):
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
    """Lifecycle step to be executed during startup or shutdown."""

    name: str
    """Name of the lifecycle step."""

    startup: LifecycleHook
    """Startup hook to be executed during startup."""

    shutdown: LifecycleHook
    """Shutdown hook to be executed during shutdown."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecyclePlan:
    """Declarative plan for application lifecycle."""

    steps: tuple[LifecycleStep, ...] = attrs.field(factory=tuple)

    # ....................... #

    def _generate_name(self) -> str:
        return f"lifecycle_step_{len(self.steps)}"

    # ....................... #

    def _check_name_collision(self, name: str) -> None:
        if any(step.name == name for step in self.steps):
            raise CoreError(f"Lifecycle step name collision: {name}")

    # ....................... #

    @classmethod
    def from_steps(cls, *steps: LifecycleStep) -> Self:
        return cls(steps=steps)

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #
    #! TODO: review ... maybe remove this strange method

    def with_step(
        self,
        *,
        startup: LifecycleHook,
        shutdown: LifecycleHook = noop_hook,
        name: Optional[str] = None,
    ) -> Self:
        """Add a new lifecycle step to the plan.

        :param startup: Startup hook to be executed during startup.
        :param shutdown: Shutdown hook to be executed during shutdown.
        :param name: Name of the lifecycle step. If not provided, a unique name will be generated.
        :returns: A new :class:`LifecyclePlan` with the step added.
        :raises CoreError: If the name is already used.
        """

        name = name or self._generate_name()
        self._check_name_collision(name)

        step = LifecycleStep(name=name, startup=startup, shutdown=shutdown)

        return attrs.evolve(self, steps=(*self.steps, step))

    # ....................... #

    async def startup(self, ctx: ExecutionContext) -> None:
        executed: list[LifecycleStep] = []

        try:
            for step in self.steps:
                await step.startup(ctx)
                executed.append(step)

        except Exception:
            for step in reversed(executed):
                try:
                    await step.shutdown(ctx)

                except Exception:
                    pass

            raise

    # ....................... #

    async def shutdown(self, ctx: ExecutionContext) -> None:
        for step in reversed(self.steps):
            try:
                await step.shutdown(ctx)
            except Exception:
                pass

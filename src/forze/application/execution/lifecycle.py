from typing import Protocol, Self, final

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

    startup: LifecycleHook = noop_hook
    """Startup hook to be executed during startup."""

    shutdown: LifecycleHook = noop_hook
    """Shutdown hook to be executed during shutdown."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecyclePlan:
    """Declarative plan for application lifecycle."""

    steps: tuple[LifecycleStep, ...] = attrs.field(factory=tuple)

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
        cls._check_name_collision(*steps)

        return cls(steps=steps)

    # ....................... #

    def with_steps(self, *steps: LifecycleStep) -> Self:
        self._check_name_collision(*self.steps, *steps)

        return attrs.evolve(self, steps=(*self.steps, *steps))

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

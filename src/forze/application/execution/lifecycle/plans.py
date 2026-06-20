"""Frozen and resolved lifecycle plans."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import (
    ExecutionGraph,
    LifecycleModule,
    LifecycleStep,
)
from forze.application.contracts.execution.builders import steps_graph_from_sequence
from forze.base.primitives import AbstractSequence

from .run import run_lifecycle_shutdown, run_lifecycle_startup

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LifecyclePlan:
    """Declarative plan for application lifecycle.

    Collects :class:`LifecycleModule` callables and/or :class:`LifecycleStep`
    instances. :meth:`freeze` merges modules, validates the dependency graph,
    and returns a :class:`FrozenLifecyclePlan` for startup and shutdown.
    """

    modules: tuple[LifecycleModule, ...] = attrs.field(factory=tuple)
    """Modules to invoke when freezing."""

    steps: tuple[LifecycleStep, ...] = attrs.field(factory=tuple)
    """Lifecycle steps to include when freezing."""

    concurrent: bool = False
    """When ``True``, run steps within the same wave concurrently at runtime."""

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

    def with_concurrent(self, concurrent: bool = True) -> Self:
        """Return a new plan with the given concurrent wave execution flag.

        :param concurrent: When ``True``, steps in the same wave run concurrently.
        :returns: New plan instance.
        """

        return attrs.evolve(self, concurrent=concurrent)

    # ....................... #

    def freeze(self) -> FrozenLifecyclePlan:
        """Freeze the plan into a validated execution graph.

        Invokes each module, concatenates with :attr:`steps`, and builds
        topological waves from capability and dependency metadata.

        :returns: Frozen lifecycle plan.
        """

        if not self.modules and not self.steps:
            logger.trace("Lifecycle plan is empty; returning empty frozen plan")

            return FrozenLifecyclePlan(concurrent=self.concurrent)

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
            return FrozenLifecyclePlan(concurrent=self.concurrent)

        graph = steps_graph_from_sequence(AbstractSequence(items=tuple(collected)))

        logger.trace(
            "Frozen lifecycle plan with %s step(s) in %s wave(s): %s",
            len(graph.steps),
            len(graph.waves),
            graph.waves,
        )

        return FrozenLifecyclePlan(graph=graph, concurrent=self.concurrent)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenLifecyclePlan:
    """Frozen lifecycle plan with validated topological waves."""

    graph: ExecutionGraph[LifecycleStep] = attrs.field(factory=ExecutionGraph)
    """Lifecycle steps in topological waves."""

    concurrent: bool = False
    """When ``True``, run steps within the same wave concurrently."""

    # ....................... #

    async def startup(self, ctx: "ExecutionContext") -> None:
        """Run startup hooks in forward wave order."""

        await run_lifecycle_startup(self.graph, ctx, concurrent=self.concurrent)

    # ....................... #

    async def shutdown(self, ctx: "ExecutionContext") -> None:
        """Run shutdown hooks in reverse wave order."""

        await run_lifecycle_shutdown(self.graph, ctx, concurrent=self.concurrent)

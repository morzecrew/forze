from contextlib import asynccontextmanager
from typing import AsyncIterator, final

import attrs

from forze.base.primitives import RuntimeVar

from .context import ExecutionContext
from .deps import DepsPlan
from .lifecycle import LifecyclePlan

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ExecutionRuntime:
    """Runnable scope which combines deps plan, lifecycle plan and execution context."""

    deps: DepsPlan
    """Dependencies plan."""

    lifecycle: LifecyclePlan = attrs.field(factory=LifecyclePlan)
    """Lifecycle plan."""

    ctx: RuntimeVar[ExecutionContext] = attrs.field(
        factory=lambda: RuntimeVar("execution_context"),
        repr=False,
        init=False,
    )
    """Execution context."""

    # ....................... #

    def get_context(self) -> ExecutionContext:
        return self.ctx.get()

    # ....................... #

    def create_context(self) -> None:
        ctx = ExecutionContext(deps=self.deps.build())
        self.ctx.set_once(ctx)

    # ....................... #

    async def startup(self) -> None:
        await self.lifecycle.startup(self.get_context())

    # ....................... #

    async def shutdown(self) -> None:
        ctx = self.get_context()

        try:
            await self.lifecycle.shutdown(ctx)

        finally:
            self.ctx.reset()

    # ....................... #

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
        self.create_context()

        try:
            await self.startup()
            yield

        finally:
            await self.shutdown()

from typing import TYPE_CHECKING, Any, Sequence

import attrs

from ..middlewares import Middleware, TxMiddleware
from .plan import MiddlewarePlan
from .resolver import CapabilityResolver
from .runners import CapabilityAfterCommitRunner, CapabilitySlotMiddlewareRunner
from .scheduler import CapabilityScheduler
from .slot import MiddlewareSlot

if TYPE_CHECKING:
    from ..context import ExecutionContext


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionChainCompiler:
    """Compiler for execution chains."""

    plan: MiddlewarePlan
    """Plan to compile."""

    # ....................... #

    def _compile_slot(
        self,
        slot: MiddlewareSlot,
        ctx: "ExecutionContext",
    ) -> CapabilitySlotMiddlewareRunner[Any, Any]:
        scheduler = CapabilityScheduler.from_plan(self.plan, slot)
        resolver = CapabilityResolver(scheduler=scheduler)
        steps = resolver.resolve(ctx)

        return CapabilitySlotMiddlewareRunner(slot=slot, steps=steps)

    # ....................... #

    def _compile_after_commit(
        self,
        ctx: "ExecutionContext",
    ) -> CapabilityAfterCommitRunner[Any, Any]:
        compiled_slot = self._compile_slot(MiddlewareSlot.after_commit, ctx)

        return CapabilityAfterCommitRunner(slot_runner=compiled_slot)

    # ....................... #

    def compile_(self, ctx: "ExecutionContext") -> Sequence[Middleware[Any, Any]]:
        after_commit = self._compile_after_commit(ctx)

        chain: list[Middleware[Any, Any]] = []
        tx_inserted = False

        for slot in MiddlewareSlot.iter_slot_order():
            if (
                slot.requires_tx()
                and not tx_inserted
                and self.plan.tx_route is not None
            ):
                tx = TxMiddleware[Any, Any](
                    route=self.plan.tx_route,
                    runnable=ctx.transaction,
                ).with_after_commit(after_commit)

                chain.append(tx)
                tx_inserted = True

            if slot is MiddlewareSlot.after_commit:
                continue

            if slot.is_schedulable():
                chain.append(self._compile_slot(slot, ctx))
                continue

            chain.extend(spec.factory(ctx) for spec in self.plan.for_slot(slot))

        return tuple(chain)

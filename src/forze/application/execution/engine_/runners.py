from typing import Sequence, cast

import attrs

from forze.base.errors import CoreError

from ..middlewares import Guard, Middleware, NextCall, OnSuccess, Skip
from .readiness import CapabilityReadiness
from .resolver import ResolvedCapabilityStep
from .slot import MiddlewareSlot

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilitySlotMiddlewareRunner[Args, R](Middleware[Args, R]):
    """Middleware that runs a capability slot."""

    slot: MiddlewareSlot
    """Slot to run."""

    steps: Sequence[ResolvedCapabilityStep]
    """Steps to run."""

    readiness: CapabilityReadiness = attrs.field(
        factory=CapabilityReadiness,
        init=False,
    )
    """Readiness tracker for the slot."""

    # ....................... #

    async def _run(self, args: Args, result: R | None) -> None:
        if not self.slot.is_schedulable():
            raise CoreError(
                f"Slot {self.slot!r} is not schedulable",
            )

        for step in self.steps:
            spec = step.spec
            mw = step.runnable

            if not self.readiness.is_ready(spec.requires):
                #! TODO: log
                continue

            try:
                if self.slot.is_before():
                    out = await cast(Guard[Args], mw)(args)

                else:
                    result = cast(R, result)
                    out = await cast(OnSuccess[Args, R], mw)(args, result)

            except Exception as exc:
                raise exc

            if isinstance(out, Skip):
                self.readiness.mark_skipped(spec.provides)

            else:
                self.readiness.mark_success(spec.provides)

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        if self.slot in (MiddlewareSlot.before, MiddlewareSlot.tx_before):
            await self._run(args, None)
            return await next(args)

        result = await next(args)
        await self._run(args, result)

        return result


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityAfterCommitRunner[Args, R](OnSuccess[Args, R]):
    """Runner for after-commit capability hooks."""

    slot_runner: CapabilitySlotMiddlewareRunner[Args, R]
    """Runner for the slot that runs after-commit capability hooks."""

    # ....................... #

    async def __call__(self, args: Args, result: R) -> None:
        async def done(_args: Args) -> R:
            return result

        await self.slot_runner(done, args)

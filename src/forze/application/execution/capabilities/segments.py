"""Segment middleware: multiple guards or effects sharing a :class:`CapabilityStore`."""

from typing import Any, TypeVar

import attrs

from forze.application._logger import logger
from forze.base.errors import CoreError

from ..context import ExecutionContext
from ..middleware import (
    Effect,
    EffectMiddleware,
    Guard,
    Middleware,
    NextCall,
)
from .async_util import maybe_await
from .trace import (
    CapabilitySkip,
    CapabilityStore,
    SchedulableCapabilitySpec,
    capability_step_label,
)

# ----------------------- #

ArgsT = TypeVar("ArgsT")
ResT = TypeVar("ResT")

# ....................... #


@attrs.define(slots=True, kw_only=True)
class CapabilityGuardSegmentMiddleware(Middleware[ArgsT, ResT]):
    """Runs multiple guards in capability order with a shared :class:`CapabilityStore`."""

    bucket: str
    store: CapabilityStore
    steps: tuple[tuple[Guard[ArgsT], SchedulableCapabilitySpec], ...]

    # ....................... #

    async def __call__(self, next: NextCall[ArgsT, ResT], args: ArgsT) -> ResT:
        for guard, spec in self.steps:
            label = capability_step_label(spec, guard)

            if not self.store.is_ready(spec.requires):
                logger.debug(
                    "Skipping guard (missing capability): bucket=%s label=%s",
                    self.bucket,
                    label,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running guard (capability segment): bucket=%s label=%s",
                self.bucket,
                label,
            )

            raw = guard(args)
            result = await maybe_await(raw)

            if isinstance(result, CapabilitySkip):
                self.store.mark_missing(spec.provides)

                logger.debug(
                    "Guard skipped: bucket=%s label=%s reason=%s",
                    self.bucket,
                    label,
                    result.reason,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="skipped_return",
                    detail=result.reason,
                )

            else:
                self.store.mark_success(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="ran",
                    detail=None,
                )

        return await next(args)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class CapabilityEffectSegmentMiddleware(Middleware[ArgsT, ResT]):
    """Runs multiple effects in capability order after ``next`` returns."""

    bucket: str
    store: CapabilityStore
    steps: tuple[tuple[Effect[ArgsT, ResT], SchedulableCapabilitySpec], ...]

    # ....................... #

    async def __call__(self, next: NextCall[ArgsT, ResT], args: ArgsT) -> ResT:
        res = await next(args)

        for effect, spec in self.steps:
            label = capability_step_label(spec, effect)

            if not self.store.is_ready(spec.requires):
                logger.debug(
                    "Skipping effect (missing capability): bucket=%s label=%s",
                    self.bucket,
                    label,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running effect (capability segment): bucket=%s label=%s",
                self.bucket,
                label,
            )

            raw = effect(args, res)
            out = await maybe_await(raw)

            if isinstance(out, CapabilitySkip):
                self.store.mark_missing(spec.provides)

                logger.debug(
                    "Effect skipped: bucket=%s label=%s reason=%s",
                    self.bucket,
                    label,
                    out.reason,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="skipped_return",
                    detail=out.reason,
                )

            else:
                res = out
                self.store.mark_success(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="ran",
                    detail=None,
                )

        return res


# ....................... #


def resolve_after_commit_effects(
    ctx: ExecutionContext,
    ordered_specs: tuple[SchedulableCapabilitySpec, ...],
) -> tuple[Any, ...]:
    effects: list[Any] = []

    for s in ordered_specs:
        mw = s.factory(ctx)

        if not isinstance(mw, EffectMiddleware):
            raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

        effects.append(
            mw.effect,  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
        )

    return tuple(effects)

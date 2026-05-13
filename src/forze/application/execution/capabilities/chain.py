"""Build middleware tuple when the capability engine is enabled."""

from typing import Any, cast

import attrs

from forze.application.execution.bucket import Bucket
from forze.base.errors import CoreError

from ..context import ExecutionContext
from ..middleware import Middleware, TxMiddleware
from ..plan.operation import OperationPlan
from ..plan.spec import MiddlewareSpec
from .after_commit import CapabilityAfterCommitRunner
from .resolvers import resolve_effect_steps, resolve_guard_steps
from .scheduler import schedule_capability_specs
from .segments import (
    CapabilityEffectSegmentMiddleware,
    CapabilityGuardSegmentMiddleware,
    resolve_after_commit_effects,
)
from .trace import CapabilityExecutionEvent, CapabilityStore, SchedulableCapabilitySpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityChainBuilder:
    """Composes the flat middleware tuple with capability segments (see :meth:`UsecasePlan.resolve`)."""

    ctx: ExecutionContext
    """Execution context."""

    plan: OperationPlan
    """Operation plan."""

    capability_execution_trace: list[CapabilityExecutionEvent] | None = None
    """Capability execution trace."""

    # ....................... #

    def _segment_guards(
        self,
        store: CapabilityStore,
        bucket: Bucket,
        specs: tuple[MiddlewareSpec, ...],
    ) -> Middleware[Any, Any]:
        ordered = cast(
            tuple[MiddlewareSpec, ...],
            schedule_capability_specs(
                cast(tuple[SchedulableCapabilitySpec, ...], specs),
                bucket=bucket.value,
            ),
        )
        steps = resolve_guard_steps(
            self.ctx,
            cast(tuple[SchedulableCapabilitySpec, ...], ordered),
            bucket=bucket.value,
        )

        return CapabilityGuardSegmentMiddleware[Any, Any](
            bucket=bucket.value,
            store=store,
            steps=steps,
        )

    # ....................... #

    def _segment_effects(
        self,
        store: CapabilityStore,
        bucket: Bucket,
        specs: tuple[MiddlewareSpec, ...],
    ) -> Middleware[Any, Any]:
        ordered = cast(
            tuple[MiddlewareSpec, ...],
            schedule_capability_specs(
                cast(tuple[SchedulableCapabilitySpec, ...], specs),
                bucket=bucket.value,
            ),
        )
        steps = resolve_effect_steps(
            self.ctx,
            cast(tuple[SchedulableCapabilitySpec, ...], ordered),
            bucket=bucket.value,
        )

        return CapabilityEffectSegmentMiddleware[Any, Any](
            bucket=bucket.value,
            store=store,
            steps=steps,
        )

    # ....................... #

    def build(
        self,
        *,
        outer_before: tuple[MiddlewareSpec, ...],
        outer_wrap: tuple[MiddlewareSpec, ...],
        outer_finally: tuple[MiddlewareSpec, ...],
        outer_on_failure: tuple[MiddlewareSpec, ...],
        outer_after: tuple[MiddlewareSpec, ...],
        in_tx_before: tuple[MiddlewareSpec, ...],
        in_tx_finally: tuple[MiddlewareSpec, ...],
        in_tx_on_failure: tuple[MiddlewareSpec, ...],
        in_tx_wrap: tuple[MiddlewareSpec, ...],
        in_tx_after: tuple[MiddlewareSpec, ...],
        after_commit_specs: tuple[MiddlewareSpec, ...],
    ) -> tuple[Middleware[Any, Any], ...]:
        store = CapabilityStore(trace_events=self.capability_execution_trace)

        after_commit_ordered = schedule_capability_specs(
            cast(tuple[SchedulableCapabilitySpec, ...], after_commit_specs),
            bucket=Bucket.after_commit.value,
        )

        after_commit_effects = resolve_after_commit_effects(
            self.ctx, after_commit_ordered
        )

        runner = CapabilityAfterCommitRunner(
            store=store,
            effects=after_commit_effects,
            specs=after_commit_ordered,
        )

        chain: list[Middleware[Any, Any]] = []

        if outer_before:
            chain.append(self._segment_guards(store, Bucket.outer_before, outer_before))

        chain.extend(s.factory(self.ctx) for s in outer_wrap)
        chain.extend(s.factory(self.ctx) for s in outer_finally)
        chain.extend(s.factory(self.ctx) for s in outer_on_failure)

        if self.plan.tx is not None:
            tx = TxMiddleware[Any, Any](ctx=self.ctx, route=self.plan.tx.route)

            if after_commit_effects:
                tx = tx.with_after_commit(runner)

            chain.append(tx)

            if in_tx_before:
                chain.append(
                    self._segment_guards(store, Bucket.in_tx_before, in_tx_before)
                )

            chain.extend(s.factory(self.ctx) for s in in_tx_finally)
            chain.extend(s.factory(self.ctx) for s in in_tx_on_failure)
            chain.extend(s.factory(self.ctx) for s in in_tx_wrap)

            if in_tx_after:
                chain.append(
                    self._segment_effects(store, Bucket.in_tx_after, in_tx_after)
                )

        elif after_commit_specs:
            raise CoreError(
                "after_commit middlewares present but transaction is disabled for this operation"
            )

        if outer_after:
            chain.append(self._segment_effects(store, Bucket.outer_after, outer_after))

        return tuple(chain)

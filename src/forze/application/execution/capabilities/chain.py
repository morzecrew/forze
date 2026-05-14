"""Build middleware tuple when the capability engine is enabled."""

from typing import Any

import attrs

from forze.application.execution.bucket import BucketKey
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
from .trace import CapabilityExecutionEvent, CapabilityStore

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityChainBuilder:
    """Composes the flat middleware tuple with capability segments (see :meth:`UsecasePlan.resolve`)."""

    ctx: ExecutionContext
    """Execution context."""

    capability_execution_trace: list[CapabilityExecutionEvent] | None = None
    """Capability execution trace."""

    # ....................... #

    def _segment_guards(
        self,
        store: CapabilityStore,
        key: BucketKey,
        specs: tuple[MiddlewareSpec, ...],
    ) -> Middleware[Any, Any]:
        label = key.label
        ordered = schedule_capability_specs(specs, bucket=label)
        steps = resolve_guard_steps(self.ctx, ordered, bucket=label)

        return CapabilityGuardSegmentMiddleware[Any, Any](
            bucket=label,
            store=store,
            steps=steps,
        )

    # ....................... #

    def _segment_effects(
        self,
        store: CapabilityStore,
        key: BucketKey,
        specs: tuple[MiddlewareSpec, ...],
    ) -> Middleware[Any, Any]:
        label = key.label
        ordered = schedule_capability_specs(specs, bucket=label)
        steps = resolve_effect_steps(self.ctx, ordered, bucket=label)

        return CapabilityEffectSegmentMiddleware[Any, Any](
            bucket=label,
            store=store,
            steps=steps,
        )

    # ....................... #

    def build(self, plan: OperationPlan) -> tuple[Middleware[Any, Any], ...]:
        def _s(k: BucketKey) -> tuple[MiddlewareSpec, ...]:
            return plan.specs_for_chain(k)

        outer_before = _s(BucketKey.OUTER_BEFORE)
        outer_wrap = _s(BucketKey.OUTER_WRAP)
        outer_finally = _s(BucketKey.OUTER_FINALLY)
        outer_on_failure = _s(BucketKey.OUTER_ON_FAILURE)
        outer_after = _s(BucketKey.OUTER_AFTER)
        in_tx_before = _s(BucketKey.IN_TX_BEFORE)
        in_tx_finally = _s(BucketKey.IN_TX_FINALLY)
        in_tx_on_failure = _s(BucketKey.IN_TX_ON_FAILURE)
        in_tx_wrap = _s(BucketKey.IN_TX_WRAP)
        in_tx_after = _s(BucketKey.IN_TX_AFTER)
        after_commit_specs = _s(BucketKey.AFTER_COMMIT)

        store = CapabilityStore(trace_events=self.capability_execution_trace)

        after_commit_ordered = schedule_capability_specs(
            after_commit_specs,
            bucket=BucketKey.AFTER_COMMIT.label,
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
            chain.append(
                self._segment_guards(store, BucketKey.OUTER_BEFORE, outer_before)
            )

        chain.extend(s.factory(self.ctx) for s in outer_wrap)
        chain.extend(s.factory(self.ctx) for s in outer_finally)
        chain.extend(s.factory(self.ctx) for s in outer_on_failure)

        if plan.tx is not None:
            tx = TxMiddleware[Any, Any](ctx=self.ctx, route=plan.tx.route)

            if after_commit_effects:
                tx = tx.with_after_commit(runner)

            chain.append(tx)

            if in_tx_before:
                chain.append(
                    self._segment_guards(store, BucketKey.IN_TX_BEFORE, in_tx_before)
                )

            chain.extend(s.factory(self.ctx) for s in in_tx_finally)
            chain.extend(s.factory(self.ctx) for s in in_tx_on_failure)
            chain.extend(s.factory(self.ctx) for s in in_tx_wrap)

            if in_tx_after:
                chain.append(
                    self._segment_effects(store, BucketKey.IN_TX_AFTER, in_tx_after)
                )

        elif after_commit_specs:
            raise CoreError(
                "after_commit middlewares present but transaction is disabled for this operation"
            )

        if outer_after:
            chain.append(
                self._segment_effects(store, BucketKey.OUTER_AFTER, outer_after)
            )

        return tuple(chain)

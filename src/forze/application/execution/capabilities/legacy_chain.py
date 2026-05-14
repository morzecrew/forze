"""Build middleware tuple when the capability engine is disabled (legacy ordering)."""

from typing import Any

import attrs

from forze.application.execution.bucket import BucketKey
from forze.base.errors import CoreError

from ..context import ExecutionContext
from ..middleware import Effect, EffectMiddleware, Middleware, TxMiddleware
from ..plan.operation import OperationPlan
from ..plan.spec import MiddlewareSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class LegacyChainBuilder:
    """Flat middleware tuple without capability segments."""

    ctx: ExecutionContext
    """Execution context."""

    # ....................... #

    def build(self, plan: OperationPlan) -> tuple[Middleware[Any, Any], ...]:
        def _s(k: BucketKey) -> tuple[MiddlewareSpec, ...]:
            return plan.specs_for_chain(k)

        after_commit = _s(BucketKey.AFTER_COMMIT)
        after_commit_effects: list[Effect[Any, Any]] = []

        for s in after_commit:
            mw = s.factory(self.ctx)

            if not isinstance(mw, EffectMiddleware):
                raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

            after_commit_effects.append(mw.effect)

        chain: list[Middleware[Any, Any]] = []

        chain.extend(s.factory(self.ctx) for s in _s(BucketKey.OUTER_BEFORE))
        chain.extend(s.factory(self.ctx) for s in _s(BucketKey.OUTER_WRAP))
        chain.extend(s.factory(self.ctx) for s in _s(BucketKey.OUTER_FINALLY))
        chain.extend(s.factory(self.ctx) for s in _s(BucketKey.OUTER_ON_FAILURE))

        if plan.tx is not None:
            chain.append(
                TxMiddleware[Any, Any](
                    ctx=self.ctx,
                    route=plan.tx.route,
                ).with_after_commit(*after_commit_effects)
            )
            chain.extend(s.factory(self.ctx) for s in _s(BucketKey.IN_TX_BEFORE))
            chain.extend(s.factory(self.ctx) for s in _s(BucketKey.IN_TX_FINALLY))
            chain.extend(s.factory(self.ctx) for s in _s(BucketKey.IN_TX_ON_FAILURE))
            chain.extend(s.factory(self.ctx) for s in _s(BucketKey.IN_TX_WRAP))
            chain.extend(s.factory(self.ctx) for s in _s(BucketKey.IN_TX_AFTER))

        chain.extend(s.factory(self.ctx) for s in _s(BucketKey.OUTER_AFTER))

        return tuple(chain)

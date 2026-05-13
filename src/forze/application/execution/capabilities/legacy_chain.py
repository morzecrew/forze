"""Build middleware tuple when the capability engine is disabled (legacy ordering)."""

from typing import Any

import attrs

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

    plan: OperationPlan
    """Operation plan."""

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
        after_commit: tuple[MiddlewareSpec, ...],
    ) -> tuple[Middleware[Any, Any], ...]:
        after_commit_effects: list[Effect[Any, Any]] = []

        for s in after_commit:
            mw = s.factory(self.ctx)

            if not isinstance(mw, EffectMiddleware):
                raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

            after_commit_effects.append(mw.effect)

        chain: list[Middleware[Any, Any]] = []

        chain.extend(s.factory(self.ctx) for s in outer_before)
        chain.extend(s.factory(self.ctx) for s in outer_wrap)
        chain.extend(s.factory(self.ctx) for s in outer_finally)
        chain.extend(s.factory(self.ctx) for s in outer_on_failure)

        if self.plan.tx is not None:
            chain.append(
                TxMiddleware[Any, Any](
                    ctx=self.ctx,
                    route=self.plan.tx.route,
                ).with_after_commit(*after_commit_effects)
            )
            chain.extend(s.factory(self.ctx) for s in in_tx_before)
            chain.extend(s.factory(self.ctx) for s in in_tx_finally)
            chain.extend(s.factory(self.ctx) for s in in_tx_on_failure)
            chain.extend(s.factory(self.ctx) for s in in_tx_wrap)
            chain.extend(s.factory(self.ctx) for s in in_tx_after)

        chain.extend(s.factory(self.ctx) for s in outer_after)

        return tuple(chain)

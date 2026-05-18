"""Middleware chain compiler for one resolved operation."""

from typing import Any, Literal

import attrs

from ..context import ExecutionContext
from ..middlewares import Middleware, TxMiddleware
from ..plan.spec import MiddlewareSpec
from .capabilities import (
    CapabilityAfterCommitRunner,
    CapabilityExecutionEvent,
    CapabilityStageMiddleware,
    CapabilityStore,
    execution_ordered_specs,
    resolve_capability_steps,
)
from .model import OperationStages
from .stages import Stage

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionChainCompiler:
    """Compile resolved operation stages into a middleware tuple."""

    ctx: ExecutionContext
    capability_execution_trace: list[CapabilityExecutionEvent] | None = None

    # ....................... #

    def _compile_capability_stage(
        self,
        store: CapabilityStore,
        stage: Stage,
        specs: tuple[MiddlewareSpec, ...],
    ) -> CapabilityStageMiddleware[Any, Any]:
        kind: Literal["guard", "success_hook"]

        if stage.explain_kind.value == "guard":
            kind = "guard"

        else:
            kind = "success_hook"

        ordered = execution_ordered_specs(specs, stage=stage.value)
        steps = resolve_capability_steps(
            self.ctx,
            ordered,
            stage=stage.value,
            kind=kind,
        )

        return CapabilityStageMiddleware[Any, Any](
            stage=stage.value,
            kind=kind,
            store=store,
            steps=steps,
        )

    # ....................... #

    def _compile_after_commit(
        self,
        store: CapabilityStore,
        stages: OperationStages,
    ) -> CapabilityAfterCommitRunner | None:
        specs = stages.specs_for_chain(Stage.after_commit)

        if not specs:
            return None

        ordered = execution_ordered_specs(specs, stage=Stage.after_commit.value)
        steps = resolve_capability_steps(
            self.ctx,
            ordered,
            stage=Stage.after_commit.value,
            kind="success_hook",
        )

        return CapabilityAfterCommitRunner(store=store, steps=steps)

    # ....................... #

    def build(self, stages: OperationStages) -> tuple[Middleware[Any, Any], ...]:
        store = CapabilityStore(trace_events=self.capability_execution_trace)
        after_commit = self._compile_after_commit(store, stages)

        chain: list[Middleware[Any, Any]] = []
        tx_inserted = False

        for stage in Stage.iter_chain_order():
            if stage.requires_tx and not tx_inserted and stages.tx_route is not None:
                tx = TxMiddleware[Any, Any](
                    runnable=self.ctx.transaction,
                    route=stages.tx_route,
                )

                if after_commit is not None:
                    tx = tx.with_after_commit(after_commit)

                chain.append(tx)
                tx_inserted = True

            if stage is Stage.after_commit:
                continue

            specs = stages.specs_for_chain(stage)

            if not specs:
                continue

            if stage.schedulable:
                chain.append(self._compile_capability_stage(store, stage, specs))
                continue

            chain.extend(spec.factory(self.ctx) for spec in specs)

        return tuple(chain)

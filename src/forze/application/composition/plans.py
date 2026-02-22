from typing import Any, Callable, Literal, Optional, Self, TypeVar, cast

import attrs

from forze.base.errors import CoreError

from ..kernel.ports import AppRuntimePort
from ..kernel.usecase import Effect as UcEffect
from ..kernel.usecase import Guard as UcGuard
from ..kernel.usecase import TxUsecase, Usecase

# ----------------------- #

Uc = Usecase[Any, Any]
Guard = UcGuard[Any]
Effect = UcEffect[Any, Any]

Phase = Literal["auth", "validate", "before_write", "after_write", "finalize"]
Operation = Literal["create", "update", "delete", "restore", "kill"]

_PHASE_ORDER: dict[Phase, int] = {
    "auth": 0,
    "validate": 1,
    "before_write": 2,
    "after_write": 3,
    "finalize": 4,
}

#! How to extend list of supported operations ? should be default to crud only ?

U = TypeVar("U", bound=Usecase[Any, Any])
Builder = Callable[[AppRuntimePort], U]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardSpec:
    phase: Phase
    guard: Guard


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectSpec:
    phase: Phase
    effect: Effect


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    builder: Optional[Builder[Any]] = None
    guards: tuple[GuardSpec, ...] = ()
    effects: tuple[EffectSpec, ...] = ()
    side_guards: tuple[GuardSpec, ...] = ()
    side_effects: tuple[EffectSpec, ...] = ()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    ops: dict[Operation, OperationPlan] = attrs.field(factory=dict)

    # ....................... #

    def override(self, op: Operation, builder: Builder[U]) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        new_ops[op] = attrs.evolve(cur, builder=builder)

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def before(
        self,
        op: Operation,
        guard: Guard,
        phase: Phase = "validate",
        *,
        side: bool = False,
    ) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        guard_spec = GuardSpec(phase=phase, guard=guard)

        if side:
            new_ops[op] = attrs.evolve(cur, side_guards=(*cur.side_guards, guard_spec))

        else:
            new_ops[op] = attrs.evolve(cur, guards=(*cur.guards, guard_spec))

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def after(
        self,
        op: Operation,
        effect: Effect,
        phase: Phase = "finalize",
        *,
        side: bool = False,
    ) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        effect_spec = EffectSpec(phase=phase, effect=effect)

        if side:
            new_ops[op] = attrs.evolve(
                cur,
                side_effects=(*cur.side_effects, effect_spec),
            )

        else:
            new_ops[op] = attrs.evolve(cur, effects=(*cur.effects, effect_spec))

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def build(self, op: Operation, runtime: AppRuntimePort, default: U) -> U:
        plan = self.ops.get(op)
        uc = default

        if plan and plan.builder:
            builder = cast(Builder[U], plan.builder)
            uc = builder(runtime)

            guards = tuple(
                gs.guard
                for gs in sorted(plan.guards, key=lambda x: _PHASE_ORDER[x.phase])
            )
            effects = tuple(
                es.effect
                for es in sorted(plan.effects, key=lambda x: _PHASE_ORDER[x.phase])
            )
            side_guards = tuple(
                gs.guard
                for gs in sorted(plan.side_guards, key=lambda x: _PHASE_ORDER[x.phase])
            )
            side_effects = tuple(
                es.effect
                for es in sorted(plan.side_effects, key=lambda x: _PHASE_ORDER[x.phase])
            )

            if isinstance(uc, TxUsecase):
                uc = (
                    uc.with_guards(*guards)
                    .with_effects(*effects)
                    .with_side_guards(*side_guards)
                    .with_side_effects(*side_effects)
                )

            else:
                #! TODO: log warning that side guards and effects are not supported for non-tx usecases
                #! and they will be translated into guards and effects
                uc = uc.with_effects(
                    *effects,
                    *side_effects,
                ).with_guards(
                    *side_guards,
                    *guards,
                )

        return uc

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self) -> Self:
        acc: dict[Operation, OperationPlan] = {}

        for p in plans:
            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())

                # prevent conflicting builders
                if cur.builder and pl.builder and cur.builder is not pl.builder:
                    raise CoreError(f"Conflicting builders for operation: {op}")

                # override: last wins
                builder = pl.builder if pl.builder is not None else cur.builder
                acc[op] = OperationPlan(
                    builder=builder,
                    guards=(*cur.guards, *pl.guards),
                    effects=(*cur.effects, *pl.effects),
                    side_guards=(*cur.side_guards, *pl.side_guards),
                    side_effects=(*cur.side_effects, *pl.side_effects),
                )

        return cls(ops=acc)

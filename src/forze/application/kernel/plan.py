from typing import Any, Callable, Optional, Self, TypeVar, cast

import attrs

from forze.base.errors import CoreError

from .dependencies import UsecaseContext
from .usecase import Effect, Guard, TxUsecase, Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

GuardFactory = Callable[[UsecaseContext], Guard[Any]]
EffectFactory = Callable[[UsecaseContext], Effect[Any, Any]]
UsecaseFactory = Callable[[UsecaseContext], U]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardSpec:
    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    guard: GuardFactory


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectSpec:
    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    effect: EffectFactory


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    override: Optional[UsecaseFactory[Any]] = None
    guards: tuple[GuardSpec, ...] = attrs.field(factory=tuple)
    effects: tuple[EffectSpec, ...] = attrs.field(factory=tuple)
    side_guards: tuple[GuardSpec, ...] = attrs.field(factory=tuple)
    side_effects: tuple[EffectSpec, ...] = attrs.field(factory=tuple)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    ops: dict[str, OperationPlan] = attrs.field(factory=dict)

    # ....................... #

    def override(self, op: str, factory: UsecaseFactory[U]) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        new_ops[op] = attrs.evolve(cur, override=factory)

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def before(
        self,
        op: str,
        guard: GuardFactory,
        priority: int = 0,
        *,
        side: bool = False,
    ) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        guard_spec = GuardSpec(priority=priority, guard=guard)

        if side:
            new_ops[op] = attrs.evolve(cur, side_guards=(*cur.side_guards, guard_spec))

        else:
            new_ops[op] = attrs.evolve(cur, guards=(*cur.guards, guard_spec))

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def after(
        self,
        op: str,
        effect: EffectFactory,
        priority: int = 0,
        *,
        side: bool = False,
    ) -> Self:
        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        effect_spec = EffectSpec(priority=priority, effect=effect)

        if side:
            new_ops[op] = attrs.evolve(
                cur,
                side_effects=(*cur.side_effects, effect_spec),
            )

        else:
            new_ops[op] = attrs.evolve(cur, effects=(*cur.effects, effect_spec))

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def resolve(self, op: str, ctx: UsecaseContext, default: UsecaseFactory[U]) -> U:
        plan = self.ops.get(op)
        factory = default

        if plan and plan.override:
            factory = cast(UsecaseFactory[U], plan.override)

        uc = factory(ctx)

        if plan:
            guards = tuple(
                gs.guard(ctx) for gs in sorted(plan.guards, key=lambda x: x.priority)
            )
            effects = tuple(
                es.effect(ctx) for es in sorted(plan.effects, key=lambda x: x.priority)
            )
            side_guards = tuple(
                gs.guard(ctx)
                for gs in sorted(plan.side_guards, key=lambda x: x.priority)
            )
            side_effects = tuple(
                es.effect(ctx)
                for es in sorted(plan.side_effects, key=lambda x: x.priority)
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

                #! TODO: introduce explicit on_conflict or so with warn, error, ignore
                #! so it's clear when side guards and effects treated as usual ones
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
        acc: dict[str, OperationPlan] = {}

        for p in plans:
            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())

                # prevent conflicting builders
                if cur.override and pl.override and cur.override is not pl.override:
                    raise CoreError(f"Conflicting overrides for operation: {op}")

                # override: last wins
                override = pl.override if pl.override is not None else cur.override
                acc[op] = OperationPlan(
                    override=override,
                    guards=(*cur.guards, *pl.guards),
                    effects=(*cur.effects, *pl.effects),
                    side_guards=(*cur.side_guards, *pl.side_guards),
                    side_effects=(*cur.side_effects, *pl.side_effects),
                )

        return cls(ops=acc)

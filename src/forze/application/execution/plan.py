"""Composition plan for application usecases.

This module defines small, composable building blocks that describe how
guards and effects should wrap a concrete :class:`~forze.application.execution.usecase.Usecase`
implementation. Plans are keyed by operation name and can be merged or extended
at composition time.
"""

from typing import Any, Callable, Final, Optional, Self, TypeVar, cast, final

import attrs

from forze.base.errors import CoreError

from .context import ExecutionContext
from .usecase import Effect, Guard, Middleware, TxUsecase, Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that produces a :class:`~forze.application.kernel.usecase.Guard` from a :class:`ExecutionContext`."""

EffectFactory = Callable[[ExecutionContext], Effect[Any, Any]]
"""Factory that produces an :class:`~forze.application.kernel.usecase.Effect` from a :class:`ExecutionContext`."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that produces a :class:`~forze.application.kernel.usecase.Middleware` from a :class:`ExecutionContext`."""

UsecaseFactory = Callable[[ExecutionContext], U]
"""Factory that builds a concrete :class:`~forze.application.kernel.usecase.Usecase` instance from a :class:`ExecutionContext`."""

WILDCARD: Final[str] = "*"  #! ??? how to attach middlewares to all operations?

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardSpec:
    """Specification for a guard attached to an operation plan.

    Guards are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    guard: GuardFactory


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectSpec:
    """Specification for an effect attached to an operation plan.

    Effects are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    effect: EffectFactory


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to an operation plan.

    Middlewares are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    middleware: MiddlewareFactory


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Per-operation composition describing overrides, guards and effects.

    The plan tracks:

    * an optional ``override`` usecase factory
    * ordered guards/effects that wrap the operation
    * "side" guards/effects that execute outside a transaction for
      transactional usecases.
    """

    override: Optional[UsecaseFactory[Any]] = None
    guards: tuple[GuardSpec, ...] = attrs.field(factory=tuple)
    effects: tuple[EffectSpec, ...] = attrs.field(factory=tuple)
    middlewares: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    side_guards: tuple[GuardSpec, ...] = attrs.field(factory=tuple)
    side_effects: tuple[EffectSpec, ...] = attrs.field(factory=tuple)

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self):
        """Merge multiple plans into a single aggregate plan.

        :param plans: Plans to merge.
        :returns: A new :class:`OperationPlan` with combined operations.
        """

        acc: OperationPlan = OperationPlan()

        for plan in plans:
            # prevent conflicting builders
            if acc.override and plan.override and acc.override is not plan.override:
                raise CoreError("Conflicting overrides for operation")

            override = plan.override if plan.override is not None else acc.override
            acc = OperationPlan(
                override=override,
                guards=(*acc.guards, *plan.guards),
                effects=(*acc.effects, *plan.effects),
                middlewares=(*acc.middlewares, *plan.middlewares),
                side_guards=(*acc.side_guards, *plan.side_guards),
                side_effects=(*acc.side_effects, *plan.side_effects),
            )

        return acc


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    """Mutable description of how usecases for operations are composed.

    A plan is a mapping from operation name to :class:`OperationPlan`. It can
    be extended with additional guards and effects or merged with other plans
    to form a final composition.
    """

    ops: dict[str, OperationPlan] = attrs.field(factory=dict)

    # ....................... #

    def _get_base_plan(self):
        return self.ops.get(WILDCARD, OperationPlan())

    # ....................... #

    def override(self, op: str, factory: UsecaseFactory[U]) -> Self:
        """Override the base usecase factory for a specific operation.

        :param op: Logical operation name (e.g. ``"get"`` or ``"search"``).
        :param factory: Factory that builds the concrete usecase.
        :returns: A new :class:`UsecasePlan` with the override applied.
        """

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Override on wildcard operation `{op}` is not allowed")

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
        """Attach a guard to run before a usecase for an operation.

        :param op: Logical operation name.
        :param guard: Factory for the guard to attach.
        :param priority: Sorting key; higher values run earlier.
        :param side: When ``True``, attach as a side guard that will run
            outside transactions for :class:`~forze.application.kernel.usecase.TxUsecase`
            instances.
        :returns: A new :class:`UsecasePlan` with the guard added.
        """

        cur = self.ops.get(op, OperationPlan())
        new_ops = dict(self.ops)
        guard_spec = GuardSpec(priority=priority, guard=guard)

        if side:
            new_ops[op] = attrs.evolve(cur, side_guards=(*cur.side_guards, guard_spec))

        else:
            new_ops[op] = attrs.evolve(cur, guards=(*cur.guards, guard_spec))

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def wrap(self, op: str, middleware: MiddlewareFactory, priority: int = 0) -> Self:
        """Attach a middleware to wrap a usecase for an operation.

        :param op: Logical operation name.
        :param middleware: Factory for the middleware to attach.
        :param priority: Sorting key; higher values run earlier.
        :returns: A new :class:`UsecasePlan` with the middleware added.
        """

        cur = self.ops.get(op, OperationPlan())
        middleware_spec = MiddlewareSpec(priority=priority, middleware=middleware)

        new_ops = dict(self.ops)
        new_ops[op] = attrs.evolve(cur, middlewares=(*cur.middlewares, middleware_spec))

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
        """Attach an effect to run after a usecase for an operation.

        :param op: Logical operation name.
        :param effect: Factory for the effect to attach.
        :param priority: Sorting key; higher values run earlier.
        :param side: When ``True``, attach as a side effect that will run
            outside transactions for :class:`~forze.application.kernel.usecase.TxUsecase`
            instances.
        :returns: A new :class:`UsecasePlan` with the effect added.
        """

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

    def resolve(self, op: str, ctx: ExecutionContext, default: UsecaseFactory[U]) -> U:
        """Build a composed usecase instance for an operation.

        The method chooses an override factory when configured, instantiates
        the usecase with the provided :class:`ExecutionContext`, and then applies
        guards/effects (including side variants for transactional usecases) in
        priority order.

        :param op: Logical operation name.
        :param ctx: Context that provides dependencies and access to infrastructure.
        :param default: Fallback factory when no override is configured.
        :returns: A composed :class:`~forze.application.kernel.usecase.Usecase`
            instance ready to be invoked.
        """

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Resolve on wildcard operation `{op}` is not allowed")

        base_plan = self._get_base_plan()
        op_plan = self.ops.get(op, base_plan)
        plan = OperationPlan.merge(base_plan, op_plan)

        factory = default

        if plan and plan.override:
            factory = cast(UsecaseFactory[U], plan.override)

        uc = factory(ctx)

        if plan:
            guards = tuple(
                gs.guard(ctx)
                for gs in sorted(
                    plan.guards,
                    key=lambda x: x.priority,
                    reverse=True,
                )
            )
            middlewares = tuple(
                ms.middleware(ctx)
                for ms in sorted(
                    plan.middlewares,
                    key=lambda x: x.priority,
                    reverse=True,
                )
            )
            effects = tuple(
                es.effect(ctx)
                for es in sorted(
                    plan.effects,
                    key=lambda x: x.priority,
                    reverse=True,
                )
            )
            side_guards = tuple(
                gs.guard(ctx)
                for gs in sorted(
                    plan.side_guards,
                    key=lambda x: x.priority,
                    reverse=True,
                )
            )
            side_effects = tuple(
                es.effect(ctx)
                for es in sorted(
                    plan.side_effects,
                    key=lambda x: x.priority,
                    reverse=True,
                )
            )

            if isinstance(uc, TxUsecase):
                uc = (
                    uc.with_guards(*guards)
                    .with_middlewares(*middlewares)
                    .with_effects(*effects)
                    .with_side_guards(*side_guards)
                    .with_side_effects(*side_effects)
                )

            else:
                #! TODO: log warning that side guards and effects are not supported for non-tx usecases
                #! and they will be translated into guards and effects

                #! TODO: introduce explicit on_conflict or so with warn, error, ignore
                #! so it's clear when side guards and effects treated as usual ones
                uc = (
                    uc.with_effects(
                        *effects,
                        *side_effects,
                    )
                    .with_guards(
                        *side_guards,
                        *guards,
                    )
                    .with_middlewares(
                        *middlewares,
                    )
                )

        return uc

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self) -> Self:
        """Merge multiple plans into a single aggregate plan.

        Later plans override earlier ones when both define an override factory
        for the same operation; guards and effects are concatenated in the
        order plans are provided.

        :param plans: Plans to merge.
        :returns: A new :class:`UsecasePlan` with combined operations.
        :raises CoreError: If two plans provide incompatible overrides for the
            same operation (different factory objects).
        """

        acc: dict[str, OperationPlan] = {}

        for p in plans:
            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())
                acc[op] = OperationPlan.merge(cur, pl)

        return cls(ops=acc)

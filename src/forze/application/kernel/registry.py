from typing import Any, Callable, Optional, Self, TypeVar, cast

import attrs

from forze.base.errors import CoreError

from .dependencies import UsecaseContext
from .plan import UsecasePlan
from .usecase import Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

UsecaseFactory = Callable[[UsecaseContext], U]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecaseRegistry:
    defaults: dict[str, UsecaseFactory[Any]] = attrs.field(factory=dict)
    plan: UsecasePlan = attrs.field(factory=UsecasePlan, init=False, repr=False)
    overriden: frozenset[str] = attrs.field(factory=frozenset, init=False, repr=False)

    # ....................... #

    def register(self, op: str, factory: UsecaseFactory[Any]) -> Self:
        if op in self.defaults:
            raise CoreError(
                f"Usecase factory is already registered for operation: {op}"
            )

        new = dict(self.defaults)
        new[op] = factory

        return attrs.evolve(self, defaults=new)

    # ....................... #

    def override(self, op: str, factory: UsecaseFactory[Any]) -> Self:
        if op not in self.defaults:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        new = dict(self.defaults)
        new[op] = factory

        return attrs.evolve(
            self,
            defaults=new,
            overriden=frozenset({*self.overriden, op}),
        )

    # ....................... #

    def register_many(self, ops: dict[str, UsecaseFactory[Any]]) -> Self:
        already_registered = set(self.defaults.keys()).intersection(ops.keys())

        if already_registered:
            raise CoreError(
                f"Usecase factories are already registered for operations: {already_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        return attrs.evolve(self, defaults=new)

    # ....................... #

    def override_many(self, ops: dict[str, UsecaseFactory[Any]]) -> Self:
        not_yet_registered = set(ops.keys()).difference(self.defaults.keys())

        if not_yet_registered:
            raise CoreError(
                f"Usecase factories are not registered for operations: {not_yet_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        return attrs.evolve(
            self,
            defaults=new,
            overriden=frozenset({*self.overriden, *ops.keys()}),
        )

    # ....................... #

    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        allow_override_on_overriden: bool = False,
    ) -> Self:
        if not allow_override_on_overriden:
            for op, pl in extra.ops.items():
                if pl.override is not None and op in self.overriden:
                    raise CoreError(
                        f"Plan override for '{op}' conflicts with registry override. "
                        "Use allow_override_on_overridden=True explicitly."
                    )

        merged = UsecasePlan.merge(self.plan, extra)

        return attrs.evolve(self, plan=merged)

    # ....................... #

    def exists(self, op: str) -> bool:
        return op in self.defaults

    # ....................... #

    def resolve(
        self,
        op: str,
        ctx: UsecaseContext,
        *,
        expected: Optional[type[U]] = None,
    ) -> U:
        factory = self.defaults.get(op)

        if not factory:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        uc = self.plan.resolve(op, ctx, cast(UsecaseFactory[U], factory))

        if expected is not None and not isinstance(uc, expected):
            raise CoreError(f"Usecase '{op}' has unexpected type: {type(uc)!r}")

        return uc

    #! TODO: add resolve_tx method separately !

from typing import Any, Callable, Literal, Optional, Self, TypeVar, cast, overload

import attrs

from forze.base.errors import CoreError

from .dependencies import UsecaseContext
from .plan import UsecasePlan
from .usecase import Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

UsecaseFactory = Callable[[UsecaseContext], U]

# ....................... #


@attrs.define(slots=True)
class UsecaseRegistry:
    defaults: dict[str, UsecaseFactory[Any]] = attrs.field(factory=dict)

    # Non initable fields
    __plan: UsecasePlan = attrs.field(factory=UsecasePlan, init=False, repr=False)
    __overriden: frozenset[str] = attrs.field(factory=frozenset, init=False, repr=False)

    # ....................... #

    @overload
    def register(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[True],
    ) -> None: ...

    @overload
    def register(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[False] = False,
    ) -> Self: ...

    def register(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        if op in self.defaults:
            raise CoreError(
                f"Usecase factory is already registered for operation: {op}"
            )

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            return

        else:
            new_instance = type(self)(defaults=new)
            return new_instance

    # ....................... #

    @overload
    def override(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[True],
    ) -> None: ...

    @overload
    def override(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[False] = False,
    ) -> Self: ...

    def override(
        self,
        op: str,
        factory: UsecaseFactory[Any],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        if op not in self.defaults:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            self.__overriden = frozenset({*self.__overriden, op})
            return

        else:
            new_instance = type(self)(defaults=new)
            new_instance.__overriden = frozenset({*self.__overriden, op})
            return new_instance

    # ....................... #

    @overload
    def register_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: Literal[True],
    ) -> None: ...

    @overload
    def register_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: Literal[False] = False,
    ) -> Self: ...

    def register_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        already_registered = set(self.defaults.keys()).intersection(ops.keys())

        if already_registered:
            raise CoreError(
                f"Usecase factories are already registered for operations: {already_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            return

        else:
            new_instance = type(self)(defaults=new)
            return new_instance

    # ....................... #

    @overload
    def override_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: Literal[True],
    ) -> None: ...
    @overload
    def override_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: Literal[False] = False,
    ) -> Self: ...

    def override_many(
        self,
        ops: dict[str, UsecaseFactory[Any]],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        not_yet_registered = set(ops.keys()).difference(self.defaults.keys())

        if not_yet_registered:
            raise CoreError(
                f"Usecase factories are not registered for operations: {not_yet_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            self.__overriden = frozenset({*self.__overriden, *ops.keys()})
            return

        else:
            new_instance = type(self)(defaults=new)
            new_instance.__overriden = frozenset({*self.__overriden, *ops.keys()})
            return new_instance

    # ....................... #

    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[True],
        allow_override_on_overriden: bool = False,
    ) -> None: ...
    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[False] = False,
        allow_override_on_overriden: bool = False,
    ) -> Self: ...

    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: bool = False,
        allow_override_on_overriden: bool = False,
    ) -> Optional[Self]:
        if not allow_override_on_overriden:
            for op, pl in extra.ops.items():
                if pl.override is not None and op in self.__overriden:
                    raise CoreError(
                        f"Plan override for '{op}' conflicts with registry override. "
                        "Use allow_override_on_overridden=True explicitly."
                    )

        merged = UsecasePlan.merge(self.__plan, extra)

        if inplace:
            self.__plan = merged
            return

        else:
            new_instance = type(self)(defaults=self.defaults)
            new_instance.__plan = merged

            return new_instance

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

        uc = self.__plan.resolve(op, ctx, cast(UsecaseFactory[U], factory))

        if expected is not None and not isinstance(uc, expected):
            raise CoreError(f"Usecase '{op}' has unexpected type: {type(uc)!r}")

        return uc

    #! TODO: add resolve_tx method separately !

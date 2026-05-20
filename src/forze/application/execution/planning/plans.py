from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterable, Never, Self

import attrs

from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from ..core.contracts import OnSuccess
from .binders import ScopeBinder, TransactionScopeBinder
from .scopes import (
    FrozenScope,
    FrozenTransactionScope,
    ResolvedScope,
    ResolvedTransactionScope,
    Scope,
    TransactionScope,
)
from .steps import DispatchStep

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


def _root_commit_fn(x: Any) -> Never:
    raise CoreError("Cannot commit a plan to the root")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Operation plan for a distinct operation."""

    _outer: Scope = attrs.field(factory=Scope, alias="outer")
    """Outer scope for this operation."""

    _tx: TransactionScope = attrs.field(factory=TransactionScope, alias="tx")
    """Transaction scope for this operation."""

    # ....................... #

    def iter_dispatch(self) -> Iterable[StrKey]:
        for step in self._outer.dispatch.items:
            yield step.target

        for step in self._tx.dispatch.items:
            yield step.target

        for step in self._tx.dispatch_after_commit.items:
            yield step.target

    # ....................... #

    def bind_outer(self) -> ScopeBinder[Self, Never]:
        """Enter an outer scope and return a binder for it."""

        return ScopeBinder(
            parent=self,
            source=self._outer,
            commit_fn=lambda p, s: attrs.evolve(p, outer=s),
            root_commit_fn=_root_commit_fn,
        )

    # ....................... #

    def bind_tx(self) -> TransactionScopeBinder[Self, Never]:
        """Enter a transaction scope and return a binder for it."""

        return TransactionScopeBinder(
            parent=self,
            source=self._tx,
            commit_fn=lambda p, s: attrs.evolve(p, tx=s),
            root_commit_fn=_root_commit_fn,
        )

    # ....................... #

    def freeze(self) -> FrozenOperationPlan:
        frozen_outer = self._outer.freeze()
        frozen_tx = self._tx.freeze()

        return FrozenOperationPlan(outer=frozen_outer, tx=frozen_tx)

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *plans: Self) -> Self:  # type: ignore[misc, override]
        """Merge multiple operation plans into a single plan."""

        merged_outer = Scope.merge(*[plan._outer for plan in plans])
        merged_tx = TransactionScope.merge(*[plan._tx for plan in plans])

        return cls(outer=merged_outer, tx=merged_tx)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *plans: Self) -> Self:  # type: ignore[misc, override]
        return type(self).merge(self, *plans)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenOperationPlan:
    """Frozen operation plan."""

    outer: FrozenScope = attrs.field(factory=FrozenScope)
    """Frozen outer scope for this operation."""

    tx: FrozenTransactionScope = attrs.field(factory=FrozenTransactionScope)
    """Frozen transaction scope for this operation."""

    # ....................... #

    def resolve(
        self,
        ctx: "ExecutionContext",
        dispatch_resolver: Callable[
            [DispatchStep, "ExecutionContext"],
            OnSuccess[Any, Any],
        ],
    ) -> ResolvedOperationPlan:
        resolved_outer = self.outer.resolve(ctx, dispatch_resolver)
        resolved_tx = self.tx.resolve(ctx, dispatch_resolver)

        return ResolvedOperationPlan(outer=resolved_outer, tx=resolved_tx)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedOperationPlan:
    """Resolved operation plan."""

    outer: ResolvedScope = attrs.field(factory=ResolvedScope)
    """Resolved outer scope for this operation."""

    tx: ResolvedTransactionScope = attrs.field(factory=ResolvedTransactionScope)
    """Resolved transaction scope for this operation."""

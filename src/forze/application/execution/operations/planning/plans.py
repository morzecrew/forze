from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Callable, Iterable, Never, Self

import attrs

from forze.application.contracts.execution import (
    DispatchStep,
    MiddlewareStep,
    OnSuccess,
)
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .binders import ScopeBinder, TransactionScopeBinder
from .scopes import (
    FrozenScope,
    FrozenTransactionScope,
    ResolvedScope,
    ResolvedTransactionScope,
    Scope,
    TransactionScope,
)

if TYPE_CHECKING:
    from ...context import ExecutionContext

# ----------------------- #


def _root_commit_fn(x: Any) -> Never:
    raise exc.internal("Cannot commit a plan to the root")


# ....................... #


class OperationKind(StrEnum):
    """Whether an operation reads (``QUERY``) or writes (``COMMAND``).

    The default is ``COMMAND`` (read-write). A ``QUERY`` operation is forbidden from
    acquiring a command (write) port — enforced when the read-only flag is bound for its
    duration (see ``InvocationContext.bind_read_only``).
    """

    COMMAND = "command"
    QUERY = "query"


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Operation plan for a distinct operation."""

    _outer: Scope = attrs.field(factory=Scope, alias="outer")
    """Outer scope for this operation."""

    _tx: TransactionScope = attrs.field(factory=TransactionScope, alias="tx")
    """Transaction scope for this operation."""

    kind: OperationKind = attrs.field(default=OperationKind.COMMAND)
    """Read (``QUERY``) vs write (``COMMAND``) classification; defaults to ``COMMAND``."""

    # ....................... #

    def iter_dispatch(self) -> Iterable[StrKey]:
        for step in self._outer.dispatch.items:
            yield step.target

        for step in self._tx.dispatch.items:
            yield step.target

        for step in self._tx.dispatch_after_commit.items:
            yield step.target

    # ....................... #

    def tx_requires_route(self) -> bool:
        """Return whether the transaction scope has stages that require a route."""

        return self._tx.has_stages()

    # ....................... #

    def tx_route(self) -> StrKey | None:
        """Transaction route for this plan, if set."""

        return self._tx.route

    # ....................... #

    def iter_wrap_steps(self) -> Iterable[MiddlewareStep]:
        """Yield every middleware wrap step across the plan's scopes."""

        yield from self._outer.wrap.items
        yield from self._tx.wrap.items

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

        return FrozenOperationPlan(outer=frozen_outer, tx=frozen_tx, kind=self.kind)

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *plans: Self) -> Self:  # type: ignore[misc, override]
        """Merge multiple operation plans into a single plan."""

        merged_outer = Scope.merge(*[plan._outer for plan in plans])
        merged_tx = TransactionScope.merge(*[plan._tx for plan in plans])

        # Restrictive wins: a QUERY in any merged layer keeps the operation read-only.
        merged_kind = (
            OperationKind.QUERY
            if any(plan.kind is OperationKind.QUERY for plan in plans)
            else OperationKind.COMMAND
        )

        return cls(outer=merged_outer, tx=merged_tx, kind=merged_kind)

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

    kind: OperationKind = attrs.field(default=OperationKind.COMMAND)
    """Read (``QUERY``) vs write (``COMMAND``) classification."""

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

        return ResolvedOperationPlan(
            outer=resolved_outer, tx=resolved_tx, kind=self.kind
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedOperationPlan:
    """Resolved operation plan."""

    outer: ResolvedScope = attrs.field(factory=ResolvedScope)
    """Resolved outer scope for this operation."""

    tx: ResolvedTransactionScope = attrs.field(factory=ResolvedTransactionScope)
    """Resolved transaction scope for this operation."""

    kind: OperationKind = attrs.field(default=OperationKind.COMMAND)
    """Read (``QUERY``) vs write (``COMMAND``) classification."""

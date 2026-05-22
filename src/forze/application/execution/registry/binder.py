from typing import TYPE_CHECKING, Self

import attrs

from forze.base.primitives import StrKey

from ..planning.binders import ScopeBinder, TransactionScopeBinder
from ..planning.plans import OperationPlan

if TYPE_CHECKING:
    from ..planning.scopes import Scope, TransactionScope
    from .registries import OperationRegistry

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRegistryBinder:
    """Binder for an operation registry."""

    _parent: "OperationRegistry" = attrs.field(alias="parent")
    """Parent for this binder."""

    _ops: set[StrKey] = attrs.field(alias="ops")
    """Operations to bind for this binder."""

    _acc: OperationPlan = attrs.field(alias="acc", factory=OperationPlan)
    """Accumulated plan for this binder."""

    # ....................... #

    def finish(self) -> "OperationRegistry":
        """Finish binding and return updated operation registry."""

        plans = self._parent.get_plans()

        for op in self._ops:
            if op in plans:
                plans[op] = plans[op].merge(self._acc)

            else:
                plans[op] = self._acc

        new_parent = attrs.evolve(self._parent, plans=plans)

        return new_parent

    # ....................... #

    def bind_outer(self) -> ScopeBinder[Self, "OperationRegistry"]:
        """Enter an outer scope for all operations assigned with this binder."""

        def _commit_fn(parent: Self, scope: "Scope") -> Self:
            old_acc = parent._acc
            new_acc = attrs.evolve(old_acc, outer=scope)

            return attrs.evolve(parent, acc=new_acc)

        return ScopeBinder(
            parent=self,
            source=self._acc._outer,  # type: ignore
            commit_fn=_commit_fn,
            root_commit_fn=lambda p: p.finish(),
        )

    # ....................... #

    def bind_tx(self) -> TransactionScopeBinder[Self, "OperationRegistry"]:
        """Enter a transaction scope for all operations assigned with this binder."""

        def _commit_fn(parent: Self, scope: "TransactionScope") -> Self:
            old_acc = parent._acc
            new_acc = attrs.evolve(old_acc, tx=scope)

            return attrs.evolve(parent, acc=new_acc)

        return TransactionScopeBinder(
            parent=self,
            source=self._acc._tx,  # type: ignore
            commit_fn=_commit_fn,
            root_commit_fn=lambda p: p.finish(),
        )

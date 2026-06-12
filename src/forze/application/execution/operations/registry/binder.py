from datetime import timedelta
from typing import TYPE_CHECKING, Self

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeySelector

from ..planning.binders import ScopeBinder, TransactionScopeBinder
from ..planning.plans import OperationKind, OperationPlan

if TYPE_CHECKING:
    from ..planning.scopes import Scope, TransactionScope
    from .registries import OperationRegistry

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRegistryBinder:
    """Binder for an operation registry."""

    _parent: "OperationRegistry" = attrs.field(alias="parent")
    """Parent for this binder."""

    _ops: set[StrKey] | None = attrs.field(default=None, alias="ops")
    """Operations to bind for this binder (bind mode)."""

    _patch_selector: StrKeySelector.Spec | None = attrs.field(
        default=None,
        alias="patch_selector",
    )
    """Selector for plan patches (patch mode)."""

    _acc: OperationPlan = attrs.field(alias="acc", factory=OperationPlan)
    """Accumulated plan for this binder."""

    # ....................... #

    def finish(self) -> "OperationRegistry":
        """Finish binding and return updated operation registry."""

        if self._patch_selector is not None:
            return self._parent.commit_patch(self._patch_selector, self._acc)

        if not self._ops:
            raise exc.internal("No operations provided")

        plans = self._parent.get_plans()

        for op in self._ops:
            if op in plans:
                plans[op] = plans[op].merge(self._acc)

            else:
                plans[op] = self._acc

        new_parent = attrs.evolve(self._parent, plans=plans)

        return new_parent

    # ....................... #

    def as_query(self) -> Self:
        """Mark these operations as read-only (``QUERY``).

        A read-only operation may not acquire a command (write) port — enforced for the
        operation's duration. Place early in the chain, e.g.
        ``registry.bind(op).as_query().bind_tx().set_route("pg").finish()``.
        """

        return attrs.evolve(
            self, acc=attrs.evolve(self._acc, kind=OperationKind.QUERY)
        )

    # ....................... #

    def as_command(self) -> Self:
        """Mark these operations as read-write (``COMMAND``) — the default."""

        return attrs.evolve(
            self, acc=attrs.evolve(self._acc, kind=OperationKind.COMMAND)
        )

    # ....................... #

    def with_deadline(self, deadline: timedelta) -> Self:
        """Declare a per-invocation time budget for these operations.

        Bound at operation entry, so the budget covers the whole plan (hooks,
        transaction, dispatch) and propagates to dispatched operations; expiry
        raises a non-retryable ``TIMEOUT`` (``code="deadline_exceeded"``).
        Merge is restrictive: across patches, explicit plans, and a
        caller-bound deadline, the tightest budget wins — a layer can shorten
        it, never extend it. Works in patch mode too, e.g.
        ``registry.patch(selector).with_deadline(timedelta(seconds=10)).finish()``
        for a fleet-wide default.
        """

        return attrs.evolve(self, acc=attrs.evolve(self._acc, deadline=deadline))

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

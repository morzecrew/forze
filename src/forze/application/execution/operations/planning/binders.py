from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, Protocol, Self, overload

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .scopes import Scope, TransactionScope

if TYPE_CHECKING:
    from forze.application.contracts.execution import (
        BeforeStep,
        DispatchStep,
        FinallyStep,
        MiddlewareStep,
        OnFailureStep,
        OnSuccessStep,
    )

# ----------------------- #


class _Parent(Protocol):
    """Parent interface protocol."""

    def bind_outer(self) -> ScopeBinder[Self, Any]: ...
    def bind_tx(self) -> TransactionScopeBinder[Self, Any]: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ScopeBinder[P: _Parent, R]:
    """Binder for a scope plan."""

    _parent: P = attrs.field(alias="parent")
    """Parent for this binder."""

    _source: Scope = attrs.field(alias="source")
    """Source plan for this binder."""

    _acc: Scope = attrs.field(alias="acc", factory=Scope)
    """Accumulator plan for this binder."""

    _commit_fn: Callable[[P, Scope], P] = attrs.field(alias="commit_fn")
    """Function to commit the plan back to the parent."""

    _root_commit_fn: Callable[[P], R] | None = attrs.field(
        default=None,
        alias="root_commit_fn",
    )
    """Function to commit the plan to the root."""

    # ....................... #

    def _patch_acc(self, acc: Scope) -> Self:
        """Patch the accumulator plan."""

        return attrs.evolve(self, acc=acc)

    # ....................... #

    def wrap(self, *steps: "MiddlewareStep") -> Self:
        """Add wrap steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            wrap=self._acc.wrap.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def finally_(self, *steps: "FinallyStep") -> Self:
        """Add finally steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            finally_=self._acc.finally_.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def on_failure(self, *steps: "OnFailureStep") -> Self:
        """Add on failure steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            on_failure=self._acc.on_failure.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def before(self, *steps: "BeforeStep") -> Self:
        """Add before steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            before=self._acc.before.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def on_success(self, *steps: "OnSuccessStep") -> Self:
        """Add on success steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            on_success=self._acc.on_success.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def dispatch(self, *steps: "DispatchStep") -> Self:
        """Add dispatch steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            dispatch=self._acc.dispatch.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    @overload
    def finish(self, *, deep: Literal[False] = False) -> P:
        """Finish binding and return updated parent."""
        ...

    @overload
    def finish(self, *, deep: Literal[True]) -> R:
        """Finish binding and return updated root."""
        ...

    def finish(self, *, deep: bool = False) -> P | R:
        """Finish binding and return updated parent or root."""

        new_plan = self._source.merge(self._acc)
        new_parent = self._commit_fn(self._parent, new_plan)

        if not deep:
            return new_parent

        if self._root_commit_fn is None:
            raise exc.internal(
                "Cannot finish a scope binder without a root commit function"
            )

        return self._root_commit_fn(new_parent)

    # ....................... #

    def bind_outer(self) -> ScopeBinder[P, R]:
        """Enter an outer scope and return a binder for it."""

        new_parent = self.finish(deep=False)

        return new_parent.bind_outer()

    # ....................... #

    def bind_tx(self) -> TransactionScopeBinder[P, R]:
        """Enter a transaction scope and return a binder for it."""

        new_parent = self.finish(deep=False)

        return new_parent.bind_tx()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TransactionScopeBinder[P: _Parent, R](ScopeBinder[P, R]):
    """Binder for a transaction scope plan."""

    _source: TransactionScope = attrs.field(alias="source")
    """Source plan for this binder."""

    _acc: TransactionScope = attrs.field(alias="acc", factory=TransactionScope)
    """Accumulator plan for this binder."""

    _commit_fn: Callable[[P, TransactionScope], P] = attrs.field(alias="commit_fn")  # type: ignore[override, assignment]
    """Function to commit the plan back to the parent."""

    _root_commit_fn: Callable[[P], R] | None = attrs.field(
        default=None,
        alias="root_commit_fn",
    )
    """Function to commit the plan to the root."""

    # ....................... #

    def after_commit(self, *steps: "OnSuccessStep") -> Self:
        """Add after commit steps to the plan.

        After-commit steps run *after* the root transaction has committed. Every
        registered callback runs even when earlier ones fail; failures are logged
        and aggregated into a single ``after_commit_failed`` internal error raised
        once all callbacks ran. A callback failure does **not** roll back the
        (already committed) transaction.
        """

        new_acc = attrs.evolve(
            self._acc,
            after_commit=self._acc.after_commit.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def dispatch_after_commit(self, *steps: "DispatchStep") -> Self:
        """Add dispatch after commit steps to the plan."""

        new_acc = attrs.evolve(
            self._acc,
            dispatch_after_commit=self._acc.dispatch_after_commit.add(*steps),
        )

        return self._patch_acc(new_acc)

    # ....................... #

    def set_route(self, route: StrKey) -> Self:
        """Set the transaction route."""

        new_acc = attrs.evolve(self._acc, route=route)

        return self._patch_acc(new_acc)

    # ....................... #

    def reset_route(self) -> Self:
        """Reset the transaction route."""

        new_acc = attrs.evolve(self._acc, route=None)

        return self._patch_acc(new_acc)

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Never, Self

import attrs

from forze.application.contracts.execution import (
    BeforeStep,
    DeclaresAuthn,
    DeclaresAuthz,
    DispatchStep,
    MiddlewareStep,
    OnSuccess,
    OnSuccessStep,
    ProvidesIdempotency,
    SuppliesTransactionCommit,
)
from forze.application.contracts.transaction import IsolationLevel
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

    two_phase: bool = attrs.field(default=False)
    """Whether the handler is a two-phase ``prepare``/``apply`` handler.

    When ``True``, the engine runs ``handler.prepare(args)`` in the outer scope —
    outside the transaction, under the read-only flag — and threads its payload
    into ``handler.apply(args, payload)`` inside the transaction. ``prepare`` runs
    exactly once per invocation even under retry/hedge. Requires a transaction
    route (validated at freeze)."""

    deadline: timedelta | None = attrs.field(default=None)
    """Per-invocation time budget for this operation, or ``None`` for no cap.

    Bound at operation entry via the task-scoped deadline (see
    ``context.deadline``), so it covers the whole plan — hooks, transaction,
    dispatch chains — and propagates to dispatched operations. Tighten-only
    against a caller-bound deadline: the earlier of the two wins, so a caller
    can shorten the budget but never extend it past the plan's cap. Expiry
    raises a non-retryable ``TIMEOUT`` (``code="deadline_exceeded"``).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.deadline is not None and self.deadline.total_seconds() <= 0:
            raise exc.configuration("Operation deadline must be positive")

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

    def tx_isolation(self) -> IsolationLevel | None:
        """Required transaction isolation for this plan, if declared."""

        return self._tx.isolation

    # ....................... #

    def iter_wrap_steps(self) -> Iterable[MiddlewareStep]:
        """Yield every middleware wrap step across the plan's scopes."""

        yield from self._outer.wrap.items
        yield from self._tx.wrap.items

    # ....................... #

    def iter_before_steps(self) -> Iterable[BeforeStep]:
        """Yield every before step across the plan's scopes."""

        yield from self._outer.before.items
        yield from self._tx.before.items

    # ....................... #

    def supports_idempotency_key(self) -> bool:
        """Whether the plan carries a wrap that deduplicates on a bound idempotency key.

        Structural detection (:class:`ProvidesIdempotency`), shared by the freeze-time
        hedging gate and the operation catalog. "Supports", not "requires": such a wrap
        is a no-op when the invocation binds no idempotency key — it replays a stored
        result only for callers that *do* send one.
        """

        return any(
            isinstance(step.factory, ProvidesIdempotency) and step.factory.provides_idempotency()
            for step in self.iter_wrap_steps()
        )

    # ....................... #

    def declared_permission_keys(self) -> tuple[str, ...]:
        """Sorted union of permission keys declared by the plan's authz hooks.

        Structural detection (:class:`DeclaresAuthz`) over the plan's before and wrap
        steps. Honesty caveat: declared-hook introspection only, **not** a security
        statement — an empty result does not mean the operation is unguarded (its
        handler may enforce authorization invisibly), and a declaring hook may scope
        or deny access beyond its named keys.
        """

        factories: list[Any] = [step.factory for step in self.iter_before_steps()]
        factories += [step.factory for step in self.iter_wrap_steps()]

        keys: set[str] = set()

        for factory in factories:
            if isinstance(factory, DeclaresAuthz):
                keys.update(factory.permission_keys())

        return tuple(sorted(keys))

    # ....................... #

    def requires_authentication(self) -> bool:
        """Whether the plan declares it needs an authenticated principal.

        Structural detection over the before/wrap steps: a :class:`DeclaresAuthn`
        hook that requires authn, **or** any :class:`DeclaresAuthz` hook —
        authorization presupposes a bound principal, so an authz-guarded
        operation requires authentication too. Same honesty caveat as
        :meth:`declared_permission_keys`: declared-hook introspection only, so
        ``False`` does not prove the operation is open.
        """

        factories: list[Any] = [step.factory for step in self.iter_before_steps()]
        factories += [step.factory for step in self.iter_wrap_steps()]

        return any(
            (isinstance(factory, DeclaresAuthn) and factory.requires_authn())
            or isinstance(factory, DeclaresAuthz)
            for factory in factories
        )

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

    def _inject_idempotency_commit(self, tx: TransactionScope) -> TransactionScope:
        """Auto-inject the paired in-transaction record-write hook for an idempotency wrap.

        When the plan carries an idempotency wrap (:class:`ProvidesIdempotency`) *and* has a
        transaction route, add its ``commit_on_success`` factory as an ``on_success`` step on
        the transaction scope, so a co-located store commits the result record atomically
        with the business writes (closing the crash window). No route means nothing to be
        atomic with — the middleware records out of transaction — so nothing is injected;
        adding a tx-scope stage to a routeless plan would also wrongly require a route.
        """

        if tx.route is None:
            return tx

        suppliers = [
            step.factory
            for step in self.iter_wrap_steps()
            if isinstance(step.factory, SuppliesTransactionCommit)
        ]

        # Stable id keyed by position among the *idempotency* wraps only (not all wraps),
        # so it does not shift when unrelated wraps are added; the common single-wrap case
        # is just ``idempotency_commit``. Multiple idempotency wraps are unusual (each dedups
        # the whole op) but get distinct steps rather than colliding.
        injected = [
            OnSuccessStep(
                id="idempotency_commit" if index == 0 else f"idempotency_commit_{index}",
                factory=factory.commit_on_success(),
            )
            for index, factory in enumerate(suppliers)
        ]

        if not injected:
            return tx

        return attrs.evolve(tx, on_success=tx.on_success.add(*injected))

    # ....................... #

    def freeze(self) -> FrozenOperationPlan:
        frozen_outer = self._outer.freeze()
        frozen_tx = self._inject_idempotency_commit(self._tx).freeze()

        return FrozenOperationPlan(
            outer=frozen_outer,
            tx=frozen_tx,
            kind=self.kind,
            two_phase=self.two_phase,
            deadline=self.deadline,
            supports_idempotency_key=self.supports_idempotency_key(),
            required_permissions=self.declared_permission_keys(),
            requires_authn=self.requires_authentication(),
        )

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

        # Restrictive wins (commutative, like ``kind``): the tightest declared
        # deadline caps the operation — a layer can shorten the budget, never
        # extend it. To give one operation a longer budget than a broad patch
        # default, narrow the patch selector instead.
        deadlines = [plan.deadline for plan in plans if plan.deadline is not None]
        merged_deadline = min(deadlines, default=None)

        # Sticky (OR), so it travels with the registration that set it and a
        # default-``False`` patch never clobbers it.
        merged_two_phase = any(plan.two_phase for plan in plans)

        return cls(
            outer=merged_outer,
            tx=merged_tx,
            kind=merged_kind,
            two_phase=merged_two_phase,
            deadline=merged_deadline,
        )

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

    two_phase: bool = attrs.field(default=False)
    """Whether the handler is a two-phase ``prepare``/``apply`` handler."""

    deadline: timedelta | None = attrs.field(default=None)
    """Per-invocation time budget declared by the plan, or ``None`` for no cap."""

    supports_idempotency_key: bool = attrs.field(default=False)
    """Derived at freeze (:meth:`OperationPlan.supports_idempotency_key`): the plan
    carries an idempotency wrap that replays a stored result for a duplicate bound
    idempotency key. The wrap is a no-op when the invocation binds no key."""

    required_permissions: tuple[str, ...] = attrs.field(factory=tuple)
    """Derived at freeze (:meth:`OperationPlan.declared_permission_keys`): sorted union
    of permission keys declared by the plan's authz hooks. Declared-hook introspection
    only, not a security statement — empty does not mean unguarded."""

    requires_authn: bool = attrs.field(default=False)
    """Derived at freeze (:meth:`OperationPlan.requires_authentication`): the plan
    declares it needs a bound principal (an authn guard, or any authz hook — authz
    presupposes authn). Declared-hook introspection only — ``False`` does not mean
    the operation is open."""

    # ....................... #

    def resolve(
        self,
        ctx: ExecutionContext,
        dispatch_resolver: Callable[
            [DispatchStep, ExecutionContext],
            OnSuccess[Any, Any],
        ],
    ) -> ResolvedOperationPlan:
        resolved_outer = self.outer.resolve(ctx, dispatch_resolver)
        resolved_tx = self.tx.resolve(ctx, dispatch_resolver)

        return ResolvedOperationPlan(
            outer=resolved_outer,
            tx=resolved_tx,
            kind=self.kind,
            two_phase=self.two_phase,
            # Seconds precomputed once at resolve so the per-call hot path
            # never touches timedelta arithmetic.
            deadline_s=(None if self.deadline is None else self.deadline.total_seconds()),
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

    two_phase: bool = attrs.field(default=False)
    """Whether the handler is a two-phase ``prepare``/``apply`` handler — the engine
    runs ``prepare`` outside the transaction and ``apply`` inside it."""

    deadline_s: float | None = attrs.field(default=None)
    """Plan-declared time budget in seconds (precomputed at resolve), or ``None``."""

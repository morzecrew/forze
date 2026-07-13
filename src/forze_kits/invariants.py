"""Runtime enforcement of a :class:`~forze.application.contracts.invariants.SystemInvariant`.

:func:`evaluate` reads the read-set's aggregate under the current context and checks the predicate,
returning a structured :class:`InvariantResult`. :func:`enforce` schedules that check *after* the
writing transaction commits and raises on a violation — the **detective** control. :func:`enforce_preventive`
runs it *inside* the writing transaction under a sufficient isolation floor, so a violation rolls the
write back — the **preventive** control. :func:`propose` is a **dry-run filter** for agent-authoring:
it applies a proposed write and checks the laws inside a transaction, then *rolls back* (nothing
persists), returning a machine-checkable verdict of whether the write would be accepted. The same
:func:`evaluate` kernel underlies all of them and the DST oracle — the *proof* a green ``propose`` is
only a filter against.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Literal, final

import attrs

from forze.application.contracts.execution import (
    OnSuccess,
    OnSuccessFactory,
    OnSuccessStep,
)
from forze.application.contracts.invariants import (
    AGGREGATE_FIELD,
    CountAll,
    SystemInvariant,
    computed_aggregate,
    scope_filter,
)
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class InvariantResult:
    """The outcome of evaluating a :class:`SystemInvariant`: the observed aggregate and whether it held."""

    name: str
    """The invariant's name (for violation messages and oracle provenance)."""

    observed: float
    """The aggregate value read over the read-set — a :class:`SumOf`'s total or a :class:`CountAll`'s cardinality."""

    held: bool
    """Whether :attr:`~forze.application.contracts.invariants.SystemInvariant.holds` accepted the aggregate."""


# ....................... #


async def evaluate(
    invariant: SystemInvariant,
    ctx: ExecutionContext,
    params: Mapping[str, Any],
) -> InvariantResult:
    """Read the read-set's aggregate under *ctx* and check the predicate — a pure read, no enforcement.

    Builds the scope filter from *params*, runs ``count`` (for :class:`CountAll`) or a no-group
    ``$sum`` ``aggregate_many`` (for :class:`SumOf`) over the scoped set, and applies
    ``invariant.holds`` to the aggregate as a ``float``. This is the kernel both :func:`enforce` and
    the DST oracle reuse, so it stays side-effect free (it only reads).
    """

    read_set = invariant.read_set
    filters = scope_filter(read_set, params)
    query = ctx.document.query(read_set.spec)
    aggregate = invariant.aggregate

    if isinstance(aggregate, CountAll):
        observed = float(await query.count(filters))
    else:  # SumOf — a no-group aggregate is the total over the scoped set (one row).
        page = await query.aggregate_many(
            {"$computed": computed_aggregate(aggregate)}, filters=filters
        )
        raw = page.hits[0].get(AGGREGATE_FIELD) if page.hits else 0
        observed = float(raw if raw is not None else 0)

    return InvariantResult(name=invariant.name, observed=observed, held=invariant.holds(observed))


# ....................... #


async def enforce(
    invariant: SystemInvariant,
    ctx: ExecutionContext,
    params: Mapping[str, Any],
) -> None:
    """Detective enforcement: after the writing transaction commits, evaluate the law and raise if it broke.

    Schedules the check via :meth:`ctx.tx_ctx.run_or_defer
    <forze.application.execution.context.transaction.TransactionContext.run_or_defer>` so it observes
    committed state (including the write that prompted it). On violation it raises ``exc.domain`` —
    **detective**, not preventive: the offending write is already durable, so this surfaces the breach
    (via the post-commit machinery), it does not roll it back. Reach for :func:`enforce_preventive`
    when the law must be *prevented* rather than detected.
    """

    async def _check() -> None:
        result = await evaluate(invariant, ctx, params)
        if not result.held:
            raise exc.domain(
                f"system invariant {invariant.name!r} violated: "
                f"aggregate observed {result.observed}"
            )

    # A detective check is a deliberate domain outcome, not a best-effort effect: its
    # violation must surface to the caller even though the writing transaction committed.
    await ctx.tx_ctx.run_or_defer(_check, fatal=True)


# ....................... #


async def enforce_preventive(
    invariant: SystemInvariant,
    ctx: ExecutionContext,
    params: Mapping[str, Any],
) -> None:
    """Preventive enforcement: evaluate the law *inside* the writing transaction and raise before commit.

    Unlike :func:`enforce` (detective, post-commit), this runs immediately, so a violation propagates
    out of the transaction scope and **rolls the write back** — the bad state never becomes durable.
    Call it inside the writing transaction, after the writes::

        async with ctx.tx_ctx.scope(route, isolation=inv.required_isolation):
            ...writes...
            await enforce_preventive(inv, ctx, params)

    It is correct only at or above the law's
    :attr:`~forze.application.contracts.invariants.SystemInvariant.required_isolation`, so it **fails
    closed** (``exc.configuration``) if the active transaction is weaker (or absent) — a caller cannot
    silently get preventive enforcement that a write-skew interleaving would defeat. The
    *backend*-capability half of the gate is already paid when the handler opens
    ``scope(route, isolation=required)`` (that fail-closes against the manager's ``TxCapabilities``);
    against a conformance-verified backend the reported level is checked to match the real engine,
    which is what makes this prevention trustworthy under real concurrency, not just within one tx.
    """

    active = ctx.tx_ctx.current_isolation()
    required = invariant.required_isolation

    if active is None or active < required:
        raise exc.configuration(
            f"preventive enforcement of system invariant {invariant.name!r} requires the writing "
            f"transaction to run at isolation >= {required.name}, but it is at "
            f"{active.name if active is not None else 'no explicit isolation (or no transaction)'} — "
            f"open it with scope(route, isolation={required.name})",
            code="system_invariant_isolation_floor",
        )

    result = await evaluate(invariant, ctx, params)

    if not result.held:
        raise exc.domain(
            f"system invariant {invariant.name!r} would be violated: "
            f"aggregate observed {result.observed}",
            code="system_invariant_violated",
        )


# ....................... #
# bind_invariants — thread enforcement into a write op's plan instead of hand-calling it.


InvariantParams = Callable[[Any, Any], Mapping[str, Any]]
"""Extract a law's scope-key params from a write op's ``(args, result)`` — e.g.
``lambda args, result: {"ledger_id": result.ledger_id}``."""

InvariantMode = Literal["preventive", "detective"]
"""``preventive`` (raise inside the tx, roll the write back) or ``detective`` (defer to post-commit,
surface a committed breach). See :func:`enforce_preventive` / :func:`enforce`."""


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class InvariantEnforcement:
    """A :class:`SystemInvariant` bound to a write op: which law, how to scope it, and how to enforce."""

    law: SystemInvariant
    """The declared law to enforce."""

    params: InvariantParams
    """Maps the op's ``(args, result)`` to the law's scope-key params (see :func:`scope_filter`)."""

    mode: InvariantMode = "preventive"
    """Preventive (in-tx, rollback) or detective (post-commit, surface)."""


def _enforcement_factory(enforcement: InvariantEnforcement) -> OnSuccessFactory:
    law = enforcement.law
    extract = enforcement.params
    preventive = enforcement.mode == "preventive"

    def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        async def _hook(args: Any, result: Any) -> None:
            params = extract(args, result)

            if preventive:
                await enforce_preventive(law, ctx, params)
            else:
                await enforce(law, ctx, params)

        return _hook

    return _factory


def bind_invariants(
    reg: OperationRegistry,
    op_key: StrKey,
    *enforcements: InvariantEnforcement,
    tx_route: StrKey = "default",
) -> OperationRegistry:
    """Thread invariant enforcement into a write op's plan, so the caller stops hand-writing it.

    Each enforcement runs as an on-success step *inside* op *op_key*'s writing transaction, after the
    write, in declaration order: a **preventive** law raises before commit (rolling the write back), a
    **detective** law defers its check to post-commit (surfacing a committed breach). The transaction is
    opened at the strongest preventive law's
    :attr:`~forze.application.contracts.invariants.SystemInvariant.required_isolation`, so the preventive
    floor is met — an op with no preventive law keeps the manager's default isolation. The op becomes
    transactional on *tx_route*, which must resolve a transaction manager. No-op when *enforcements* is
    empty.
    """

    if not enforcements:
        return reg

    plan = reg.bind(op_key).bind_tx().set_route(tx_route)

    preventive = [e.law.required_isolation for e in enforcements if e.mode == "preventive"]
    if preventive:
        plan = plan.set_isolation(max(preventive))

    for index, enforcement in enumerate(enforcements):
        plan = plan.on_success(
            OnSuccessStep(
                id=f"invariant_{enforcement.mode}_{enforcement.law.name}_{index}",
                factory=_enforcement_factory(enforcement),
            )
        )

    return plan.finish(deep=True)


# ....................... #
# propose — a rollback-only dry-run filter for agent-authoring verdicts ("would this write be accepted?").


class _ProposalRollback(Exception):
    """Forces the propose dry-run's transaction to roll back after the checks (nothing persists)."""


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class ProposalVerdict:
    """The outcome of a :func:`propose` dry-run: whether the write would be accepted, and why not.

    **A filter, not a proof.** ``holds`` is "the proposed write applied cleanly and the checked laws
    held *against the state the dry-run saw*" — it is not a guarantee: under concurrency the answer can
    change between the dry-run and a real write (TOCTOU), and it is only as sound as the backend's
    conformance. The *proof* is the DST oracle (``compile_oracle``). Use this to filter/explain a
    proposed write, never to certify it safe.
    """

    holds: bool
    """The write applied without error and every checked law held — the proposal would be accepted."""

    results: tuple[InvariantResult, ...]
    """Per-law outcomes evaluated against the post-write (then rolled-back) state."""

    error: str | None = None
    """The domain/precondition error if the proposed write itself was rejected (then ``results`` is empty)."""

    @property
    def failed(self) -> tuple[str, ...]:
        """Names of the checked laws that would be violated."""

        return tuple(result.name for result in self.results if not result.held)


async def propose(
    ctx: ExecutionContext,
    apply: Callable[[ExecutionContext], Awaitable[None]],
    checks: Sequence[tuple[SystemInvariant, Mapping[str, Any]]],
    *,
    route: StrKey,
    isolation: IsolationLevel | None = None,
) -> ProposalVerdict:
    """Dry-run *apply* and report whether the proposed write would be accepted — **without persisting**.

    Opens its **own root transaction** on *route*, runs *apply* (the proposed write(s), through the
    governed ports so tenancy/encryption/domain guards apply), evaluates each ``(law, params)`` in
    *checks* against the resulting state, then **rolls the transaction back** so nothing the dry-run
    did is durable. Returns a :class:`ProposalVerdict`: ``holds`` true iff the write applied cleanly
    and every law held. A domain/precondition failure from *apply* (the write itself rejected) is
    captured as ``error`` with ``holds`` false and the laws are *not* evaluated; an error from a
    *check* (e.g. a misconfigured scope filter) propagates — that is a setup bug, not a verdict.

    **Must run at the top level** (no active transaction). It **fails closed** (``exc.precondition``)
    if called inside one: a nested scope is only a savepoint, and savepoint-level rollback is not
    guaranteed across backends, so the dry-run's writes could silently ride the enclosing transaction
    and commit — the opposite of a no-side-effect probe. The no-side-effect property also assumes the
    backend rolls a transaction back faithfully (the same conformance horizon as everything here); over
    a backend whose rollback is a no-op, the dry-run's writes can persist.

    **This is a filter, not a proof** — TOCTOU under concurrency, mock-horizon bounded. Never read
    ``holds=True`` as a guarantee; the DST oracle (:func:`~forze_dst.oracle.compile_oracle`) is the
    proof. The rollback is forced by raising an internal sentinel inside the scope.
    """

    if ctx.tx_ctx.depth() > 0:
        raise exc.precondition(
            "propose() must open its own root transaction and cannot run inside an active "
            "transaction scope: a nested scope is a savepoint whose rollback is not guaranteed "
            "across backends, so the dry-run's writes could persist into the enclosing transaction",
            code="propose_inside_transaction",
        )

    results: list[InvariantResult] = []
    apply_error: list[str] = []

    with contextlib.suppress(_ProposalRollback):
        async with ctx.tx_ctx.scope(route, isolation=isolation):
            try:
                await apply(ctx)
            except CoreException as rejection:
                apply_error.append(str(rejection))
            else:
                results.extend([await evaluate(law, ctx, params) for law, params in checks])

            raise _ProposalRollback()  # always roll back — a dry-run persists nothing

    error = apply_error[0] if apply_error else None
    holds = error is None and all(result.held for result in results)

    return ProposalVerdict(holds=holds, results=tuple(results), error=error)

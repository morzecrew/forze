"""Runtime enforcement of a :class:`~forze.application.contracts.invariants.SystemInvariant`.

:func:`evaluate` reads the read-set's aggregate under the current context and checks the predicate,
returning a structured :class:`InvariantResult`. :func:`enforce` schedules that check to run *after*
the writing transaction commits and raises on a violation — the **detective** control (RFC 0012
§4.B): because it runs post-commit, a breach is *reported*, not prevented. The same :func:`evaluate`
kernel is what the DST oracle reuses to verify the law at each committed point (RFC 0012 §4.D).

The preventive mode (evaluate in-transaction under a sufficient isolation floor so a concurrent
violator is rejected) is a later phase; until then ``enforce`` is honestly detective, and the call
site is where the choice is made.
"""

from __future__ import annotations

from typing import Any, Mapping, final

import attrs

from forze.application.contracts.invariants import Count, SystemInvariant
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class InvariantResult:
    """The outcome of evaluating a :class:`SystemInvariant`: the observed aggregate and whether it held."""

    name: str
    """The invariant's name (for violation messages and oracle provenance)."""

    observed: float
    """The aggregate value read over the read-set — a :class:`Sum`'s total or a :class:`Count`'s cardinality."""

    held: bool
    """Whether :attr:`~forze.application.contracts.invariants.SystemInvariant.holds` accepted the aggregate."""


# ....................... #


async def evaluate(
    invariant: SystemInvariant,
    ctx: ExecutionContext,
    params: Mapping[str, Any],
) -> InvariantResult:
    """Read the read-set's aggregate under *ctx* and check the predicate — a pure read, no enforcement.

    Builds the scope filter from *params*, runs ``count`` (for :class:`Count`) or a no-group
    ``$sum`` ``aggregate_many`` (for :class:`Sum`) over the scoped set, and applies
    ``invariant.holds`` to the aggregate as a ``float``. This is the kernel both :func:`enforce` and
    the DST oracle reuse, so it stays side-effect free (it only reads).
    """

    read_set = invariant.read_set
    filters = read_set.scope(params)
    query = ctx.document.query(read_set.spec)
    aggregate = invariant.aggregate

    if isinstance(aggregate, Count):
        observed = float(await query.count(filters))
    else:  # Sum — a no-group aggregate is the global total over the scoped set (one row).
        page = await query.aggregate_many(
            {"$computed": {"value": {"$sum": aggregate.field}}}, filters=filters
        )
        raw = page.hits[0].get("value") if page.hits else 0
        observed = float(raw if raw is not None else 0)

    return InvariantResult(
        name=invariant.name, observed=observed, held=invariant.holds(observed)
    )


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
    (via the post-commit machinery), it does not roll it back. Reach for the preventive mode (RFC
    0012 §4.B) when the law must be *prevented* rather than detected.
    """

    async def _check() -> None:
        result = await evaluate(invariant, ctx, params)
        if not result.held:
            raise exc.domain(
                f"system invariant {invariant.name!r} violated: "
                f"aggregate observed {result.observed}"
            )

    await ctx.tx_ctx.run_or_defer(_check)

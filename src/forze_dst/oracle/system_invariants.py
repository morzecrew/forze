"""Compile a :class:`~forze.application.contracts.invariants.SystemInvariant` into a DST oracle.

A cross-aggregate law (a conservation or cardinality predicate over a read-set, RFC 0012) is declared
once and gets two consumers: ``forze_kits`` *enforces* it at runtime, and this module *verifies* it
under simulation. :func:`compile_oracle` turns one or more laws into a :class:`CompiledOracle` — an
``observe`` hook plus the matching DST :data:`~forze_dst.oracle.invariants.Invariant`\\ s — that a
``Simulation``/``Cluster`` runs like any other oracle.

**v0 — final-state, grouped.** The observe hook runs once over a clean context after all work, issues
**one grouped aggregate** per law (group by the law's :attr:`~forze.application.contracts.invariants.ReadSet.scope_keys`,
reduce by its aggregate), and ``record_event``\\ s the outcome for **every scope the run produced** —
so a generated workload of many orders/ledgers is checked exhaustively, not at a hand-listed few. The
invariant then flags any recorded scope whose aggregate failed the predicate.

Two honest bounds. (1) It sees the *end* state only, so a violation created mid-run and later healed
is missed (the per-commit trace fold is v1, RFC 0012 §4.D). (2) It checks the scopes **present in the
data** — a scope with no matching records produces no group and is not checked; for the
zero-anchored laws this primitive targets (a sum that must be ``0``, a count that must be ``<= 1``)
an absent scope holds trivially, so this is benign, but a *minimum*-style predicate (e.g. a sum that
must be ``>= 100``) would not be caught for a scope that vanished. Within those bounds it is sound.

**Trust boundary (RFC 0004 / RFC 0012 §4.D).** The verdict is only as strong as the conformance
behind the read-set's backend: over the conformance-verified mock (≡ Postgres/Mongo for the isolation
family) a green result refers to the real engine; over an unverified port it is a model tautology for
the concurrency-dependent part. The oracle reads simulated state — it does not itself re-establish
that equivalence.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, final

import attrs

from forze.application.contracts.invariants import (
    AGGREGATE_FIELD,
    SystemInvariant,
    computed_aggregate,
)
from forze.application.contracts.querying import AggregatesExpression
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_dst.oracle.invariants import Invariant, Violation
from forze_dst.oracle.recorder import History, record_event

# ----------------------- #

SYSTEM_INVARIANT_KIND = "system_invariant"
"""The recorder event kind the observe hook stamps for each checked scope."""


@final
@attrs.define(frozen=True, kw_only=True)
class CompiledOracle:
    """A compiled :class:`SystemInvariant` oracle: the ``observe`` hook + its DST invariants.

    Wire it into a run: ``Simulation(..., observe=oracle.observe, invariants=[*oracle.invariants])``
    (same for ``Cluster``). ``observe`` records each scope's aggregate; each invariant flags any
    recorded scope of its law that failed the predicate.
    """

    observe: Callable[[ExecutionContext], Awaitable[None]]
    invariants: tuple[Invariant, ...]


# ....................... #


async def _grouped(
    law: SystemInvariant, ctx: ExecutionContext
) -> list[tuple[dict[str, Any], float]]:
    """One grouped aggregate over the law's read-set in final state: ``(scope, aggregate)`` per group.

    Groups by the law's ``scope_keys`` (a global law — empty keys — yields one whole-collection row),
    filtered by its constant ``where``. The reducer is shared with runtime evaluation via
    :func:`~forze.application.contracts.invariants.computed_aggregate`, so oracle and enforcement
    measure the same thing.
    """

    read_set = law.read_set
    computed = computed_aggregate(law.aggregate)

    aggregates: AggregatesExpression = {"$computed": computed}
    if read_set.scope_keys:
        aggregates = {
            "$groups": {key: key for key in read_set.scope_keys},
            "$computed": computed,
        }

    page = await ctx.document.query(read_set.spec).aggregate_many(
        aggregates, filters=read_set.where
    )

    rows: list[tuple[dict[str, Any], float]] = []
    for row in page.hits:
        missing = [key for key in read_set.scope_keys if key not in row]
        if missing:
            raise exc.internal(
                f"system invariant {law.name!r}: grouped aggregate returned a row missing "
                f"scope key(s) {missing} — {row!r}"
            )

        scope = {key: row[key] for key in read_set.scope_keys}
        value = row.get(AGGREGATE_FIELD)
        rows.append((scope, float(value if value is not None else 0)))

    return rows


def _invariant_for(law: SystemInvariant) -> Invariant:
    """A DST invariant flagging every recorded scope of *law* whose aggregate failed the predicate."""

    def _check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant=law.name,
                message=(
                    f"system invariant {law.name!r} violated at scope "
                    f"{event.fields.get('scope')}: aggregate observed "
                    f"{event.fields.get('observed')}"
                ),
                events=(event,),
            )
            for event in history.of_kind(SYSTEM_INVARIANT_KIND)
            if event.fields.get("name") == law.name and not event.fields["held"]
        ]

    return _check


def compile_oracle(*laws: SystemInvariant) -> CompiledOracle:
    """Compile *laws* into a :class:`CompiledOracle` (final-state, grouped — v0).

    The returned ``observe`` hook records, per law and per scope, the aggregate and whether the
    predicate held; the returned invariants flag the failures. Pass several laws to check them in one
    pass. With no laws it is an inert oracle (empty observe, no invariants). Law names must be unique
    — events are attributed by name, so a duplicate would cross-contaminate violation reporting.
    """

    names = [law.name for law in laws]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise exc.configuration(
            f"compile_oracle: duplicate law name(s) {duplicates}; each SystemInvariant needs a "
            "unique name (the oracle attributes recorded events by name)",
            code="duplicate_system_invariant_name",
        )

    async def observe(ctx: ExecutionContext) -> None:
        for law in laws:
            for scope, observed in await _grouped(law, ctx):
                record_event(
                    SYSTEM_INVARIANT_KIND,
                    name=law.name,
                    scope=scope,
                    observed=observed,
                    held=law.holds(observed),
                )

    return CompiledOracle(
        observe=observe,
        invariants=tuple(_invariant_for(law) for law in laws),
    )

"""Compile a :class:`~forze.application.contracts.invariants.SystemInvariant` into a DST oracle.

A cross-aggregate law (a conservation or cardinality predicate over a read-set) is declared once and
gets two consumers: ``forze_kits`` *enforces* it at runtime, and this module *verifies* it under
simulation. :func:`compile_oracle` turns one or more laws into a :class:`CompiledOracle` — an
``observe`` hook plus the matching DST :data:`~forze_dst.oracle.invariants.Invariant`\\ s — that a
``Simulation``/``Cluster`` runs like any other oracle.

**v0 — final-state, grouped.** The observe hook runs once over a clean context after all work, issues
**one grouped aggregate** per law (group by the law's :attr:`~forze.application.contracts.invariants.ReadSet.scope_keys`,
reduce by its aggregate), and ``record_event``\\ s the outcome for **every scope the run produced** —
so a generated workload of many orders/ledgers is checked exhaustively, not at a hand-listed few. The
invariant then flags any recorded scope whose aggregate failed the predicate.

Two honest bounds. (1) It sees the *end* state only, so a violation created mid-run and later healed
is missed (that is what v1 below catches). (2) It checks the scopes **present in the
data** — a scope with no matching records produces no group and is not checked; for the
zero-anchored laws this primitive targets (a sum that must be ``0``, a count that must be ``<= 1``)
an absent scope holds trivially, so this is benign, but a *minimum*-style predicate (e.g. a sum that
must be ``>= 100``) would not be caught for a scope that vanished. Within those bounds it is sound.

**v1 — per-commit trace fold** (``compile_oracle(per_commit=True)``). Instead of querying final
state, it reads the value-trace (needs ``SimulationConfig.capture_values``), reconstructs the
read-set's aggregate AS-OF EACH committed transaction, and asserts the predicate after every commit —
so a violation a later transaction heals is caught at the commit where it existed. See
:func:`_per_commit_invariant` for the mechanism and its bounds. It reconstructs the *faithful* world
(a rolled-back transaction undoes its writes), which sharpens the trust boundary below: against an
unfaithful backend it reports the faithful answer, not the actual state.

**Trust boundary.** The verdict is only as strong as the conformance behind the read-set's backend:
over the conformance-verified mock (≡ Postgres/Mongo for the isolation family) a green result refers
to the real engine; over an unverified port it is a model tautology for the concurrency-dependent
part. The oracle reads simulated state — it does not itself re-establish that equivalence.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Awaitable, Callable, Iterable, Mapping, cast, final

import attrs

from forze.application.contracts.invariants import (
    AGGREGATE_FIELD,
    Count,
    SystemInvariant,
    computed_aggregate,
)
from forze.application.contracts.querying import (
    AggregatesExpression,
    QueryFilterExpression,
)
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


# ....................... #
# v1 — per-commit trace fold. Reconstructs the read-set's aggregate AS-OF EACH committed transaction
# from the value-trace (requires SimulationConfig.capture_values) and checks the predicate after every
# commit, so a violation a concurrent interleaving creates and a later transaction heals is caught at
# the commit where it existed. A pure History-reading Invariant (no observe-time work) — it reads the
# folded trace, which lands AFTER the observe hook runs. Bounds (see the module docstring): it folds
# create/update *result* events (the full post-write entity, so no partial-update gotcha) keyed by id,
# which assumes writes return their entity (the default ``return_new=True``) — a ``return_diff`` result
# is not a full entity and is not folded; it does not yet fold deletes; the where is scalar-equality
# only; a redacted field can't be summed. In a ``Cluster``, only node + observe traces are folded —
# not the ``setup`` hook — so establish any state the fold must see in the node (or use run_recorded).

_TX_DOMAIN = "tx"
_COMMAND_PHASE = "command"


def _matches_where(
    entity: Mapping[str, Any],
    where: QueryFilterExpression | None,
) -> bool:
    """Whether a reconstructed *entity* satisfies the read-set's constant ``where``.

    v1 supports scalar ``{"$values": {field: value}}`` equality — the shape the cross-aggregate laws
    use. A richer filter raises (a documented v1 limit; wire the query matcher when one is needed).
    """

    if where is None:
        return True

    where_map = cast("Mapping[str, Any]", where)
    values = where_map.get("$values")
    if (
        set(where_map) != {"$values"}
        or not isinstance(values, Mapping)
        or any(isinstance(value, Mapping) for value in values.values())
    ):
        raise exc.configuration(
            "per-commit oracle supports only scalar {$values: {...}} equality in a read-set "
            "'where'; richer filters are a v1 limit",
            code="unsupported_where_in_per_commit_oracle",
        )

    return all(entity.get(field) == value for field, value in values.items())


def _aggregate_by_scope(
    law: SystemInvariant, entities: Iterable[Mapping[str, Any]]
) -> dict[tuple[Any, ...], float]:
    """The aggregate per scope over the where-matched materialized *entities*."""

    groups: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for entity in entities:
        if _matches_where(entity, law.read_set.where):
            groups[tuple(entity.get(key) for key in law.read_set.scope_keys)].append(
                entity
            )

    aggregate = law.aggregate
    if isinstance(aggregate, Count):
        return {scope: float(len(members)) for scope, members in groups.items()}

    # Sum — the fold bypasses the backend's numeric validation, so guard non-numeric values with a
    # clear configuration error rather than a bare TypeError mid-fold (a missing field is treated 0).
    field = aggregate.field
    totals: dict[tuple[Any, ...], float] = {}
    for scope, members in groups.items():
        total = 0.0
        for member in members:
            value = member.get(field)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise exc.configuration(
                    f"per-commit oracle: read-set field {field!r} must be numeric to Sum, but an "
                    f"entity in scope holds {type(value).__name__} {value!r}",
                    code="non_numeric_sum_field",
                )
            total += float(value)
        totals[scope] = total
    return totals


def _per_commit_invariant(law: SystemInvariant) -> Invariant:
    """Check *law* after every committed transaction by folding the value-trace (v1)."""

    route = law.read_set.spec.name

    def _check(history: History) -> list[Violation]:
        traces = history.of_kind("trace")

        # Full post-write entities (create/update result events) per transaction, in trace order.
        writes: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        saw_command_write = False
        for event in traces:
            fields = event.fields
            if fields.get("route") != route or fields.get("phase") != _COMMAND_PHASE:
                continue
            saw_command_write = True
            result = fields.get("result")
            tx_id = fields.get("tx_id")
            if isinstance(result, Mapping) and "id" in result and tx_id is not None:
                writes[int(tx_id)].append(result)

        # Committed roots only, in commit order (the exit's trace_seq). The exit event fires from a
        # ``finally`` on commit AND rollback, so a rolled-back transaction is excluded by requiring
        # outcome == "commit"; only root scopes emit exits (depth 1), checked for good measure.
        commits = sorted(
            (int(fields["tx_id"]), int(fields.get("trace_seq", -1)))
            for event in traces
            for fields in (event.fields,)
            if fields.get("trace_domain") == _TX_DOMAIN
            and fields.get("op") == "exit"
            and fields.get("outcome") == "commit"
            and fields.get("tx_depth") == 1
            and fields.get("tx_id") is not None
        )
        commits.sort(key=lambda pair: pair[1])

        if saw_command_write and not any(writes.values()):
            raise exc.configuration(
                f"per-commit oracle for {law.name!r}: writes to {route!r} are on the trace but no "
                "entity values were captured — enable SimulationConfig.capture_values (and keep "
                "return_new=True on the writes)",
                code="per_commit_oracle_needs_capture_values",
            )

        materialized: dict[Any, Mapping[str, Any]] = {}
        first_failure: dict[tuple[Any, ...], tuple[int, float]] = {}
        for tx_id, _seq in commits:
            for entity in writes.get(tx_id, []):
                materialized[entity["id"]] = entity
            for scope, observed in _aggregate_by_scope(
                law, materialized.values()
            ).items():
                if scope not in first_failure and not law.holds(observed):
                    first_failure[scope] = (tx_id, observed)

        return [
            Violation(
                invariant=law.name,
                message=(
                    f"system invariant {law.name!r} was violated at a committed point (tx{tx_id}) "
                    f"— scope {dict(zip(law.read_set.scope_keys, scope))}: aggregate observed "
                    f"{observed}"
                ),
            )
            for scope, (tx_id, observed) in first_failure.items()
        ]

    return _check


def compile_oracle(*laws: SystemInvariant, per_commit: bool = False) -> CompiledOracle:
    """Compile *laws* into a :class:`CompiledOracle`.

    Default (**v0, final-state**): the returned ``observe`` hook records, per law and per scope, the
    aggregate and whether the predicate held; the invariants flag the failures. With ``per_commit``
    (**v1, the per-commit trace fold**): the invariants instead read the value-trace and check the law
    after *every committed transaction*, catching a violation that a later transaction heals (the
    ``observe`` hook is then a no-op — v1 reads the folded trace directly). v1 needs
    ``SimulationConfig.capture_values`` on. Pass several laws to check them in one pass; with no laws
    it is inert. Law names must be unique — the oracle attributes results by name.
    """

    names = [law.name for law in laws]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise exc.configuration(
            f"compile_oracle: duplicate law name(s) {duplicates}; each SystemInvariant needs a "
            "unique name (the oracle attributes recorded events by name)",
            code="duplicate_system_invariant_name",
        )

    if per_commit:

        async def per_commit_observe(_ctx: ExecutionContext) -> None:
            return None  # v1 reads the folded trace directly; no observe-time work

        return CompiledOracle(
            observe=per_commit_observe,
            invariants=tuple(_per_commit_invariant(law) for law in laws),
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

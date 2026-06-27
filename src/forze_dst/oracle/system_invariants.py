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

from collections import defaultdict
from collections.abc import Mapping as MappingABC
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
    evaluate_filter,
    value_at_path,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .invariants import Invariant, Violation
from .recorder import History, record_event

# ----------------------- #

SYSTEM_INVARIANT_KIND = "system_invariant"
"""The recorder event kind the observe hook stamps for each checked scope."""

# ....................... #


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
    law: SystemInvariant,
    ctx: ExecutionContext,
) -> list[tuple[JsonDict, float]]:
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

    rows: list[tuple[JsonDict, float]] = []

    for row in page.hits:
        if missing := [key for key in read_set.scope_keys if key not in row]:
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
_DELETE_OPS = frozenset({"kill", "kill_many"})


def _aggregate_by_scope(
    law: SystemInvariant,
    entities: Iterable[Mapping[str, Any]],
) -> dict[tuple[Any, ...], float]:
    """The aggregate per scope over the where-matched materialized *entities*."""

    groups: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)

    for entity in entities:
        # Match the read-set's ``where`` with the SAME shared evaluator the mock adapter and the
        # predicate oracle use (so the per-commit fold agrees with the runtime on the full filter DSL,
        # incl. dotted paths) — not a second, restricted matcher.
        if evaluate_filter(cast(JsonDict, entity), law.read_set.where):
            groups[
                tuple(value_at_path(entity, key) for key in law.read_set.scope_keys)
            ].append(entity)

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
            value = value_at_path(member, field)

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


# ....................... #


def _per_commit_invariant(law: SystemInvariant) -> Invariant:
    """Check *law* after every committed transaction by folding the value-trace (v1)."""

    route = law.read_set.spec.name

    def _check(history: History) -> list[Violation]:
        traces = history.of_kind("trace")

        # Per-transaction mutations in trace order: an upsert (create/update result event) carries the
        # full post-write entity; a delete (``kill``) carries only its key and removes the row — so the
        # fold drops it rather than letting a deleted document linger and skew the aggregate.
        mutations: dict[int, list[tuple[Any, Mapping[str, Any] | None]]] = defaultdict(list)
        saw_upsert_op = False
        captured_upsert = False

        for event in traces:
            fields = event.fields

            if fields.get("route") != route or fields.get("phase") != _COMMAND_PHASE:
                continue

            tx_id = fields.get("tx_id")
            if tx_id is None:
                continue

            if fields.get("op") in _DELETE_OPS:
                key = fields.get("key")
                if key is not None:  # kill_many carries no per-row key — a documented v1 bound
                    mutations[int(tx_id)].append((key, None))
                continue

            saw_upsert_op = True
            result = fields.get("result")

            if isinstance(result, MappingABC) and "id" in result:
                captured_upsert = True
                mutations[int(tx_id)].append(
                    (result["id"], result)  # pyright: ignore[reportUnknownArgumentType]
                )

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

        if saw_upsert_op and not captured_upsert:
            raise exc.configuration(
                f"per-commit oracle for {law.name!r}: writes to {route!r} are on the trace but no "
                "entity values were captured — enable SimulationConfig.capture_values (and keep "
                "return_new=True on the writes)",
                code="per_commit_oracle_needs_capture_values",
            )

        materialized: dict[Any, Mapping[str, Any]] = {}
        first_failure: dict[tuple[Any, ...], tuple[int, float]] = {}

        for tx_id, _seq in commits:
            for entity_id, entity in mutations.get(tx_id, []):
                if entity is None:
                    materialized.pop(entity_id, None)  # a committed delete drops the row
                else:
                    materialized[entity_id] = entity

            for scope, observed in _aggregate_by_scope(
                law,
                entities=materialized.values(),
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


# ....................... #


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

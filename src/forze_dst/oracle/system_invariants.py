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

A **paired** law (:attr:`~forze.application.contracts.invariants.SystemInvariant.against`) runs one
grouped aggregate per side and checks every scope present on **either** side, with ``0`` for the
side that has no rows there. With a parent read-set on one side, every parent is a scope, so a
parent left with no children is checked — the minimum-style law bound (2) would otherwise miss.
The sides are matched by each scope value's string form (:func:`_canonical`), so a ``UUID`` id and
a string reference to it are one scope. Runtime evaluation of one binding reads the same two
values and, like the oracle, does not check a binding with rows on neither side, so they agree.

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

import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from decimal import MAX_PREC, Decimal, InvalidOperation, localcontext
from enum import Enum
from typing import Any, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.invariants import (
    AGGREGATE_FIELD,
    AggregateValue,
    CountAll,
    ReadSet,
    Reducer,
    SystemInvariant,
    aggregate_value,
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


Scope = tuple[Any, ...]
"""One scope's values, in the order of the law's :attr:`~ReadSet.scope_keys`."""

Totals = dict[Scope, tuple[Scope, AggregateValue]]
"""Per scope, keyed by its :func:`_canonical` values: the scope as read, and its aggregate."""

Checked = list[tuple[Scope, Scope, AggregateValue, AggregateValue | None]]
"""``(key, scope, observed, against)`` for every scope a law checks."""


def _canonical(value: Any) -> Any:
    """A scope value in the form both sides are matched by: a ``UUID`` as its string and an
    ``Enum`` as its value, the form the value trace carries them in. So a ``UUID`` id and a
    string reference to it are one scope, on a backend that returns either."""

    if isinstance(value, Enum):
        return _canonical(value.value)

    return str(value) if isinstance(value, UUID) else value


def _keyed(scope: Scope) -> Scope:
    return tuple(_canonical(value) for value in scope)


async def _grouped(
    law: SystemInvariant,
    read_set: ReadSet,
    reducer: Reducer,
    ctx: ExecutionContext,
) -> Totals:
    """One grouped aggregate over one side of *law* in final state: the aggregate per scope.

    Groups by the read-set's ``scope_keys`` (a global law — empty keys — yields one whole-collection
    row), filtered by its constant ``where``. The reducer is shared with runtime evaluation via
    :func:`~forze.application.contracts.invariants.computed_aggregate`, so oracle and enforcement
    measure the same thing.
    """

    computed = computed_aggregate(reducer)

    aggregates: AggregatesExpression = {"$computed": computed}

    if read_set.scope_keys:
        aggregates = {
            "$groups": {key: key for key in read_set.scope_keys},
            "$computed": computed,
        }

    page = await ctx.document.query(read_set.spec).aggregate_many(
        aggregates, filters=read_set.where
    )

    totals: Totals = {}

    for row in page.hits:
        if missing := [key for key in read_set.scope_keys if key not in row]:
            raise exc.internal(
                f"system invariant {law.name!r}: grouped aggregate returned a row missing "
                f"scope key(s) {missing} — {row!r}"
            )

        scope = tuple(row[key] for key in read_set.scope_keys)
        value = aggregate_value(read_set, reducer, row.get(AGGREGATE_FIELD))
        totals[_keyed(scope)] = (scope, value)

    return totals


def _paired(law: SystemInvariant, own: Totals, other: Totals | None) -> Checked:
    """Every scope *law* checks, with its aggregate(s).

    One read-set: its scopes. Paired: every scope with rows on either side (this side's first),
    with the empty aggregate for the side that has none there.
    """

    side = law.against

    if side is None or other is None:
        return [(key, scope, observed, None) for key, (scope, observed) in own.items()]

    own_empty = aggregate_value(law.read_set, law.aggregate, None)
    other_empty = aggregate_value(side.read_set, side.aggregate, None)

    return [
        (
            key,
            (own.get(key) or other[key])[0],
            own[key][1] if key in own else own_empty,
            other[key][1] if key in other else other_empty,
        )
        for key in {**own, **other}
    ]


async def _final_state(law: SystemInvariant, ctx: ExecutionContext) -> Checked:
    """Every scope *law* checks over final state, with its aggregate(s)."""

    own = await _grouped(law, law.read_set, law.aggregate, ctx)
    side = law.against
    other = None if side is None else await _grouped(law, side.read_set, side.aggregate, ctx)

    return _paired(law, own, other)


def _described(observed: Any, against: Any) -> str:
    """The observed aggregate(s), for a violation message."""

    return f"aggregate observed {observed}" + ("" if against is None else f" against {against}")


def _invariant_for(law: SystemInvariant) -> Invariant:
    """A DST invariant flagging every recorded scope of *law* whose aggregate failed the predicate."""

    def _check(history: History) -> list[Violation]:
        return [
            Violation(
                invariant=law.name,
                message=(
                    f"system invariant {law.name!r} violated at scope "
                    f"{event.fields.get('scope')}: "
                    f"{_described(event.fields.get('observed'), event.fields.get('against'))}"
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
# A ``Decimal`` reaches the trace as its JSON string; the fold parses it back exactly for a field
# the read model declares ``Decimal``, so every trace producer works. A paired law folds both
# routes, and matches the two sides by each scope value's string form, as the final-state oracle.

_TX_DOMAIN = "tx"
_COMMAND_PHASE = "command"
_DELETE_OPS = frozenset({"kill", "kill_many"})


def _number(value: Any, field: str, *, exact: bool) -> int | float | Decimal:
    """*value* as a number to sum. A ``Decimal`` reaches the value trace as its JSON string, so a
    field the read model declares ``Decimal`` is parsed back from one, exactly."""

    if exact and isinstance(value, str):
        with contextlib.suppress(InvalidOperation):
            value = Decimal(value)

    # The fold bypasses the backend's numeric validation, so guard non-numeric values with a
    # clear configuration error rather than a bare TypeError mid-fold.
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise exc.configuration(
            f"per-commit oracle: read-set field {field!r} must be numeric to sum, but an "
            f"entity in scope holds {type(value).__name__} {value!r}",
            code="non_numeric_sum_field",
        )

    return value


def _exact_sum(values: Sequence[int | float | Decimal]) -> AggregateValue:
    """Total *values* as a store would: exactly when they are decimals, in float when any is one."""

    if any(isinstance(value, float) for value in values):
        return sum(float(value) for value in values)

    # The default 28-digit context would round a long decimal total; a store does not.
    with localcontext(prec=MAX_PREC):
        return (
            sum(values, Decimal(0)) if any(isinstance(v, Decimal) for v in values) else sum(values)
        )


def _aggregate_by_scope(
    read_set: ReadSet,
    reducer: Reducer,
    entities: Iterable[Mapping[str, Any]],
) -> Totals:
    """The aggregate per scope over the where-matched materialized *entities*."""

    groups: dict[Scope, tuple[Scope, list[Mapping[str, Any]]]] = {}

    for entity in entities:
        # Match the read-set's ``where`` with the SAME shared evaluator the mock adapter and the
        # predicate oracle use (so the per-commit fold agrees with the runtime on the full filter DSL,
        # incl. dotted paths) — not a second, restricted matcher.
        if evaluate_filter(cast(JsonDict, entity), read_set.where):
            scope = tuple(value_at_path(entity, key) for key in read_set.scope_keys)
            groups.setdefault(_keyed(scope), (scope, []))[1].append(entity)

    if isinstance(reducer, CountAll):
        return {
            key: (scope, aggregate_value(read_set, reducer, len(members)))
            for key, (scope, members) in groups.items()
        }

    # SumOf — a missing field is treated 0, as a store's SUM skips NULL.
    field = reducer.field
    exact = isinstance(aggregate_value(read_set, reducer, None), Decimal)  # declared Decimal
    totals: Totals = {}

    for key, (scope, members) in groups.items():
        values = [
            _number(value, field, exact=exact)
            for member in members
            if (value := value_at_path(member, field)) is not None
        ]
        totals[key] = (scope, aggregate_value(read_set, reducer, _exact_sum(values)))

    return totals


# ....................... #


def _per_commit_invariant(law: SystemInvariant) -> Invariant:
    """Check *law* after every committed transaction by folding the value-trace (v1)."""

    side = law.against
    own_route = law.read_set.spec.name
    routes = {own_route} if side is None else {own_route, side.read_set.spec.name}

    def _check(history: History) -> list[Violation]:
        traces = history.of_kind("trace")

        # Per-transaction mutations in trace order: an upsert (create/update result event) carries the
        # full post-write entity; a delete (``kill``) carries only its key and removes the row — so the
        # fold drops it rather than letting a deleted document linger and skew the aggregate.
        mutations: dict[int, list[tuple[str, Any, Mapping[str, Any] | None]]] = defaultdict(list)
        saw_upsert_op = False
        captured_upsert = False

        for event in traces:
            fields = event.fields

            route = fields.get("route")

            if route not in routes or fields.get("phase") != _COMMAND_PHASE:
                continue

            tx_id = fields.get("tx_id")

            if tx_id is None:
                continue

            if fields.get("op") in _DELETE_OPS:
                key = fields.get("key")

                if key is None:
                    # A bulk delete (kill_many) records no per-row key, so the fold
                    # cannot drop the removed rows — checking the law against rows
                    # that no longer exist would be unsound. Fail closed; the
                    # final-state oracle (compile_oracle without per_commit) handles
                    # deletes by querying the real committed state.
                    raise exc.configuration(
                        f"per-commit oracle for {law.name!r}: a bulk delete to "
                        f"{route!r} records no per-row key, so its removed rows "
                        "cannot be folded — use the final-state oracle "
                        "(compile_oracle without per_commit) for runs that bulk-delete",
                        code="per_commit_oracle_cannot_fold_bulk_delete",
                    )

                mutations[int(tx_id)].append((route, key, None))
                continue

            saw_upsert_op = True
            result = fields.get("result")

            if isinstance(result, Mapping) and "id" in result:
                captured_upsert = True
                mutations[int(tx_id)].append(
                    (route, result["id"], result)  # pyright: ignore[reportUnknownArgumentType]
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
                f"per-commit oracle for {law.name!r}: writes to {sorted(routes)} are on the trace but "
                "no entity values were captured — enable SimulationConfig.capture_values (and keep "
                "return_new=True on the writes)",
                code="per_commit_oracle_needs_capture_values",
            )

        materialized: dict[str, dict[Any, Mapping[str, Any]]] = {route: {} for route in routes}
        first_failure: dict[Scope, tuple[int, Scope, AggregateValue, AggregateValue | None]] = {}

        for tx_id, _seq in commits:
            for route, entity_id, entity in mutations.get(tx_id, []):
                if entity is None:
                    materialized[route].pop(entity_id, None)  # a committed delete drops the row
                else:
                    materialized[route][entity_id] = entity

            own = _aggregate_by_scope(law.read_set, law.aggregate, materialized[own_route].values())
            other = (
                None
                if side is None
                else _aggregate_by_scope(
                    side.read_set, side.aggregate, materialized[side.read_set.spec.name].values()
                )
            )

            for key, scope, observed, against in _paired(law, own, other):
                if key not in first_failure and not law.holds_for(observed, against):
                    first_failure[key] = (tx_id, scope, observed, against)

        return [
            Violation(
                invariant=law.name,
                message=(
                    f"system invariant {law.name!r} was violated at a committed point (tx{tx_id}) "
                    f"— scope {dict(zip(law.read_set.scope_keys, scope, strict=False))}: "
                    f"{_described(observed, against)}"
                ),
            )
            for tx_id, scope, observed, against in first_failure.values()
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
            for _key, scope, observed, against in await _final_state(law, ctx):
                record_event(
                    SYSTEM_INVARIANT_KIND,
                    name=law.name,
                    scope=dict(zip(law.read_set.scope_keys, scope, strict=True)),
                    observed=observed,
                    against=against,
                    held=law.holds_for(observed, against),
                )

    return CompiledOracle(
        observe=observe,
        invariants=tuple(_invariant_for(law) for law in laws),
    )

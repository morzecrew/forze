"""System invariants — declared laws that span more than one record.

The entity-level :func:`~forze.domain.validation.invariant` sees only one ``self`` and so cannot
express a relation *across* records (a ledger's balances sum to zero, at most one captured payment
per order). A :class:`SystemInvariant` declares exactly that: a named **read-set** (a document spec
plus a scope filter) reduced by one **aggregate**, and a predicate the aggregate must satisfy —
optionally against a second read-set reduced the same way and scoped by the same values (a sum
bounded by another sum, or by a parent's field).

It is a *pure declaration* — the runtime helper in ``forze_kits`` enforces it and the ``forze_dst``
compiler verifies it under simulation. Enforcement is **preventive** only when the read-set
co-locates with the write in one sufficiently-isolated transaction; otherwise it is **detective**
(evaluated after commit, so a violation is reported, not prevented). The mode is chosen at the call
site, never here.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from types import UnionType
from typing import Annotated, Any, Union, cast, final, get_args, get_origin

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import (
    AggregateComputedFieldExpression,
    QueryFilterExpression,
)
from forze.application.contracts.transaction import IsolationLevel
from forze.base.exceptions import exc

# ----------------------- #

AGGREGATE_FIELD = "__forze_aggregate__"
"""The output alias :func:`computed_aggregate` reduces into. Internal and deliberately unlikely as a
real document field, so it never collides with a grouped query's :attr:`~ReadSet.scope_keys` aliases
in the same result row (a scope key named ``"value"`` would have)."""

# ----------------------- #
# Aggregates — the closed set of reducers that collapse a read-set to one comparable number. Closed
# on purpose: each both pushes down to the query port and folds from a recorded trace at each commit.


@final
@attrs.define(frozen=True, slots=True)
class SumOf:
    """Sum a numeric field over the read-set."""

    field: str


@final
@attrs.define(frozen=True, slots=True)
class CountAll:
    """Count the records in the read-set."""


Reducer = SumOf | CountAll
"""The aggregate that collapses a scoped read-set to a single comparable number."""

AggregateValue = int | float | Decimal
"""What a reducer hands :attr:`SystemInvariant.holds`: a :class:`CountAll`'s ``int``, or a
:class:`SumOf`'s total — an exact ``Decimal`` over a field the read model declares ``Decimal``,
else a ``float``. See :func:`aggregate_value`."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class ReadSet:
    """The records a law ranges over: a document spec, the fields it is *scoped by*, and a constant filter.

    A law holds **per distinct value of its scope** — at most one captured payment *per order*, a zero
    sum *per ledger*. :attr:`scope_keys` names those scope fields (``("order_id",)``, ``("ledger_id",)``);
    :attr:`where` is the constant predicate every record in scope must also match (e.g. ``status ==
    "captured"``), or ``None``. Empty ``scope_keys`` declares a *global* law (one check over the whole
    ``where``-filtered collection).

    Declaring the scope as *fields* rather than an opaque filter-builder is what lets the DST oracle
    **group by them and check every scope the run produced** (not a hand-listed few), while runtime
    enforcement derives the single-binding filter via :func:`scope_filter`. Reuse ``tenant_aware`` in
    ``where`` for per-tenant scoping.
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    scope_keys: tuple[str, ...] = ()
    where: QueryFilterExpression | None = None


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class Measure:
    """A read-set reduced by an aggregate: the second side of a paired law.

    See :attr:`SystemInvariant.against`. Its :attr:`~ReadSet.scope_keys` pair with the law's own
    **by position**, so a law over order lines scoped by ``("order_id",)`` against
    ``ReadSet(spec=ORDERS, scope_keys=("id",))`` reads, for each order, the order whose ``id`` is the
    lines' ``order_id``. Both sides' scope fields must hold the same type, since the oracle matches
    their values by equality.
    """

    read_set: ReadSet
    aggregate: Reducer


# ....................... #


def computed_aggregate(reducer: Reducer) -> AggregateComputedFieldExpression:
    """The ``$computed`` clause reducing a (grouped or whole) read-set to a single ``"value"`` field.

    Shared by runtime evaluation and the DST oracle so both speak the same aggregate — a ``CountAll``
    becomes ``$count``, a ``SumOf`` becomes ``$sum`` of its field.
    """

    if isinstance(reducer, CountAll):
        return {AGGREGATE_FIELD: {"$count": None}}

    return {AGGREGATE_FIELD: {"$sum": reducer.field}}


def scope_filter(read_set: ReadSet, params: Mapping[str, Any]) -> QueryFilterExpression | None:
    """The filter selecting **one binding's** records: the scope-key equalities (from *params*) AND
    the constant :attr:`~ReadSet.where`.

    ``None`` when the law is global with no constant filter. The single-binding counterpart of the
    oracle's group-by-:attr:`~ReadSet.scope_keys` query — both derive from the same declaration, so a
    binding the runtime checks and a group the oracle checks select the same records.
    """

    missing = [key for key in read_set.scope_keys if key not in params]
    if missing:
        raise exc.configuration(
            f"scope_filter: params is missing scope key(s) {missing} — the read-set is scoped by "
            f"{list(read_set.scope_keys)}, so every one must be bound",
            code="missing_scope_key",
        )

    scope_values = {key: params[key] for key in read_set.scope_keys}
    scope_pred: QueryFilterExpression | None = {"$values": scope_values} if scope_values else None

    if read_set.where is not None and scope_pred is not None:
        return {"$and": [read_set.where, scope_pred]}

    return read_set.where if scope_pred is None else scope_pred


def _declares_decimal(model: Any, path: str) -> bool:
    """Whether *model* declares the field at dotted *path* a ``Decimal`` (optionally ``None``)."""

    annotation: Any = model

    for segment in path.split("."):
        fields = getattr(annotation, "model_fields", None)
        field = fields.get(segment) if isinstance(fields, dict) else None

        if field is None:
            return False

        annotation = field.annotation

        if get_origin(annotation) in (Union, UnionType):
            members = [m for m in get_args(annotation) if m is not type(None)]
            annotation = members[0] if len(members) == 1 else annotation

        if get_origin(annotation) is Annotated:
            annotation = get_args(annotation)[0]

    return isinstance(annotation, type) and issubclass(annotation, Decimal)


def aggregate_value(read_set: ReadSet, reducer: Reducer, raw: Any) -> AggregateValue:
    """The value :attr:`SystemInvariant.holds` receives for *raw*, a backend's aggregate.

    *raw* is ``None`` over no rows, which counts as ``0``. A :class:`CountAll` is an ``int``. A
    :class:`SumOf` over a field the read model declares ``Decimal`` stays an exact ``Decimal``; over
    any other field it is a ``float``, as before. The type follows the declaration rather than the
    value a backend returns, so a law sees the same type on every backend (Postgres sums a ``bigint``
    column to ``numeric``, the mock sums it to ``int``). Shared by runtime evaluation and both DST
    oracles.
    """

    if isinstance(reducer, CountAll):
        return int(raw or 0)

    if _declares_decimal(read_set.spec.read, reducer.field):
        # str() first: exact for an int, and for a store's own decimal type (BSON Decimal128).
        return raw if isinstance(raw, Decimal) else Decimal(str(raw or 0))

    return float(raw or 0)


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class SystemInvariant:
    """A declared law over a read-set: an aggregate of the scoped records must satisfy a predicate.

    Cross-record by construction — what :func:`~forze.domain.validation.invariant` cannot express.
    *holds* is a **pure** predicate over the aggregate (see :data:`AggregateValue`: a
    :class:`CountAll`'s ``int``, a :class:`SumOf`'s ``float``, or an exact ``Decimal`` over a
    ``Decimal`` field); it must be deterministic and side-effect free, because it runs both in
    production and inside the deterministic simulation. *name* identifies the law in violations and
    in the oracle's provenance.

    With :attr:`against`, the law is **paired**: *holds* takes two arguments, this read-set's
    aggregate and the other side's, for the same scope — ``holds=lambda lines, order: lines <=
    order``. A scope with rows on either side is checked, and the side with none contributes ``0``,
    so a parent read-set on one side makes every parent a scope, including one with no children.
    """

    name: str
    read_set: ReadSet
    aggregate: Reducer
    holds: Callable[[Any], bool] | Callable[[Any, Any], bool]
    """A one-argument predicate, or a two-argument one when :attr:`against` is set."""

    against: Measure | None = None
    """The second side of a paired law, scoped by the same values as :attr:`read_set` (its scope
    keys pair with this read-set's by position), or ``None`` for a law over one read-set."""

    required_isolation: IsolationLevel = IsolationLevel.SERIALIZABLE
    """The minimum isolation a *preventive* check needs to be correct under concurrency. Most
    predicate-over-read-set laws are **write-skew prone** — two transactions each read the set, each
    write disjoint rows, and the combination breaks the law though neither alone does — which only
    ``SERIALIZABLE`` prevents; hence the default. Lower it (e.g. ``SNAPSHOT``) only for a law whose
    sole conflict mode is a lost update (two writers clobbering the *same* row). Ignored by detective
    enforcement; the preventive path fails closed unless the writing transaction runs at least here
    (and the backend's conformance-verified ``TxCapabilities`` reports the level)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.against is not None and len(self.against.read_set.scope_keys) != len(
            self.read_set.scope_keys
        ):
            raise exc.configuration(
                f"system invariant {self.name!r}: against is scoped by "
                f"{list(self.against.read_set.scope_keys)} but the law by "
                f"{list(self.read_set.scope_keys)} — the keys pair by position, so both sides need "
                "the same number",
                code="system_invariant_scope_mismatch",
            )

    # ....................... #

    def holds_for(self, observed: AggregateValue, against: AggregateValue | None = None) -> bool:
        """Apply :attr:`holds` to one scope's aggregate, and the other side's for a paired law."""

        if self.against is None:
            return cast(Callable[[Any], bool], self.holds)(observed)

        return cast(Callable[[Any, Any], bool], self.holds)(observed, against)

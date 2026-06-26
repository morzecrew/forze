"""System invariants — declared laws that span more than one record.

The entity-level :func:`~forze.domain.validation.invariant` guards a *single* entity: it runs on
create and every update, sees only ``self``, and so structurally cannot express a relation *across*
records. The hardest correctness rules usually are exactly that — a conservation law (a ledger's
balances sum to zero), a cardinality law (at most one captured payment per order), a threshold law
(reserved never exceeds stock). A :class:`SystemInvariant` is the declaration those need: a named
**read-set** (a document spec plus a scope filter that selects the records the law ranges over)
reduced by one **aggregate**, and a **predicate** the aggregate must satisfy.

It is a *pure declaration* — nothing here runs it. It is handed to two consumers (one declaration,
two consumers): the runtime helper in ``forze_kits`` enforces it, and the DST compiler in
``forze_dst`` verifies it under simulation. The aggregate-over-a-read-set shape is deliberate: it is
the intersection of "pushes down to the query port" (``count`` / ``aggregate_many``) and
"reconstructible from a recorded trace at each committed point" (the simulation oracle).

**Honesty (see RFC 0012 §4.B / §7).** A cross-aggregate law is *not* free distributed atomicity.
Enforcement is **preventive** only when the read-set co-locates with the write in one transaction
under a sufficient (capability-verified) isolation level; otherwise it is **detective** — evaluated
after commit, so a violation is *reported*, not prevented. This module only declares the law; the
enforcement mode is chosen at the call site, and the distinction is named there, never blurred.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, final

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
# on purpose: each both pushes down to the query port and folds from a recorded trace (RFC 0012 §4.A).


@final
@attrs.define(frozen=True, slots=True)
class Sum:
    """Sum a numeric field over the read-set."""

    field: str


@final
@attrs.define(frozen=True, slots=True)
class Count:
    """Count the records in the read-set."""


Reducer = Sum | Count
"""The aggregate that collapses a scoped read-set to a single comparable number."""


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


def computed_aggregate(reducer: Reducer) -> AggregateComputedFieldExpression:
    """The ``$computed`` clause reducing a (grouped or whole) read-set to a single ``"value"`` field.

    Shared by runtime evaluation and the DST oracle so both speak the same aggregate — a ``Count``
    becomes ``$count``, a ``Sum`` becomes ``$sum`` of its field.
    """

    if isinstance(reducer, Count):
        return {AGGREGATE_FIELD: {"$count": None}}

    return {AGGREGATE_FIELD: {"$sum": reducer.field}}


def scope_filter(
    read_set: ReadSet, params: Mapping[str, Any]
) -> QueryFilterExpression | None:
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
    scope_pred: QueryFilterExpression | None = (
        {"$values": scope_values} if scope_values else None
    )

    if read_set.where is not None and scope_pred is not None:
        return {"$and": [read_set.where, scope_pred]}

    return read_set.where if scope_pred is None else scope_pred


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class SystemInvariant:
    """A declared law over a read-set: an aggregate of the scoped records must satisfy a predicate.

    Cross-record by construction — what :func:`~forze.domain.validation.invariant` cannot express.
    *holds* is a **pure** predicate over the aggregate (a :class:`Sum`'s numeric total, or a
    :class:`Count`'s cardinality, passed as a ``float``); it must be deterministic and side-effect
    free, because it runs both in production and inside the deterministic simulation. *name*
    identifies the law in violations and in the oracle's provenance.
    """

    name: str
    read_set: ReadSet
    aggregate: Reducer
    holds: Callable[[float], bool]

    required_isolation: IsolationLevel = IsolationLevel.SERIALIZABLE
    """The minimum isolation a *preventive* check needs to be correct under concurrency. Most
    predicate-over-read-set laws are **write-skew prone** — two transactions each read the set, each
    write disjoint rows, and the combination breaks the law though neither alone does — which only
    ``SERIALIZABLE`` prevents; hence the default. Lower it (e.g. ``SNAPSHOT``) only for a law whose
    sole conflict mode is a lost update (two writers clobbering the *same* row). Ignored by detective
    enforcement; the preventive path fails closed unless the writing transaction runs at least here
    (and the backend's conformance-verified ``TxCapabilities`` reports the level)."""

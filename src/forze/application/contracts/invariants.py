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
from forze.application.contracts.querying import QueryFilterExpression

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
    """The records a law ranges over: a document spec plus a scope filter built from the law's params.

    *scope* turns the law's binding parameters (which ledger, which order — a small mapping the call
    site supplies) into the :data:`~forze.application.contracts.querying.QueryFilterExpression` that
    selects the set. Reuse the ordinary filter DSL (and ``tenant_aware`` for per-tenant scoping); the
    set is "the records this law is about, for these parameters, right now".
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    scope: Callable[[Mapping[str, Any]], QueryFilterExpression]


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

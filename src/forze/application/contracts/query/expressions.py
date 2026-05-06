"""Filter, sort, and aggregate expression types for document queries."""

from __future__ import annotations

from typing import Literal, Mapping, Sequence, TypeAlias, TypedDict

from .types import Array, Numeric, Scalar

# ----------------------- #

QueryFieldShortcutValue = Scalar | Array | None
"""Shortcut value: scalar (eq), array (in), or None (null check)."""

QueryFieldOpConjunction = TypedDict(
    "QueryFieldOpConjunction",
    {
        # Equality
        "$eq": Scalar,
        "$neq": Scalar,
        # Ordering
        "$gt": Numeric,
        "$gte": Numeric,
        "$lt": Numeric,
        "$lte": Numeric,
        # Membership
        "$in": Array,
        "$nin": Array,
        # Unary
        "$null": bool,
        "$empty": bool,
        # Set relations
        "$superset": Array,
        "$subset": Array,  # whitelist (field doesn't have values outside of the list)
        "$disjoint": Array,  # blacklist (field doesn't have values inside of the list)
        "$overlaps": Array,  # intersection (field has values that are in both lists)
        #! TODO: add support for ltree operators
    },
    total=False,
)

QueryFieldMapValue = QueryFieldOpConjunction | QueryFieldShortcutValue
"""Value for a single field: operator map or shortcut."""

QueryFieldMap = Mapping[str, QueryFieldMapValue]
"""Map of field names to filter values.

Keys may use dot notation (``meta.version``) for nested document fields. Backends
interpret paths according to their storage (e.g. JSON column navigation on Postgres).
"""

# ....................... #

QueryPredicate = TypedDict("QueryPredicate", {"$fields": QueryFieldMap})
"""Predicate with ``$fields`` mapping."""

QueryConjunction = TypedDict(
    "QueryConjunction",
    {"$and": Sequence["QueryFilterExpression"]},
)
"""Conjunction of filter expressions."""

QueryDisjunction = TypedDict(
    "QueryDisjunction", {"$or": Sequence["QueryFilterExpression"]}
)
"""Disjunction of filter expressions."""

QueryFilterExpression: TypeAlias = QueryPredicate | QueryConjunction | QueryDisjunction
"""Recursive filter expression (predicate, and, or)."""

# ....................... #

QuerySortDirection = Literal["asc", "desc"]
"""Sort direction for a field."""

QuerySortExpression = Mapping[str, QuerySortDirection]
"""Map of field names to sort direction (supports dot-separated paths like filters)."""

# ....................... #

AggregateFunction = Literal["$count", "$sum", "$avg", "$min", "$max", "$median"]
"""Supported aggregate function names."""

AggregateFieldExpression = Mapping[str, str]
"""Map of aggregate output aliases to source field paths used as group keys."""

AggregateGroupKeysExpression: TypeAlias = (
    AggregateFieldExpression | list[str] | tuple[str, ...]
)
"""Group keys: alias→path map, or a list/tuple of names (alias and path are the same)."""


class AggregateComputedFunctionApplication(TypedDict, total=False):
    """Detailed aggregate function application with an optional row filter."""

    field: str
    """Source field path for value-based aggregate functions."""

    filter: QueryFilterExpression
    """Optional row filter applied only to this computed aggregate."""


AggregateComputedFunctionExpression = TypedDict(
    "AggregateComputedFunctionExpression",
    {
        "$count": str | None | AggregateComputedFunctionApplication,
        "$sum": str | AggregateComputedFunctionApplication,
        "$avg": str | AggregateComputedFunctionApplication,
        "$min": str | AggregateComputedFunctionApplication,
        "$max": str | AggregateComputedFunctionApplication,
        "$median": str | AggregateComputedFunctionApplication,
    },
    total=False,
)
"""Single aggregate function application keyed by function name."""

AggregateComputedFieldExpression = Mapping[str, AggregateComputedFunctionExpression]
"""Map of aggregate output aliases to computed aggregate function specs."""

AggregatesExpression = TypedDict(
    "AggregatesExpression",
    {
        "$fields": AggregateGroupKeysExpression,
        "$computed": AggregateComputedFieldExpression,
    },
    total=False,
)
"""Aggregate result shape: ``$fields`` (group keys) plus ``$computed`` (aggregates).

``$fields`` is either a map of output alias to source path or a homogeneous list
or tuple of field paths (each path is both alias and source). ``$computed`` maps
output aliases to aggregate function applications.
"""


# ....................... #


class PaginationExpression(TypedDict, total=False):
    """Pagination expression."""

    limit: int | None
    """Limit."""

    offset: int | None
    """Offset."""


# ....................... #


class CursorPaginationExpression(TypedDict, total=False):
    """Cursor (keyset) pagination request.

    Pass at most one of ``after`` or ``before``; if both are set, adapters may
    raise. Opaque cursors are returned on :class:`~forze.application.contracts.base.CursorPage`.
    """

    limit: int | None
    """Page size. Adapters may apply a default when omitted."""

    after: str | None
    """Opaque token from a prior response's ``next_cursor`` (forward)."""

    before: str | None
    """Opaque token from a prior response's ``prev_cursor`` (backward)."""

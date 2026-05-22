"""Filter, sort, and aggregate expression types for document queries."""

from __future__ import annotations

from typing import Literal, Mapping, NotRequired, Sequence, TypeAlias, TypedDict

from .types import Array, Numeric, Scalar

# ----------------------- #
# Filter: literal constraints ($values)

QueryValueShortcutValue = Scalar | Array | None
"""Shortcut value: scalar (eq), array (in), or None (null check)."""

QueryValueOpConjunction = TypedDict(
    "QueryValueOpConjunction",
    {
        "$eq": Scalar,
        "$neq": Scalar,
        "$gt": Numeric,
        "$gte": Numeric,
        "$lt": Numeric,
        "$lte": Numeric,
        "$in": Array,
        "$nin": Array,
        "$null": bool,
        "$empty": bool,
        "$superset": Array,
        "$subset": Array,
        "$disjoint": Array,
        "$overlaps": Array,
    },
    total=False,
)
"""Operator map for field-to-literal filters."""

QueryValueMapValue = QueryValueOpConjunction | QueryValueShortcutValue
"""Value for a single field: operator map or shortcut."""

QueryValueMap = Mapping[str, QueryValueMapValue]
"""Map of field names to literal filter values (dot paths for nested JSON)."""

# ....................... #
# Filter: field-to-field constraints ($fields)

QueryFieldsOpConjunction = TypedDict(
    "QueryFieldsOpConjunction",
    {
        "$eq": str,
        "$neq": str,
        "$gt": str,
        "$gte": str,
        "$lt": str,
        "$lte": str,
    },
    total=False,
)
"""Operator map for field-to-field compare; values are right-hand field paths."""

QueryFieldsMapValue = QueryFieldsOpConjunction | str
"""Field compare value: operator map or ``$eq`` shortcut (right-hand field path)."""

QueryFieldsMap = Mapping[str, QueryFieldsMapValue]
"""Map of left field paths to field-to-field compare specs."""

# ....................... #
# Filter: constraint bundle ($values and/or $fields)

QueryConstraintPredicate = TypedDict(
    "QueryConstraintPredicate",
    {
        "$values": QueryValueMap,
        "$fields": QueryFieldsMap,
    },
    total=False,
)
"""Literal and/or field-to-field constraints (implicit AND when both keys present)."""

QueryValuesPredicate = TypedDict("QueryValuesPredicate", {"$values": QueryValueMap})
"""Literal-only constraint (``$values``)."""

QueryFieldsPredicate = TypedDict("QueryFieldsPredicate", {"$fields": QueryFieldsMap})
"""Field-to-field-only constraint (``$fields``)."""

# ....................... #

QueryConjunction = TypedDict(
    "QueryConjunction",
    {"$and": Sequence["QueryFilterExpression"]},
)
"""Conjunction of filter expressions."""

QueryDisjunction = TypedDict(
    "QueryDisjunction",
    {"$or": Sequence["QueryFilterExpression"]},
)
"""Disjunction of filter expressions."""

QueryFilterExpression: TypeAlias = (
    QueryConstraintPredicate | QueryConjunction | QueryDisjunction
)
"""Recursive filter expression (constraints, and, or)."""

# ....................... #

QuerySortDirection = Literal["asc", "desc"]
"""Sort direction for a field."""

QuerySortExpression = Mapping[str, QuerySortDirection]
"""Map of field names to sort direction (supports dot-separated paths like filters)."""

# ....................... #

AggregateFunction = Literal["$count", "$sum", "$avg", "$min", "$max", "$median"]
"""Supported aggregate function names."""

AggregateTruncUnit = Literal["hour", "day", "week", "month"]
"""Calendar bucketing unit for ``$trunc`` group expressions."""


class AggregateTruncSpec(TypedDict):
    """Calendar bucket on a timestamp field (``$trunc`` under ``$groups``)."""

    field: str
    """Source field path (typically a ``timestamptz`` / ISO datetime)."""

    unit: AggregateTruncUnit
    """Bucket width: hour, day, week (Monday start), or month."""

    timezone: NotRequired[str]
    """IANA name (e.g. ``Europe/Paris``) or fixed offset ``+3``, ``+03:00``. Defaults to ``UTC``."""


AggregateTruncExpression = TypedDict(
    "AggregateTruncExpression",
    {"$trunc": AggregateTruncSpec},
)
"""Single ``$trunc`` group operator application."""


AggregateGroupKeyValue: TypeAlias = str | AggregateTruncExpression
"""Group dimension: path shortcut (``str``) or ``$trunc`` operator map."""


AggregateGroupKeysMapExpression = Mapping[str, AggregateGroupKeyValue]
"""Map of output alias to group dimension (path or ``$trunc``)."""


AggregateGroupKeysExpression: TypeAlias = (
    AggregateGroupKeysMapExpression | list[str] | tuple[str, ...]
)
"""Group keys: alias→dimension map, or path-only list/tuple (alias equals path)."""


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
        "$groups": AggregateGroupKeysExpression,
        "$computed": AggregateComputedFieldExpression,
    },
    total=False,
)
"""Aggregate result shape: ``$groups`` (dimensions) plus ``$computed`` (metrics).

``$groups`` map values are either a source path string (group by that field) or
``{\"$trunc\": {\"field\", \"unit\", optional \"timezone\"}}`` for calendar buckets
(output alias is the map key). List/tuple ``$groups`` accepts path strings only.
``$computed`` maps output aliases to aggregate function applications.
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

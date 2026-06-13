"""Filter, sort, and aggregate expression types for document queries."""

from __future__ import annotations

from typing import Literal, Mapping, NotRequired, Sequence, TypeAlias, TypedDict

from .types import Array, Numeric, Scalar, TextPatternValue

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
        "$like": TextPatternValue,
        "$ilike": TextPatternValue,
        "$regex": TextPatternValue,
    },
    total=False,
)
"""Operator map for field-to-literal filters."""

# ....................... #
# Element quantifiers (under $values)

QueryElementOpConjunction = TypedDict(
    "QueryElementOpConjunction",
    {
        "$eq": Scalar,
        "$neq": Scalar,
        "$gt": Numeric,
        "$gte": Numeric,
        "$lt": Numeric,
        "$lte": Numeric,
        "$like": TextPatternValue,
        "$ilike": TextPatternValue,
        "$regex": TextPatternValue,
    },
    total=False,
)
"""Operator map for a single array element (equality, ordering, and text patterns)."""

QueryElementValueMapValue = QueryElementOpConjunction | Scalar
"""Value for one element-relative field inside ``$any`` / ``$all`` / ``$none``."""

QueryElementValueMap = Mapping[str, QueryElementValueMapValue]
"""Element-relative field map (object arrays)."""

QueryElementValuesPredicate = TypedDict(
    "QueryElementValuesPredicate",
    {"$values": QueryElementValueMap},
)
"""Element-relative ``$values`` bundle for object-array quantifiers."""

QueryElementConstraint = (
    QueryElementOpConjunction | Scalar | QueryElementValuesPredicate
)
"""Inner constraint for ``$any`` / ``$all`` / ``$none`` (op map, scalar shortcut, or ``$values``)."""

QueryElementQuantifierExpression = TypedDict(
    "QueryElementQuantifierExpression",
    {
        "$any": QueryElementConstraint,
        "$all": QueryElementConstraint,
        "$none": QueryElementConstraint,
    },
    total=False,
)
"""Element quantifier application on an array field."""

QueryValueMapValue = (
    QueryValueOpConjunction
    | QueryValueShortcutValue
    | QueryElementQuantifierExpression
)
"""Value for a single field: operator map, shortcut, or element quantifier."""

QueryValueMap = Mapping[str, QueryValueMapValue]
"""Map of field names to literal filter values (dot paths for nested JSON)."""

QueryValuesPredicate = TypedDict("QueryValuesPredicate", {"$values": QueryValueMap})
"""Literal-only constraint (``$values``)."""

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

QueryNegation = TypedDict(
    "QueryNegation",
    {"$not": "QueryFilterExpression"},
)
"""Negation of a single filter expression."""

QueryFilterExpression: TypeAlias = (
    QueryConstraintPredicate
    | QueryConjunction
    | QueryDisjunction
    | QueryNegation
)
"""Recursive filter expression (constraints, and, or, not)."""

# ....................... #

QuerySortDirection = Literal["asc", "desc"]
"""Sort direction for a field."""

QuerySortNulls = Literal["first", "last"]
"""Where null sort-key values are placed, independent of direction (SQL ``NULLS
FIRST``/``LAST`` semantics). When omitted, the canonical default applies: ``first`` for
``asc``, ``last`` for ``desc`` (a null sorts as the smallest value)."""

QuerySortKeySpec = TypedDict(
    "QuerySortKeySpec",
    {"dir": QuerySortDirection, "nulls": NotRequired[QuerySortNulls]},
)
"""Explicit per-key sort spec: a direction plus optional null placement."""

QuerySortValue = QuerySortDirection | QuerySortKeySpec
"""A sort value: the direction shorthand (``"asc"``) or an explicit
:data:`QuerySortKeySpec` (``{"dir": "asc", "nulls": "last"}``)."""

QuerySortExpression = Mapping[str, QuerySortValue]
"""Map of field names to a sort value (supports dot-separated paths like filters).

Each value is either a direction string or a ``{"dir", "nulls"}`` spec. The plain string
keeps the canonical null placement; the spec form overrides it per key."""

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
        "$having": "QueryFilterExpression",
    },
    total=False,
)
"""Aggregate result shape: ``$groups`` (dimensions) plus ``$computed`` (metrics).

``$groups`` map values are either a source path string (group by that field) or
``{\"$trunc\": {\"field\", \"unit\", optional \"timezone\"}}`` for calendar buckets
(output alias is the map key). List/tuple ``$groups`` accepts path strings only.
``$computed`` maps output aliases to aggregate function applications.

``$having`` is an optional filter applied to the **aggregated** rows (post-group),
referencing only the output aliases — group keys and computed metrics — e.g. keep only
groups whose ``$count`` exceeds a threshold. It is the aggregate analogue of a SQL
``HAVING`` clause and uses the same filter grammar as ``filters``.
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

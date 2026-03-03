"""Filter and sort expression types for document queries."""

from typing import Literal, Mapping, TypedDict

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
"""Map of field names to filter values."""

# ....................... #

QueryPredicate = TypedDict("QueryPredicate", {"$fields": QueryFieldMap})
"""Predicate with ``$fields`` mapping."""

QueryConjunction = TypedDict(
    "QueryConjunction", {"$and": list["QueryFilterExpression"]}
)
"""Conjunction of filter expressions."""

QueryDisjunction = TypedDict("QueryDisjunction", {"$or": list["QueryFilterExpression"]})
"""Disjunction of filter expressions."""

QueryFilterExpression = QueryPredicate | QueryConjunction | QueryDisjunction
"""Recursive filter expression (predicate, and, or)."""

# ....................... #

QuerySortDirection = Literal["asc", "desc"]
"""Sort direction for a field."""

QuerySortExpression = Mapping[str, QuerySortDirection]
"""Map of field names to sort direction."""

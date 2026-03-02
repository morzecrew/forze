"""Filter and sort expression types for document queries."""

from typing import Literal, Mapping, TypedDict

from .types import Array, Numeric, Scalar

# ----------------------- #

FieldShortcutValue = Scalar | Array | None
"""Shortcut value: scalar (eq), array (in), or None (null check)."""

FieldOpConjunction = TypedDict(
    "FieldOpConjunction",
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

FieldMapValue = FieldOpConjunction | FieldShortcutValue
"""Value for a single field: operator map or shortcut."""

FieldMap = Mapping[str, FieldMapValue]
"""Map of field names to filter values."""

# ....................... #

Predicate = TypedDict("Predicate", {"$fields": FieldMap})
"""Predicate with ``$fields`` mapping."""

Conjunction = TypedDict("Conjunction", {"$and": list["FilterExpression"]})
"""Conjunction of filter expressions."""

Disjunction = TypedDict("Disjunction", {"$or": list["FilterExpression"]})
"""Disjunction of filter expressions."""

FilterExpression = Predicate | Conjunction | Disjunction
"""Recursive filter expression (predicate, and, or)."""

# ....................... #

SortDirection = Literal["asc", "desc"]
"""Sort direction for a field."""

SortExpression = Mapping[str, SortDirection]
"""Map of field names to sort direction."""

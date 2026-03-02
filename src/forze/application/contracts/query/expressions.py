from typing import Literal, Mapping, TypedDict

from .types import Array, Numeric, Scalar

# ----------------------- #

FieldShortcutValue = Scalar | Array | None

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
FieldMap = Mapping[str, FieldMapValue]

# ....................... #

Predicate = TypedDict("Predicate", {"$fields": FieldMap})
Conjunction = TypedDict("Conjunction", {"$and": list["FilterExpression"]})
Disjunction = TypedDict("Disjunction", {"$or": list["FilterExpression"]})

FilterExpression = Predicate | Conjunction | Disjunction

# ....................... #

SortDirection = Literal["asc", "desc"]
SortExpression = Mapping[str, SortDirection]

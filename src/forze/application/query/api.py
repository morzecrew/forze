from datetime import datetime
from typing import Literal, Mapping, TypedDict

# ----------------------- #
#! TODO: rename file (?)

Numeric = int | float | datetime
Scalar = Numeric | bool | str
SeqScalar = list[Scalar]

# ....................... #

FieldShortcutValue = Scalar | SeqScalar | None

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
        "$in": SeqScalar,
        "$nin": SeqScalar,
        # Unary
        "$null": bool,
        "$empty": bool,
        # Set relations
        "$superset": SeqScalar,
        "$subset": SeqScalar,  # whitelist (field doesn't have values outside of the list)
        "$disjoint": SeqScalar,  # blacklist (field doesn't have values inside of the list)
        "$overlaps": SeqScalar,  # intersection (field has values that are in both lists)
        #! TODO: add support for ltree operators
    },
    total=False,
)

FieldMap = Mapping[str, FieldOpConjunction | FieldShortcutValue]

# ....................... #

Predicate = TypedDict("Predicate", {"$fields": FieldMap})
Conjunction = TypedDict("Conjunction", {"$and": list["FilterExpression"]})
Disjunction = TypedDict("Disjunction", {"$or": list["FilterExpression"]})

FilterExpression = Predicate | Conjunction | Disjunction

# ....................... #

SortDirection = Literal["asc", "desc"]
SortExpression = Mapping[str, SortDirection]

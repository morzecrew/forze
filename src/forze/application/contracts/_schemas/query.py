from datetime import date, datetime
from typing import Literal, Mapping, TypedDict, TypeGuard
from uuid import UUID

# ----------------------- #

Numeric = int | float | datetime | date
Scalar = Numeric | bool | str | UUID
Array = list[Scalar]

# ....................... #

UnaryOp = Literal["$null", "$empty"]
OrdOp = Literal["$gt", "$gte", "$lt", "$lte"]
EqOp = Literal["$eq", "$neq"]
MembOp = Literal["$in", "$nin"]
SetRelOp = Literal["$superset", "$subset", "$disjoint", "$overlaps"]

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp

# ....................... #

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

# ....................... #


def is_predicate(expr: FilterExpression) -> TypeGuard[Predicate]:
    return "$fields" in expr


def is_conjunction(expr: FilterExpression) -> TypeGuard[Conjunction]:
    return "$and" in expr


def is_disjunction(expr: FilterExpression) -> TypeGuard[Disjunction]:
    return "$or" in expr


def is_field_conjunction(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldOpConjunction]:
    return isinstance(map_, dict)


def is_field_shortcut(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldShortcutValue]:
    return not isinstance(map_, dict)

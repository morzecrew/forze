from typing import TypeGuard

from .expressions import (
    Conjunction,
    Disjunction,
    FieldOpConjunction,
    FieldShortcutValue,
    FilterExpression,
    Predicate,
)

# ----------------------- #


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

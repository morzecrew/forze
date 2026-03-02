"""Type guards for filter expression discrimination."""

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
    """Return ``True`` when the expression is a predicate (``$fields``)."""
    return "$fields" in expr


def is_conjunction(expr: FilterExpression) -> TypeGuard[Conjunction]:
    """Return ``True`` when the expression is a conjunction (``$and``)."""
    return "$and" in expr


def is_disjunction(expr: FilterExpression) -> TypeGuard[Disjunction]:
    """Return ``True`` when the expression is a disjunction (``$or``)."""
    return "$or" in expr


def is_field_conjunction(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldOpConjunction]:
    """Return ``True`` when the value is an operator map (dict)."""
    return isinstance(map_, dict)


def is_field_shortcut(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldShortcutValue]:
    """Return ``True`` when the value is a shortcut (scalar, array, or None)."""
    return not isinstance(map_, dict)

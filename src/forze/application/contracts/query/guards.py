"""Type guards for filter expression discrimination."""

from typing import TypeGuard

from .expressions import (
    QueryConjunction,
    QueryDisjunction,
    QueryFieldOpConjunction,
    QueryFieldShortcutValue,
    QueryFilterExpression,
    QueryPredicate,
)

# ----------------------- #


def is_query_predicate(expr: QueryFilterExpression) -> TypeGuard[QueryPredicate]:
    """Return ``True`` when the expression is a predicate (``$fields``)."""

    return "$fields" in expr


def is_query_conjunction(expr: QueryFilterExpression) -> TypeGuard[QueryConjunction]:
    """Return ``True`` when the expression is a conjunction (``$and``)."""

    return "$and" in expr


def is_query_disjunction(expr: QueryFilterExpression) -> TypeGuard[QueryDisjunction]:
    """Return ``True`` when the expression is a disjunction (``$or``)."""

    return "$or" in expr


def is_query_field_conjunction(
    map_: QueryFieldOpConjunction | QueryFieldShortcutValue,
) -> TypeGuard[QueryFieldOpConjunction]:
    """Return ``True`` when the value is an operator map (dict)."""

    return isinstance(map_, dict)


def is_query_field_shortcut(
    map_: QueryFieldOpConjunction | QueryFieldShortcutValue,
) -> TypeGuard[QueryFieldShortcutValue]:
    """Return ``True`` when the value is a shortcut (scalar, array, or None)."""

    return not isinstance(map_, dict)

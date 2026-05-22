"""Type guards for filter expression discrimination."""

from typing import TypeGuard

from .expressions import (
    QueryConstraintPredicate,
    QueryConjunction,
    QueryDisjunction,
    QueryFieldsOpConjunction,
    QueryFilterExpression,
    QueryValueMapValue,
    QueryValueOpConjunction,
    QueryValueShortcutValue,
)

# ----------------------- #

_COMBINATOR_KEYS = frozenset({"$and", "$or"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})


def is_query_conjunction(expr: QueryFilterExpression) -> TypeGuard[QueryConjunction]:  # type: ignore[valid-type]
    """Return ``True`` when the expression is a conjunction (``$and``)."""

    return "$and" in expr.keys()  # type: ignore[attr-defined]


def is_query_disjunction(expr: QueryFilterExpression) -> TypeGuard[QueryDisjunction]:  # type: ignore[valid-type]
    """Return ``True`` when the expression is a disjunction (``$or``)."""

    return "$or" in expr.keys()  # type: ignore[attr-defined]


def is_query_constraint(expr: QueryFilterExpression) -> TypeGuard[QueryConstraintPredicate]:  # type: ignore[valid-type]
    """Return ``True`` when the expression has ``$values`` and/or ``$fields`` constraints."""

    keys = expr.keys()  # type: ignore[attr-defined]
    return bool(_CONSTRAINT_KEYS & keys) and not (_COMBINATOR_KEYS & keys)


def has_query_values(expr: QueryFilterExpression) -> bool:  # type: ignore[valid-type]
    """Return ``True`` when ``$values`` is present."""

    return "$values" in expr.keys()  # type: ignore[attr-defined]


def has_query_fields(expr: QueryFilterExpression) -> bool:  # type: ignore[valid-type]
    """Return ``True`` when field-to-field ``$fields`` is present."""

    return "$fields" in expr.keys()  # type: ignore[attr-defined]


def is_query_value_conjunction(
    map_: QueryValueOpConjunction | QueryValueMapValue,
) -> TypeGuard[QueryValueOpConjunction]:
    """Return ``True`` when the value is an operator map (dict)."""

    return isinstance(map_, dict)


def is_query_value_shortcut(
    map_: QueryValueOpConjunction | QueryValueMapValue,
) -> TypeGuard[QueryValueShortcutValue]:
    """Return ``True`` when the value is a shortcut (scalar, array, or None)."""

    return not isinstance(map_, dict)


def is_query_fields_conjunction(
    map_: QueryFieldsOpConjunction | str,
) -> TypeGuard[QueryFieldsOpConjunction]:
    """Return ``True`` when the field-compare value is an operator map (dict)."""

    return isinstance(map_, dict)


def is_query_fields_shortcut(
    map_: QueryFieldsOpConjunction | str,
) -> TypeGuard[str]:
    """Return ``True`` when the field-compare value is an ``$eq`` shortcut (path str)."""

    return isinstance(map_, str)

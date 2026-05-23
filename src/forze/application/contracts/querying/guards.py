"""Type guards for filter expression discrimination."""

from typing import TypeGuard, get_args

from .expressions import (
    QueryConstraintPredicate,
    QueryConjunction,
    QueryDisjunction,
    QueryElementQuantifierExpression,
    QueryFieldsOpConjunction,
    QueryFilterExpression,
    QueryNegation,
    QueryValueMapValue,
    QueryValueOpConjunction,
    QueryValueShortcutValue,
)
from .types import QueryElementQuantifier

# ----------------------- #

_COMBINATOR_KEYS = frozenset({"$and", "$or", "$not"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})
_ELEMENT_QUANTIFIER_KEYS = frozenset(get_args(QueryElementQuantifier))  # type: ignore[arg-type]


def is_query_conjunction(expr: QueryFilterExpression) -> TypeGuard[QueryConjunction]:  # type: ignore[valid-type]
    """Return ``True`` when the expression is a conjunction (``$and``)."""

    return "$and" in expr.keys()  # type: ignore[attr-defined]


def is_query_disjunction(expr: QueryFilterExpression) -> TypeGuard[QueryDisjunction]:  # type: ignore[valid-type]
    """Return ``True`` when the expression is a disjunction (``$or``)."""

    return "$or" in expr.keys()  # type: ignore[attr-defined]


def is_query_negation(expr: QueryFilterExpression) -> TypeGuard[QueryNegation]:  # type: ignore[valid-type]
    """Return ``True`` when the expression is a negation (``$not``)."""

    return "$not" in expr.keys()  # type: ignore[attr-defined]


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


def is_query_element_quantifier(
    map_: QueryValueMapValue,
) -> TypeGuard[QueryElementQuantifierExpression]:
    """Return ``True`` when the value is an element quantifier map (``$any`` / ``$all`` / ``$none``)."""

    if not isinstance(map_, dict):
        return False
    keys = map_.keys()  # type: ignore[attr-defined]
    return bool(_ELEMENT_QUANTIFIER_KEYS & keys) and len(keys) == 1


def is_query_value_conjunction(
    map_: QueryValueOpConjunction | QueryValueMapValue,
) -> TypeGuard[QueryValueOpConjunction]:
    """Return ``True`` when the value is a field operator map (dict, not a quantifier)."""

    return isinstance(map_, dict) and not is_query_element_quantifier(map_)  # type: ignore[arg-type]


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

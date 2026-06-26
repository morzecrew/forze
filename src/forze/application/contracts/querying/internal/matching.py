"""In-memory evaluation of a :data:`QueryFilterExpression` against a plain row mapping.

The query DSL (AST, parser, validators) lives in this package; this module adds the matching
*semantics* — the function that decides whether a row satisfies a filter — so they are defined
once and shared by every consumer that needs to filter in process: the in-memory mock adapter
(``forze_mock``) and the deterministic-simulation isolation oracle (``forze_dst``), which evaluates
a captured scan predicate against a captured write to direct predicate (phantom) dependency edges.
Keeping a single evaluator is what lets the oracle's predicate semantics match the backend's exactly
— a second implementation could silently diverge and turn a real anomaly into a false verdict.

Rows are plain ``dict``s (a ``model_dump``ed document); a missing field reads as :data:`_MISSING`,
distinct from a present ``None``, so ``$null`` / ``$neq`` behave correctly on absent fields.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Sequence, cast
from uuid import UUID

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .nodes import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from .parse import QueryFilterExpressionParser
from .text_pattern import like_pattern_to_regex

if TYPE_CHECKING:
    from ..expressions import QueryFilterExpression

# ----------------------- #

_MISSING = object()
"""Sentinel for an absent field — distinct from a present ``None`` (``$null`` must tell them apart)."""


# ....................... #


def _path_get(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return _MISSING
            cur = cur[part]  # pyright: ignore[reportUnknownVariableType]
            continue

        return _MISSING

    return cur  # pyright: ignore[reportUnknownVariableType]


def _value_is_empty(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, (str, bytes, bytearray, list, tuple, dict, set, frozenset)):
        return len(value) == 0  # pyright: ignore[reportUnknownArgumentType]

    return False


def _coerce_set(value: Any) -> set[Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        return set(value)  # pyright: ignore[reportUnknownArgumentType]

    return {value}


def _eq(left: Any, right: Any) -> bool:
    if left == right:
        return True

    if isinstance(left, UUID):
        return str(left) == str(right)

    if isinstance(right, UUID):
        return str(left) == str(right)

    return False


def _memb_contains(field_value: Any, values: Sequence[Any]) -> bool:
    if isinstance(field_value, Sequence) and not isinstance(
        field_value, (str, bytes, bytearray)
    ):
        return any(
            _eq(item, candidate)
            for item in field_value  # pyright: ignore[reportUnknownVariableType]
            for candidate in values
        )

    return any(_eq(field_value, candidate) for candidate in values)


def _match_text(value: Any, op: str, pattern: str) -> bool:
    if value is _MISSING:
        return False
    text = str(value)
    match op:
        case "$like":
            return re.search(like_pattern_to_regex(pattern), text) is not None
        case "$ilike":
            return (
                re.search(
                    like_pattern_to_regex(pattern, case_insensitive=True),
                    text,
                )
                is not None
            )
        case "$regex":
            return re.search(pattern, text) is not None
        case _:
            return False


def _is_descendant_path(a: Any, b: Any) -> bool:
    """Whether materialized path *a* is at or below path *b* (label-aware, inclusive).

    Compares dot-separated label sequences: *a* is a descendant of *b* when *b* is a
    label-boundary prefix of *a* (``top.science`` is *not* a descendant of ``top.sci``).
    Equal paths qualify — a node is its own ancestor and descendant.
    """

    if not isinstance(a, str) or not isinstance(b, str):
        return False

    a_labels = a.split(".")
    b_labels = b.split(".")

    return len(a_labels) >= len(b_labels) and a_labels[: len(b_labels)] == b_labels


def _match_field(doc: JsonDict, field: QueryField) -> bool:
    value = _path_get(doc, field.name)

    match field.op:
        case "$eq":
            if value is _MISSING:
                return False
            return _eq(value, field.value)

        case "$neq":
            if value is _MISSING:
                return True
            return not _eq(value, field.value)

        case "$gt":
            if value is _MISSING:
                return False
            try:
                return value > field.value
            except TypeError:
                return False

        case "$gte":
            if value is _MISSING:
                return False
            try:
                return value >= field.value
            except TypeError:
                return False

        case "$lt":
            if value is _MISSING:
                return False
            try:
                return value < field.value
            except TypeError:
                return False

        case "$lte":
            if value is _MISSING:
                return False
            try:
                return value <= field.value
            except TypeError:
                return False

        case "$null":
            should_be_null = bool(field.value)
            if should_be_null:
                return value is _MISSING or value is None
            return value is not _MISSING and value is not None

        case "$empty":
            should_be_empty = bool(field.value)
            if value is _MISSING:
                return False
            return _value_is_empty(value) is should_be_empty

        case "$in":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _memb_contains(value, values)

        case "$nin":
            if value is _MISSING:
                return True
            values = cast(Sequence[Any], field.value)
            return not _memb_contains(value, values)

        case "$superset":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).issuperset(values)

        case "$subset":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).issubset(values)

        case "$disjoint":
            if value is _MISSING:
                return True
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).isdisjoint(values)

        case "$overlaps":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return not _coerce_set(value).isdisjoint(values)

        case "$like" | "$ilike" | "$regex":
            return _match_text(value, field.op, str(field.value))

        case "$descendant_of":
            if value is _MISSING:
                return False
            return _is_descendant_path(value, field.value)

        case "$ancestor_of":
            if value is _MISSING:
                return False
            return _is_descendant_path(field.value, value)


def _match_compare(doc: JsonDict, node: QueryCompare) -> bool:
    left_value = _path_get(doc, node.left)
    right_value = _path_get(doc, node.right)

    match node.op:
        case "$eq":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            return _eq(left_value, right_value)

        case "$neq":
            if left_value is _MISSING:
                return True
            if right_value is _MISSING:
                return True
            return not _eq(left_value, right_value)

        case "$gt":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return left_value > right_value
            except TypeError:
                return False

        case "$gte":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return left_value >= right_value
            except TypeError:
                return False

        case "$lt":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return left_value < right_value
            except TypeError:
                return False

        case "$lte":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return left_value <= right_value
            except TypeError:
                return False


def _elem_vacuous_match(quantifier: str) -> bool:
    return quantifier in ("$all", "$none")


def _normalize_array_value(raw: Any) -> list[Any] | None:
    if raw is _MISSING or raw is None:
        return None

    if isinstance(raw, list):
        return raw  # type: ignore[return-value]

    return None


def _match_elem_inner(elem: Any, inner: QueryExpr) -> bool:
    match inner:
        case QueryField() as field if field.name == ELEM_SCALAR_FIELD:
            return _match_field({ELEM_SCALAR_FIELD: elem}, field)
        case QueryField() as field:
            if not isinstance(elem, dict):
                return False
            return _match_field(cast(JsonDict, elem), field)
        case QueryAnd(items):
            # Recurse each item so scalar-element conjunctions (a range over a
            # primitive element, e.g. {$gt:1, $lt:3}) and object-element conjunctions
            # are both handled — not just object elements.
            return all(_match_elem_inner(elem, item) for item in items)
        case QueryOr(items):
            return any(_match_elem_inner(elem, item) for item in items)
        case QueryElem() as nested:
            if nested.path == ELEM_SCALAR_FIELD:
                # Scalar array-of-arrays: the element is itself the sub-array to quantify.
                return _match_elem_over(elem, nested)

            # A nested quantifier: the element is itself a document with a sub-array.
            if not isinstance(elem, dict):
                return False
            return _match_elem(cast(JsonDict, elem), nested)
        case _:
            return False


def _match_elem_over(raw: Any, node: QueryElem) -> bool:
    """Quantify *node* over the array value *raw* (already extracted, not a field path)."""

    arr = _normalize_array_value(raw)

    if not arr:  # ``None`` (missing/non-array) or empty → vacuous
        return _elem_vacuous_match(node.quantifier)

    results = [_match_elem_inner(item, node.inner) for item in arr]

    match node.quantifier:
        case "$any":
            return any(results)
        case "$all":
            return all(results)
        case "$none":
            return not any(results)


def _match_elem(doc: JsonDict, node: QueryElem) -> bool:
    return _match_elem_over(_path_get(doc, node.path), node)


def _match_expr(doc: JsonDict, expr: QueryExpr) -> bool:
    match expr:
        case QueryField():
            return _match_field(doc, expr)

        case QueryCompare():
            return _match_compare(doc, expr)

        case QueryOr(items=items):
            return any(_match_expr(doc, item) for item in items)

        case QueryAnd(items=items):
            return all(_match_expr(doc, item) for item in items)

        case QueryNot(item):
            return not _match_expr(doc, item)

        case QueryElem():
            return _match_elem(doc, expr)

        case _:
            raise exc.internal(f"Unknown query expression: {expr!r}")


def _match_filters(doc: JsonDict, filters: "QueryFilterExpression | None") -> bool:  # type: ignore[valid-type]
    if filters is None:
        return True

    expr = QueryFilterExpressionParser.parse(filters)
    return _match_expr(doc, expr)


# ....................... #


def evaluate_filter(row: JsonDict, filters: "QueryFilterExpression | None") -> bool:
    """Whether *row* (a plain field mapping) satisfies *filters* — the in-memory DSL evaluator.

    ``None`` filters match every row (a match-all scan). The same parser the adapters use turns the
    expression into the AST, then the row is tested against it with the package's matching semantics
    (so an in-process matcher — the mock adapter, the DST predicate oracle — agrees with the backend
    that renders the same expression to SQL/Mongo). A missing field is distinct from a present
    ``None``.
    """

    return _match_filters(row, filters)

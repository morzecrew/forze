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

import operator
import re
from collections.abc import Callable, Mapping, Sequence, Set
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from types import NoneType
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.projection import MISSING as _MISSING
from forze.base.primitives.projection import path_get as _path_get
from forze.base.serialization import enum_values

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

# The dotted-path walker (:func:`_path_get`) and the absent-field sentinel (:data:`_MISSING`)
# now live in ``forze.base.primitives.projection`` so the filter matcher, the projection
# reshaper, and the backends all resolve nested paths through one implementation. They are
# imported (aliased) above; consumers that do ``from ...matching import _path_get`` still work.


def _value_is_empty(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, (str, bytes, bytearray, list, tuple, dict, set, frozenset)):
        return len(value) == 0  # pyright: ignore[reportUnknownArgumentType]

    return False


def _coerce_set(value: Any) -> set[Any]:
    items = value if isinstance(value, (list, tuple, set, frozenset)) else (value,)

    return {_set_key(item) for item in items}  # pyright: ignore[reportUnknownVariableType]


def _set_key(item: Any) -> Any:
    """*item* as a set member: an ``Enum`` member as its value, or as itself when the value
    cannot be hashed (a list); a ``UUID`` as its text, which ``$in`` also matches it by."""

    plain = _plain(item)

    try:
        hash(plain)

    except TypeError:
        plain = item

    if isinstance(plain, UUID):
        plain = str(plain)

    return instant_key(plain)


_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_INSTANT: Final = object()


def _is_aware(value: Any) -> bool:
    if not isinstance(value, datetime):
        return False

    # An app-defined tzinfo may raise; a value that cannot say whether it is aware is compared
    # as it is, which is what an unreadable operand has always been here.
    try:
        return value.utcoffset() is not None

    except Exception:
        return False


def instant_key(value: Any) -> Any:
    """A key that is equal for equal instants: an aware datetime as its distance from the UTC
    epoch, tagged so it never equals a plain ``timedelta``; anything else as is.

    For hashing and equality (set members, unique keys). A store compares ``timestamptz`` values
    as instants, while Python compares two datetimes sharing a ``tzinfo`` by their wall clocks,
    ignoring ``fold``, and across zones treats a value in a repeated hour as equal to nothing.
    """

    return (_INSTANT, value - _EPOCH) if _is_aware(value) else value


def _plain(value: Any) -> Any:
    """*value* with each ``Enum`` member in it, in containers too, as its value, as a store
    holds it.

    A row keeps the member (a ``model_dump``) while a filter names the value — the parser takes
    no plain member operand — and a plain ``Enum`` member equals nothing but itself.
    """

    return enum_values(value)


def _pair(left: Any, right: Any) -> tuple[Any, Any]:
    """*left* and *right* as instants when both are aware datetimes; otherwise as they are.

    Both or neither: a lone distance would compare cleanly against a ``timedelta`` operand,
    which a store refuses.
    """

    if _is_aware(left) and _is_aware(right):
        return left - _EPOCH, right - _EPOCH

    return left, right


def _ordered(compare: Callable[[Any, Any], bool], left: Any, right: Any) -> bool:
    return compare(*_pair(_plain(left), _plain(right)))


def _eq(left: Any, right: Any) -> bool:
    left, right = _plain(left), _plain(right)

    if operator.eq(*_pair(left, right)):
        return True

    if isinstance(left, UUID):
        return str(left) == str(right)

    if isinstance(right, UUID):
        return str(left) == str(right)

    return False


_UNKEYED: Final = object()

# Types whose ``==`` is the one :func:`_eq` applies and agrees with their hash, across all of
# them (``1``, ``1.0``, ``True`` and ``Decimal(1)`` are one key, as they are one value to ``_eq``).
_KEYED_TYPES: Final = frozenset({str, int, bool, float, Decimal, NoneType, date})


def _memb_key(value: Any) -> Any:
    """*value* as a hash key that equals another's exactly when :func:`_eq` matches the two
    values, or :data:`_UNKEYED` when ``_eq`` has rules a hash cannot express.

    Only exact builtin types are keyed: a subclass or an app type may define its own ``==``.
    A ``UUID`` is keyed by its text, which ``_eq`` also matches it by, and an aware datetime by
    its instant. A NaN is not keyed: it equals nothing, yet a set finds it by identity.
    """

    while isinstance(value, Enum):
        value = value.value

    kind = type(value)

    if kind is UUID:
        return str(value)

    if kind is datetime:
        if value.tzinfo is None:
            return value

        return (_INSTANT, value - _EPOCH) if _is_aware(value) else _UNKEYED

    if kind not in _KEYED_TYPES:
        return _UNKEYED

    if kind is float and value != value:
        return _UNKEYED

    if kind is Decimal and value.is_nan():
        return _UNKEYED

    return value


def _memb_keys(values: Sequence[Any]) -> frozenset[Any] | None:
    """The :func:`_memb_key` of every candidate, or ``None`` if any has none.

    One unkeyed candidate turns the lookup off for the whole operand: its own ``==`` could
    match any row value, so only the linear :func:`_eq` scan answers for it.
    """

    if not isinstance(values, (list, tuple)):
        return None

    keys = {_memb_key(candidate) for candidate in values}

    return None if _UNKEYED in keys else frozenset(keys)


def _memb_one(value: Any, values: Sequence[Any], keys: frozenset[Any] | None) -> bool:
    if keys is not None:
        key = _memb_key(value)

        if key is not _UNKEYED:
            return key in keys

    return any(_eq(value, candidate) for candidate in values)


def _memb_contains(
    field_value: Any,
    values: Sequence[Any],
    keys: frozenset[Any] | None = None,
) -> bool:
    """Whether *field_value*, or an item of it when it is a list, ``_eq``-matches a candidate.

    *keys* are the candidates' :func:`_memb_keys`: a keyed value is then looked up rather than
    compared with each candidate, with the same answer.
    """

    if isinstance(field_value, Sequence) and not isinstance(field_value, (str, bytes, bytearray)):
        return any(
            _memb_one(item, values, keys)
            for item in field_value  # pyright: ignore[reportUnknownVariableType]
        )

    return _memb_one(field_value, values, keys)


def _try_coerce_set(values: Sequence[Any]) -> frozenset[Any] | None:
    # A candidate that cannot be a set member raises on every row instead, as it always has.
    try:
        return frozenset(_coerce_set(values))

    except Exception:
        return None


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class _PreparedField(QueryField):
    """A membership leaf with its operand worked out once, by :func:`compile_expr`."""

    keys: frozenset[Any] | None
    """``$in`` / ``$nin``: the candidates' :func:`_memb_keys`. A set relation: the operand as
    :func:`_coerce_set` builds it. ``None``: worked out per row, as without preparing."""


_KEYED_OPS: Final = frozenset({"$in", "$nin"})
_PREPARED_OPS: Final = _KEYED_OPS | {"$superset", "$subset", "$disjoint", "$overlaps"}


def _prepare(expr: QueryExpr) -> QueryExpr:
    """*expr* with each membership leaf replaced by a :class:`_PreparedField`."""

    match expr:
        case QueryField() if type(expr) is QueryField and expr.op in _PREPARED_OPS:
            values = cast(Sequence[Any], expr.value)
            keys = _memb_keys(values) if expr.op in _KEYED_OPS else _try_coerce_set(values)

            return _PreparedField(expr.name, expr.op, expr.value, keys)

        case QueryAnd() | QueryOr():
            return attrs.evolve(expr, items=tuple(_prepare(item) for item in expr.items))

        case QueryNot():
            return attrs.evolve(expr, item=_prepare(expr.item))

        case QueryElem():
            return attrs.evolve(expr, inner=_prepare(expr.inner))

        case _:
            return expr


def _prepared_keys(field: QueryField) -> frozenset[Any] | None:
    return field.keys if isinstance(field, _PreparedField) else None


def _operand_set(field: QueryField) -> Set[Any]:
    keys = _prepared_keys(field)

    return keys if keys is not None else _coerce_set(field.value)


# ....................... #


@lru_cache(maxsize=256)
def _like_regex(pattern: str, case_insensitive: bool) -> re.Pattern[str]:
    """The compiled regex for a ``$like`` (or, *case_insensitive*, ``$ilike``) *pattern*.

    Keyed by both arguments, the only inputs of :func:`like_pattern_to_regex`.
    """

    return re.compile(like_pattern_to_regex(pattern, case_insensitive=case_insensitive))


def _match_text(value: Any, op: str, pattern: str) -> bool:
    # A missing field — or a present ``None`` — never matches a text pattern: a backend's
    # ``NULL LIKE``/regex yields no match, and the comparison ops above already guard ``None`` to
    # ``False``. Without this, ``str(None)`` becomes ``"None"`` and spuriously matches e.g. ``%on%``.
    if value is _MISSING or value is None:
        return False
    text = str(value)
    match op:
        case "$like":
            return _like_regex(pattern, False).search(text) is not None
        case "$ilike":
            return _like_regex(pattern, True).search(text) is not None
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
            # Incomparable operands read as "no match", never as an error. TypeError
            # covers mixed types; ArithmeticError covers Decimal NaN, whose ordered
            # comparisons raise InvalidOperation where float NaN merely returns False
            # (the caster refuses non-finite operands, but a native Decimal("NaN")
            # reaches here without passing through it).
            try:
                return _ordered(operator.gt, value, field.value)
            except (TypeError, ArithmeticError):
                return False

        case "$gte":
            if value is _MISSING:
                return False
            try:
                return _ordered(operator.ge, value, field.value)
            except (TypeError, ArithmeticError):
                return False

        case "$lt":
            if value is _MISSING:
                return False
            try:
                return _ordered(operator.lt, value, field.value)
            except (TypeError, ArithmeticError):
                return False

        case "$lte":
            if value is _MISSING:
                return False
            try:
                return _ordered(operator.le, value, field.value)
            except (TypeError, ArithmeticError):
                return False

        case "$null":
            # A member whose value is ``None`` is stored as ``NULL``.
            is_null = value is _MISSING or _plain(value) is None
            return is_null is bool(field.value)

        case "$empty":
            should_be_empty = bool(field.value)
            if value is _MISSING:
                return False
            return _value_is_empty(value) is should_be_empty

        case "$in":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _memb_contains(value, values, _prepared_keys(field))

        case "$nin":
            if value is _MISSING:
                return True
            values = cast(Sequence[Any], field.value)
            return not _memb_contains(value, values, _prepared_keys(field))

        case "$superset":
            if value is _MISSING:
                return False
            return _coerce_set(value).issuperset(_operand_set(field))

        case "$subset":
            if value is _MISSING:
                return False
            return _coerce_set(value).issubset(_operand_set(field))

        case "$disjoint":
            if value is _MISSING:
                return True
            return _coerce_set(value).isdisjoint(_operand_set(field))

        case "$overlaps":
            if value is _MISSING:
                return False
            return not _coerce_set(value).isdisjoint(_operand_set(field))

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
                return _ordered(operator.gt, left_value, right_value)
            except (TypeError, ArithmeticError):
                return False

        case "$gte":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return _ordered(operator.ge, left_value, right_value)
            except (TypeError, ArithmeticError):
                return False

        case "$lt":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return _ordered(operator.lt, left_value, right_value)
            except (TypeError, ArithmeticError):
                return False

        case "$lte":
            if left_value is _MISSING or right_value is _MISSING:
                return False
            try:
                return _ordered(operator.le, left_value, right_value)
            except (TypeError, ArithmeticError):
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


def _match_filters(doc: JsonDict, filters: QueryFilterExpression | None) -> bool:  # type: ignore[valid-type]
    if filters is None:
        return True

    expr = QueryFilterExpressionParser.parse(filters)
    return _match_expr(doc, expr)


# ....................... #


def evaluate_filter(row: JsonDict, filters: QueryFilterExpression | None) -> bool:
    """Whether *row* (a plain field mapping) satisfies *filters* — the in-memory DSL evaluator.

    ``None`` filters match every row (a match-all scan). The same parser the adapters use turns the
    expression into the AST, then the row is tested against it with the package's matching semantics
    (so an in-process matcher — the mock adapter, the DST predicate oracle — agrees with the backend
    that renders the same expression to SQL/Mongo). A missing field is distinct from a present
    ``None``.
    """

    return _match_filters(row, filters)


def compile_filter(
    filters: QueryFilterExpression | None,
    *,
    parser: QueryFilterExpressionParser | None = None,
) -> Callable[[JsonDict], bool]:
    """Parse *filters* **once** into a reusable ``row -> bool`` predicate (``None`` ⇒ match-all).

    Same semantics as :func:`evaluate_filter`, but the (re-usable) parse happens up front rather than
    per call — for a caller that tests one filter against many rows (the DST predicate oracle matching
    a scan's filter against every concurrent write), this avoids re-parsing the expression each time.
    May raise at parse time on a malformed filter; the returned matcher itself does not parse.
    *parser* carries a spec's filter limits; the default limits apply when it is omitted.
    """

    if filters is None:
        return lambda _row: True

    expr = (
        parser.parse_filter(filters)
        if parser is not None
        else QueryFilterExpressionParser.parse(filters)
    )
    return compile_expr(expr)


def compile_expr(expr: QueryExpr) -> Callable[[JsonDict], bool]:
    """A reusable ``row -> bool`` predicate for the parsed *expr*, with the same semantics.

    Work that depends on the filter alone is done here, once: a membership operand
    (``$in``, ``$nin``, a set relation) is turned into a hash lookup, so a row costs one probe
    rather than a comparison with each candidate.
    """

    prepared = _prepare(expr)

    return lambda row: _match_expr(row, prepared)


def value_at_path(row: Mapping[str, Any], path: str) -> Any:
    """The value at a dotted *path* in *row*, or ``None`` if any segment is absent.

    The value-extraction counterpart to :func:`evaluate_filter`'s path handling — for a caller that
    needs a nested field's *value* (a grouped aggregate's group key or its sum field), not a match.
    A present ``None`` and an absent field both read as ``None``; use :func:`evaluate_filter` with
    ``$null`` when that distinction matters.
    """

    value = _path_get(row, path)
    return None if value is _MISSING else value

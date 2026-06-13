"""In-memory query filter, sort, and aggregate helpers."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import (
    Any,
    Sequence,
    cast,
)
from uuid import UUID

from functools import cmp_to_key

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    AggregatesExpression,
    AggregatesExpressionParser,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryNot,
    QueryOr,
    QuerySortExpression,
    ordered_compare,
    resolve_sort_keys,
)
from forze.application.contracts.querying.internal.text_pattern import (
    like_pattern_to_regex,
)
from forze.application.contracts.querying.internal.time_bucket import (
    floor_to_time_bucket,
    tzinfo_from_resolved,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.query._types import _MISSING  # type: ignore[reportPrivateUsage]


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


def _path_text(obj: Any, path: str) -> str:  # type: ignore[reportPrivateUsage]
    value = _path_get(obj, path)
    if value is _MISSING or value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return " ".join(
            str(x)  # pyright: ignore[reportUnknownArgumentType]
            for x in value  # pyright: ignore[reportUnknownVariableType]
        )

    return str(value)


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


def _match_filters(doc: JsonDict, filters: QueryFilterExpression | None) -> bool:  # type: ignore[valid-type]
    if filters is None:
        return True

    expr = QueryFilterExpressionParser.parse(filters)
    return _match_expr(doc, expr)


def _project(doc: JsonDict, return_fields: Sequence[str] | None) -> JsonDict:  # type: ignore[reportPrivateUsage]
    if return_fields is None:
        return dict(doc)

    out: JsonDict = {}
    for path in return_fields:
        value = _path_get(doc, path)
        if value is _MISSING:
            continue
        out[path] = value

    return out


def _sort_docs(  # type: ignore[reportPrivateUsage]
    docs: list[JsonDict],
    sorts: QuerySortExpression | None,
) -> list[JsonDict]:
    keys = resolve_sort_keys(sorts)

    if not keys:
        return docs

    def _val(d: JsonDict, field: str) -> Any:
        v = _path_get(d, field)
        return None if v is _MISSING else v

    def _cmp(a: JsonDict, b: JsonDict) -> int:
        # Same canonical key comparison as keyset pagination: type-aware, per-key
        # direction, and null = smallest unless an explicit ``nulls`` overrides — so
        # offset and cursor sorts agree, and a missing field sorts like a null.
        for field, direction, nulls in keys:
            c = ordered_compare(
                _val(a, field),
                _val(b, field),
                direction=direction,
                nulls=nulls,
            )

            if c:
                return c

        return 0

    return sorted(docs, key=cmp_to_key(_cmp))


def _require_numeric(value: Any, *, function: str, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise exc.internal(
            f"Aggregate {function} expects numeric values for field {field!r}",
        )
    return value


def _coerce_datetime_for_bucket(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)

    if isinstance(raw, str):
        s = raw.strip().replace("Z", "+00:00")

        return datetime.fromisoformat(s)

    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)

    raise exc.internal(f"Invalid timestamp for $trunc: {raw!r}")


def _group_key_part(doc: JsonDict, expr: object) -> Any:
    from forze.application.contracts.querying import GroupRef, GroupTrunc

    match expr:
        case GroupRef(field=field):
            value = _path_get(doc, field)
            return None if value is _MISSING else value
        case GroupTrunc(field=field, unit=unit, timezone=tz):
            raw_ts = _path_get(doc, field)
            if raw_ts is _MISSING:
                return None
            tb_tz = tzinfo_from_resolved(tz)
            floored = floor_to_time_bucket(
                _coerce_datetime_for_bucket(raw_ts),
                unit=unit,
                tz=tb_tz,
            )
            return floored.isoformat()
        case _:
            raise exc.internal(f"Unsupported group expression: {expr!r}")


def _aggregate_docs(  # type: ignore[reportPrivateUsage]
    docs: Sequence[JsonDict], aggregates: AggregatesExpression
) -> list[JsonDict]:
    parsed = AggregatesExpressionParser.parse(aggregates)

    grouped: dict[tuple[Any, ...], list[JsonDict]] = {}

    for doc in docs:
        parts = tuple(_group_key_part(doc, group.expr) for group in parsed.groups)
        grouped.setdefault(parts, []).append(doc)

    if not parsed.groups and not grouped:
        grouped[()] = []

    rows: list[JsonDict] = []
    for key, items in grouped.items():
        row: JsonDict = {}
        for group, value in zip(parsed.groups, key, strict=True):
            row[group.alias] = value

        for computed in parsed.computed_fields:
            computed_items = (
                [doc for doc in items if _match_filters(doc, computed.filter)]
                if computed.filter is not None
                else items
            )

            if computed.function == "$count":
                row[computed.alias] = len(computed_items)
                continue

            if computed.field is None:
                raise exc.internal("Computed field has no field path")

            raw_values = [_path_get(doc, computed.field) for doc in computed_items]
            values = [
                value
                for value in raw_values
                if value is not _MISSING and value is not None
            ]

            match computed.function:
                case "$sum":
                    nums = [
                        _require_numeric(
                            value,
                            function=computed.function,
                            field=computed.field,
                        )
                        for value in values
                    ]
                    row[computed.alias] = sum(nums) if nums else None

                case "$avg":
                    nums = [
                        _require_numeric(
                            value,
                            function=computed.function,
                            field=computed.field,
                        )
                        for value in values
                    ]
                    row[computed.alias] = (sum(nums) / len(nums)) if nums else None

                case "$median":
                    nums = sorted(
                        _require_numeric(
                            value,
                            function=computed.function,
                            field=computed.field,
                        )
                        for value in values
                    )
                    if not nums:
                        row[computed.alias] = None
                    elif len(nums) % 2:
                        row[computed.alias] = nums[len(nums) // 2]
                    else:
                        hi = len(nums) // 2
                        row[computed.alias] = (nums[hi - 1] + nums[hi]) / 2

                case "$min":
                    row[computed.alias] = min(values) if values else None

                case "$max":
                    row[computed.alias] = max(values) if values else None

        rows.append(row)

    if parsed.having is not None:
        # ``$having``: keep only aggregated rows matching the post-group filter.
        rows = [row for row in rows if _match_expr(row, parsed.having)]

    return rows

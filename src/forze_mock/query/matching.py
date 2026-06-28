"""In-memory query sort and aggregate helpers (the filter matcher lives in core).

The filter-evaluation semantics — :func:`_match_filters` / :func:`_match_expr` / :func:`_match_field`
and the dotted :func:`_path_get` / :data:`_MISSING` sentinel — now live in
``forze.application.contracts.querying.internal.matching`` (the public entry is ``evaluate_filter``)
so the mock and the DST predicate oracle share one evaluator and can never silently diverge. They
are re-imported here so the adapters' existing ``from forze_mock.query.matching import …`` imports
keep working. The sort, projection, and aggregate folds below remain mock-local.
"""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from functools import cmp_to_key
from typing import (
    Any,
    Sequence,
    cast,
)

from forze.application.contracts.querying import (
    AggregatesExpression,
    AggregatesExpressionParser,
    QuerySortExpression,
    compile_filter,
    ordered_compare,
    resolve_sort_keys,
)
from forze.application.contracts.querying.internal.matching import (
    _MISSING,  # type: ignore[reportPrivateUsage]
    _coerce_set,  # type: ignore[reportPrivateUsage]
    _is_descendant_path,  # type: ignore[reportPrivateUsage]
    _match_expr,  # type: ignore[reportPrivateUsage]
    _match_field,  # type: ignore[reportPrivateUsage]
    _match_filters,  # type: ignore[reportPrivateUsage]
    _match_text,  # type: ignore[reportPrivateUsage]
    _memb_contains,  # type: ignore[reportPrivateUsage]
    _normalize_array_value,  # type: ignore[reportPrivateUsage]
    _path_get,  # type: ignore[reportPrivateUsage]
    _value_is_empty,  # type: ignore[reportPrivateUsage]
)
from forze.application.contracts.querying.internal.time_bucket import (
    floor_to_time_bucket,
    tzinfo_from_resolved,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# The matcher privates are re-exported (sourced from core) so existing
# ``from forze_mock.query.matching import …`` imports — adapters and the matcher coverage test —
# keep resolving after the move.
__all__ = [
    "_MISSING",
    "_coerce_set",
    "_is_descendant_path",
    "_match_expr",
    "_match_field",
    "_match_filters",
    "_match_text",
    "_memb_contains",
    "_normalize_array_value",
    "_path_get",
    "_value_is_empty",
    "_path_text",
    "_project",
    "_sort_docs",
    "_aggregate_docs",
]


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

    # Resolve each sort key's value once per document (one dotted-path walk each)
    # rather than re-walking it on every pairwise comparison the sort performs.
    decorated = [([_val(doc, field) for field, _, _ in keys], doc) for doc in docs]

    def _cmp(
        a: tuple[list[Any], JsonDict],
        b: tuple[list[Any], JsonDict],
    ) -> int:
        # Same canonical key comparison as keyset pagination: type-aware, per-key
        # direction, and null = smallest unless an explicit ``nulls`` overrides — so
        # offset and cursor sorts agree, and a missing field sorts like a null.
        a_values, b_values = a[0], b[0]

        for index, (_field, direction, nulls) in enumerate(keys):
            c = ordered_compare(
                a_values[index],
                b_values[index],
                direction=direction,
                nulls=nulls,
            )

            if c:
                return c

        return 0

    decorated.sort(key=cmp_to_key(_cmp))

    return [doc for _values, doc in decorated]


def _require_numeric(value: Any, *, function: str, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise exc.internal(
            f"Aggregate {function} expects numeric values for field {field!r}",
        )
    return value


def _numeric_values(values: list[Any], computed: Any) -> list[int | float]:
    return [
        _require_numeric(value, function=computed.function, field=computed.field)
        for value in values
    ]


def _percentile_cont(nums: list[int | float], p: float) -> float | None:
    """Continuous (interpolated) percentile — matches Postgres ``percentile_cont``."""

    if not nums:
        return None

    ordered = sorted(nums)

    if len(ordered) == 1:
        return float(ordered[0])

    idx = p * (len(ordered) - 1)
    lo = math.floor(idx)
    hi = math.ceil(idx)

    if lo == hi:
        return float(ordered[lo])

    return ordered[lo] + (ordered[hi] - ordered[lo]) * (idx - lo)


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
    from forze.application.contracts.querying import GroupField, GroupTrunc

    match expr:
        case GroupField(field=field):
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

    # Compile each computed field's filter once (not per group, per document) into a
    # reusable predicate; ``None`` means the aggregate sees every group member.
    computed_matchers = [
        compile_filter(computed.filter) if computed.filter is not None else None
        for computed in parsed.computed_fields
    ]

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

        for computed, matcher in zip(
            parsed.computed_fields, computed_matchers, strict=True
        ):
            computed_items = (
                [doc for doc in items if matcher(doc)]
                if matcher is not None
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

                case "$count_distinct":
                    row[computed.alias] = len(set(values))

                case "$stddev_pop":
                    nums = _numeric_values(values, computed)
                    row[computed.alias] = statistics.pstdev(nums) if nums else None

                case "$stddev_samp":
                    nums = _numeric_values(values, computed)
                    row[computed.alias] = (
                        statistics.stdev(nums) if len(nums) >= 2 else None
                    )

                case "$var_pop":
                    nums = _numeric_values(values, computed)
                    row[computed.alias] = statistics.pvariance(nums) if nums else None

                case "$var_samp":
                    nums = _numeric_values(values, computed)
                    row[computed.alias] = (
                        statistics.variance(nums) if len(nums) >= 2 else None
                    )

                case "$percentile":
                    nums = _numeric_values(values, computed)
                    row[computed.alias] = _percentile_cont(
                        nums, cast(float, computed.p)
                    )

        rows.append(row)

    if parsed.having is not None:
        # ``$having``: keep only aggregated rows matching the post-group filter.
        rows = [row for row in rows if _match_expr(row, parsed.having)]

    return rows

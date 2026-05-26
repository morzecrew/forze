"""Shared helpers for warehouse analytics adapters (kernel-only, no SDK imports)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions, AnalyticsSpec
from forze.application.contracts.base import (
    CountlessPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.querying import PaginationExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


def validated_params(
    spec: AnalyticsSpec[Any, Any],
    query_key: str,
    params: BaseModel | JsonDict,
) -> BaseModel:
    """Validate *params* against the spec query definition."""

    try:
        defn = spec.queries[query_key]

    except KeyError as e:
        raise exc.configuration(f"Unknown analytics query key: {query_key!r}") from e

    if isinstance(params, defn.params):
        return params

    if isinstance(params, BaseModel):  # pyright: ignore[reportUnnecessaryIsInstance]
        return pydantic_validate(defn.params, params.model_dump())

    raise exc.configuration("Analytics params must be a Pydantic model instance.")


# ....................... #


def dry_run_enabled(options: AnalyticsRunOptions | None) -> bool:
    return bool((options or {}).get("dry_run"))


# ....................... #


def timeout_seconds(options: AnalyticsRunOptions | None) -> int | None:
    if options is None:
        return None

    timeout = options.get("timeout")

    if timeout is None:
        return None

    return max(1, int(timeout.total_seconds()))


# ....................... #


def pagination_window(
    pagination: PaginationExpression | None,
) -> tuple[int | None, int | None]:
    p = dict(pagination or {})
    limit = p.get("limit")
    offset = p.get("offset")
    max_results = int(cast(Any, limit)) if limit is not None else None
    start_index = int(cast(Any, offset)) if offset is not None else None

    return max_results, start_index


# ....................... #


def shape_rows(
    rows: list[JsonDict],
    *,
    read_type: type[BaseModel],
    return_type: type[T] | None,
    return_fields: Sequence[str] | None,
) -> list[Any]:
    if return_fields is not None:
        return [{k: row.get(k) for k in return_fields} for row in rows]

    if return_type is not None:
        return pydantic_validate_many(return_type, rows)

    return pydantic_validate_many(read_type, rows)


# ....................... #


def dry_run_offset_page(
    pagination: PaginationExpression | None,
    *,
    return_count: bool,
) -> CountlessPage[Any] | Page[Any]:
    empty: list[Any] = []

    if return_count:
        return page_from_limit_offset(empty, pagination, total=0)

    return page_from_limit_offset(empty, pagination, total=None)


# ....................... #


def parse_count_row(rows: list[JsonDict], *, column: str = "forze_cnt") -> int:
    if not rows:
        return 0

    raw = rows[0].get(column, 0)

    return int(raw)

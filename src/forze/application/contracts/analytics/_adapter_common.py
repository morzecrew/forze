"""Shared helpers for warehouse analytics adapters (kernel-only, no SDK imports)."""

from typing import Any, Sequence, TypeVar, cast

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions, AnalyticsSpec
from forze.application.contracts.base import (
    CountlessPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_ANALYTICS_CURSOR_CODEC = B64UrlJsonCodec()

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


# ....................... #


def parse_analytics_cursor_limit(
    cursor: CursorPaginationExpression | None,
) -> int:
    """Return a positive page size from a cursor expression."""

    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.internal("Cursor pagination: pass at most one of 'after' or 'before'")

    lim_raw = c.get("limit")
    lim = int(cast(Any, lim_raw)) if lim_raw is not None else 10

    if lim < 1:
        raise exc.internal("Cursor pagination 'limit' must be positive")

    return lim


# ....................... #


def parse_offset_cursor_after(
    cursor: CursorPaginationExpression | None,
    *,
    backward_not_supported: str = "Backward analytics cursors are not supported.",
) -> tuple[int, int]:
    """Decode an offset-style ``after`` token into ``(start_offset, limit)``."""

    c = dict(cursor or {})
    lim = parse_analytics_cursor_limit(cursor)

    if c.get("before"):
        raise exc.internal(backward_not_supported)

    if not c.get("after"):
        return 0, lim

    try:
        payload = _ANALYTICS_CURSOR_CODEC.loads(str(c["after"]))

        if not isinstance(payload, dict):
            raise exc.internal("Invalid analytics cursor token")

        if "kc" in payload:
            raise exc.internal("Offset cursor token passed to offset-based query.")

        return int(payload["o"]), lim  # type: ignore[arg-type]

    except (ValueError, KeyError, TypeError) as e:
        raise exc.internal("Invalid analytics cursor token") from e


# ....................... #


def parse_keyset_cursor_after(
    cursor: CursorPaginationExpression | None,
    *,
    backward_not_supported: str = "Backward analytics cursors are not supported.",
) -> tuple[Any | None, int]:
    """Decode a keyset ``after`` token into ``(forze_after value, limit)``."""

    c = dict(cursor or {})
    lim = parse_analytics_cursor_limit(cursor)

    if c.get("before"):
        raise exc.internal(backward_not_supported)

    if not c.get("after"):
        return None, lim

    try:
        payload = _ANALYTICS_CURSOR_CODEC.loads(str(c["after"]))

        if not isinstance(payload, dict) or "kv" not in payload:
            raise exc.internal("Invalid analytics keyset cursor token")

        return payload["kv"], lim  # type: ignore[return-value]

    except (ValueError, KeyError, TypeError) as e:
        raise exc.internal("Invalid analytics keyset cursor token") from e


# ....................... #


def encode_offset_cursor_next_prev(
    *,
    start: int,
    page_len: int,
    limit: int,
) -> tuple[str | None, str | None]:
    has_more = page_len >= limit
    next_c = (
        _ANALYTICS_CURSOR_CODEC.dumps({"o": start + page_len}) if has_more else None
    )
    prev_c = _ANALYTICS_CURSOR_CODEC.dumps({"o": start}) if start > 0 else None

    return next_c, prev_c


# ....................... #


def encode_keyset_cursor_next(
    *,
    column: str,
    hits: list[Any],
    limit: int,
) -> str | None:
    has_more = len(hits) >= limit

    if not has_more or not hits:
        return None

    last = hits[-1]

    if isinstance(last, BaseModel):
        value = last.model_dump().get(column)

    elif isinstance(last, dict):
        value = last.get(column)  # type: ignore[assignment]

    else:
        value = getattr(last, column, None)

    if value is None:
        return None

    return _ANALYTICS_CURSOR_CODEC.dumps({"kc": column, "kv": value})


# ....................... #


def merge_forze_after_params(
    param_dict: dict[str, object],
    after_value: Any | None,
) -> dict[str, object]:
    if after_value is None:
        return param_dict

    return {**param_dict, "forze_after": after_value}

"""Shared helpers for warehouse analytics adapters."""

from typing import Any, Awaitable, Callable, Sequence, TypeVar, cast

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
from forze.base.primitives import JsonDict, StrKey
from forze.base.serialization import ModelCodec, default_model_codec

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_ANALYTICS_CURSOR_CODEC = B64UrlJsonCodec()

# ....................... #


def validated_params(
    spec: AnalyticsSpec[Any, Any],
    query_key: StrKey,
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
        return default_model_codec(defn.params).decode_mapping(
            params.model_dump(),
        )

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
    read_codec: ModelCodec[Any, Any] | None,
    read_type: type[BaseModel],
    return_type: type[T] | None,
    return_fields: Sequence[str] | None,
) -> list[Any]:
    if return_fields is not None:
        return [{k: row.get(k) for k in return_fields} for row in rows]

    if return_type is not None:
        return default_model_codec(return_type).decode_mapping_many(rows)

    codec = read_codec or default_model_codec(read_type)

    return codec.decode_mapping_many(rows)


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


async def execute_analytics_offset_page(
    *,
    pagination: PaginationExpression | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    read_codec: ModelCodec[Any, Any] | None,
    read_type: type[BaseModel],
    skip_total: bool,
    fetch_rows: Callable[[int | None, int | None], Awaitable[list[JsonDict]]],
    total_count: Callable[[], Awaitable[int]],
) -> CountlessPage[Any] | Page[Any]:
    """Run the shared offset-page flow: window, fetch, shape, optional total, page.

    Backends supply :paramref:`fetch_rows` (mapping the resolved ``(limit, offset)``
    window to their driver query) and :paramref:`total_count`. Parameter
    validation and dry-run short-circuiting stay in the adapter so error ordering
    and dry-run semantics are unchanged.

    :param pagination: Offset pagination expression (``limit`` / ``offset``).
    :param return_count: When true, attach a total unless :paramref:`skip_total`.
    :param return_type: Optional model type for ``select_*`` projections.
    :param return_fields: Optional field subset for ``project_*`` projections.
    :param read_codec: Read codec for the spec, or ``None`` to derive from ``read_type``.
    :param read_type: Default read model type.
    :param skip_total: When true, never compute a total even if ``return_count``.
    :param fetch_rows: Async ``(limit, offset) -> rows`` fetch callback.
    :param total_count: Async total-count callback, invoked only when needed.
    :returns: A :class:`~forze.application.contracts.base.Page` when a total is
        attached, otherwise a :class:`~forze.application.contracts.base.CountlessPage`.
    """

    limit, offset = pagination_window(pagination)
    rows = await fetch_rows(limit, offset)
    data = shape_rows(
        rows,
        read_codec=read_codec,
        read_type=read_type,
        return_type=return_type,
        return_fields=return_fields,
    )

    if return_count and not skip_total:
        total = await total_count()

        return page_from_limit_offset(data, pagination, total=total)

    return page_from_limit_offset(data, pagination, total=None)


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

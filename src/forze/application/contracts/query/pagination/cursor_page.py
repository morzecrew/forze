"""Trim over-fetched rows and build next/prev keyset tokens for cursor pages."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from .cursor_token import encode_keyset_v1, row_value_for_sort_key

# ----------------------- #

_DEFAULT_CURSOR_LIMIT = 10


def assert_cursor_projection_includes_sort_keys(
    *,
    return_fields: Sequence[str] | None,
    sort_keys: Sequence[str],
) -> None:
    """Raise when projected fields omit any sort keys used by keyset cursors."""
    if return_fields is None:
        return
    if all(f in return_fields for f in sort_keys):
        return
    raise CoreError(
        "When using return_fields with cursor list, the projection must include "
        "all sort and tie-breaker fields (including id).",
    )


def resolved_cursor_limit(cursor: Mapping[str, Any] | None) -> int:
    """Effective page size (default ``10`` when omitted)."""
    lim = dict(cursor or {}).get("limit")
    if lim is None:
        return _DEFAULT_CURSOR_LIMIT

    return int(lim)


def assemble_keyset_cursor_page(
    fetched: Sequence[Any],
    *,
    cursor: Mapping[str, Any] | None,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    dump_row: Callable[[Any], JsonDict],
) -> tuple[list[Any], bool, str | None, str | None]:
    """Slice ``fetched`` to the requested window and derive opaque cursors.

    Gateways commonly return ``limit + 1`` rows so callers can infer
    ``has_more`` without a separate count query.
    """
    c = dict(cursor or {})
    lim = resolved_cursor_limit(c)

    use_after = c.get("after") is not None
    use_before = c.get("before") is not None

    has_more = len(fetched) > lim
    page_raw = list(fetched)[:lim]

    if has_more and page_raw:
        last = dump_row(page_raw[-1])
        next_tok = encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            values=[row_value_for_sort_key(last, k) for k in sort_keys],
        )
    else:
        next_tok = None

    if page_raw and (use_after or (use_before and has_more)):
        first = dump_row(page_raw[0])
        prev_tok = encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            values=[row_value_for_sort_key(first, k) for k in sort_keys],
        )
    else:
        prev_tok = None

    return page_raw, has_more, next_tok, prev_tok

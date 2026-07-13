"""Cursor token helpers for mock pagination.

Two families live here:

- ``_mock_keyset_*``: keyset (seek) pagination on the shared
  ``encode_keyset_v1``/``decode_keyset_v1`` token machinery, mirroring the
  real document gateways.  Used by the mock document adapter.
- ``_mock_cursor_*``: legacy offset-index tokens still used by the mock
  analytics and search adapters.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from functools import cmp_to_key
from typing import (
    Any,
    cast,
)

from forze.application.contracts.querying import (
    CursorBinding,
    CursorPaginationExpression,
    keyset_page_bounds,
    ordered_compare,
    resolved_cursor_limit,
    row_passes_keyset_seek,
    row_value_for_sort_key,
    validate_cursor_token,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict


def _b64url_json_dumps(payload: dict[str, int]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64url_json_loads_dict(token: str) -> dict[str, int]:
    pad = "=" * (-len(token) % 4)
    raw = base64.urlsafe_b64decode(token + pad)
    data_any: Any = json.loads(raw.decode())

    if not isinstance(data_any, dict) or "s" not in data_any:
        raise ValueError

    return {"s": int(cast(Any, data_any["s"]))}


def _mock_cursor_start_and_limit(  # pyright: ignore[reportPrivateUsage, reportUnusedFunction]
    cursor: CursorPaginationExpression | None,
) -> tuple[int, int]:
    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.validation("Cursor pagination: pass at most one of 'after' or 'before'")

    # Shared coerce + positive-check + clamp to [1, MAX_CURSOR_LIMIT] (a non-int is a 400,
    # a huge value is bounded), keeping the mock's paging identical to the real backends.
    lim = resolved_cursor_limit(c)

    start = 0

    if c.get("after"):
        try:
            payload = _b64url_json_loads_dict(str(c["after"]))

        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise exc.validation("Invalid cursor token") from e

        start = int(payload["s"])

    elif c.get("before"):
        try:
            payload = _b64url_json_loads_dict(str(c["before"]))

        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise exc.validation("Invalid cursor token") from e

        page_start = int(payload["s"])
        start = max(0, page_start - lim)

    return start, int(lim)


def _mock_keyset_parse(  # pyright: ignore[reportPrivateUsage]
    cursor: CursorPaginationExpression | None,
) -> tuple[int, bool, bool]:
    """Return ``(limit, use_after, use_before)`` from a cursor expression."""

    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.validation("Cursor pagination: pass at most one of 'after' or 'before'")

    # Coerced + clamped like the offset path above and the real backends: a non-integer is a
    # clean 400 (not a raw ValueError) and a huge value is clamped to MAX_CURSOR_LIMIT rather
    # than materializing an unbounded in-memory page.
    lim = resolved_cursor_limit(c)

    return lim, c.get("after") is not None, c.get("before") is not None


def _mock_keyset_sort_docs(  # pyright: ignore[reportPrivateUsage]
    docs: list[JsonDict],
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    nulls: Sequence[str],
) -> list[JsonDict]:
    """Total-order *docs* by the keyset sort spec using the canonical key comparison.

    Sorting with :func:`ordered_compare` (per-key direction and null placement) keeps the
    in-memory order consistent with the seek comparison applied to decoded token values,
    so page boundaries match exactly.
    """

    def _cmp(a: JsonDict, b: JsonDict) -> int:
        for key, direction, null_order in zip(sort_keys, directions, nulls, strict=True):
            c = ordered_compare(
                row_value_for_sort_key(a, key),
                row_value_for_sort_key(b, key),
                direction=direction,
                nulls=null_order,
            )

            if c:
                return c

        return 0

    return sorted(docs, key=cmp_to_key(_cmp))


def _mock_keyset_window(  # pyright: ignore[reportPrivateUsage, reportUnusedFunction]
    docs: list[JsonDict],
    *,
    cursor: CursorPaginationExpression | None,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    nulls: Sequence[str],
    binding: CursorBinding | None = None,
) -> tuple[list[JsonDict], bool, str | None, str | None]:
    """Sort *docs*, seek past the cursor's sort values, and trim to one page.

    Mirrors the real gateways: ``after``/``before`` tokens are validated with
    the shared :func:`validate_cursor_token`, rows are filtered with
    :func:`row_passes_keyset_seek` (never sliced by index), and the page plus
    next/prev tokens come from the shared :func:`keyset_page_bounds`.
    *binding* is the (spec, tenant, filter) context threaded through both the
    verify and mint sides when cursor signing is on.
    Returns ``(page_docs, has_more, next_cursor, prev_cursor)``.
    """

    lim, use_after, use_before = _mock_keyset_parse(cursor)
    c = dict(cursor or {})

    ordered = _mock_keyset_sort_docs(docs, sort_keys=sort_keys, directions=directions, nulls=nulls)

    if use_after or use_before:
        token = str(c["after" if use_after else "before"])
        tv = validate_cursor_token(
            token,
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            binding=binding,
        )
        ordered = [
            row
            for row in ordered
            if row_passes_keyset_seek(
                row,
                sort_keys=sort_keys,
                directions=directions,
                nulls=nulls,
                cursor_values=tv,
                after=use_after,
            )
        ]

    if use_before:
        # Fetch order is flipped for ``before`` pages (like ORDER BY ... DESC);
        # keyset_page_bounds reverses the window back into ascending order.
        ordered = list(reversed(ordered))

    return keyset_page_bounds(
        ordered[: lim + 1],
        lim,
        sort_keys=sort_keys,
        directions=directions,
        nulls=nulls,
        binding=binding,
        use_after=use_after,
        use_before=use_before,
    )


def _mock_cursor_tokens(  # pyright: ignore[reportPrivateUsage, reportUnusedFunction]
    start: int,
    page_len: int,
    *,
    has_more: bool,
) -> tuple[str | None, str | None]:
    next_c: str | None = None
    prev_c: str | None = None

    if has_more:
        next_c = _b64url_json_dumps({"s": start + page_len})

    if start > 0:
        prev_c = _b64url_json_dumps({"s": start})

    return next_c, prev_c

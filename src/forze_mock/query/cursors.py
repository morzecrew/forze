"""Cursor token helpers for mock pagination."""

from __future__ import annotations

import base64
import json
from typing import (
    Any,
    cast,
)

from forze.application.contracts.querying import (
    CursorPaginationExpression,
)
from forze.base.exceptions import exc


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


def _mock_cursor_start_and_limit(  # type: ignore[reportPrivateUsage]
    cursor: CursorPaginationExpression | None,
    *,
    default_limit: int = 10,
) -> tuple[int, int]:
    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.internal("Cursor pagination: pass at most one of 'after' or 'before'")

    lim_raw = c.get("limit")
    lim: int = default_limit if lim_raw is None else int(cast(Any, lim_raw))

    if lim < 1:
        raise exc.internal("Cursor pagination 'limit' must be positive")

    start = 0

    if c.get("after"):
        try:
            payload = _b64url_json_loads_dict(str(c["after"]))

        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise exc.internal("Invalid cursor token") from e

        start = int(payload["s"])

    elif c.get("before"):
        try:
            payload = _b64url_json_loads_dict(str(c["before"]))

        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise exc.internal("Invalid cursor token") from e

        page_start = int(payload["s"])
        start = max(0, page_start - lim)

    return start, int(lim)


def _mock_cursor_tokens(  # type: ignore[reportPrivateUsage]
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

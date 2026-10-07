"""Pagination and stream safety limits for document adapters."""

from collections.abc import Mapping
from typing import Any

from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_MAX_SCAN_PAGES = 100_000
DEFAULT_MAX_STREAM_PAGES = 100_000
DEFAULT_MAX_CHUNKED_COMMAND_PAGES = 100_000
DEFAULT_MAX_FETCH_ALL_PAGES = 100_000

# Inclusive bounds for gateway batch/stream sizing. ``batch_size`` is static
# adapter config (rejected at construction when out of range); stream chunk size
# is a per-call argument (clamped to the nearest bound).
MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 20_000
MIN_STREAM_CHUNK_SIZE = 10
MAX_STREAM_CHUNK_SIZE = 20_000

# ....................... #


def check_page_limit(*, pages: int, max_pages: int | None, label: str) -> None:
    """Raise when an internal pagination loop exceeds *max_pages*."""

    if max_pages is not None and pages >= max_pages:
        raise exc.precondition(f"{label} exceeded max_pages={max_pages}")


# ....................... #


def assert_cursor_advanced(
    *,
    prev_cursor: str | None,
    next_cursor: str | None,
) -> None:
    """Raise when opaque cursor pagination fails to advance."""

    if next_cursor is not None and prev_cursor is not None and next_cursor == prev_cursor:
        raise exc.internal("Cursor pagination did not advance")


# ....................... #


def _exact_int(raw: Any) -> int:
    """*raw* as an integer, refusing what ``int()`` would take silently.

    ``int()`` reads ``True`` as 1, truncates ``1.9`` or ``Decimal("1.5")`` to 1, and turns an
    empty container into nothing at all once a caller writes ``raw or 0``.
    """

    if isinstance(raw, bool):
        raise TypeError(raw)

    if isinstance(raw, str):
        return int(raw)

    value = int(raw)

    if value != raw:
        raise ValueError(raw)

    return value


# ....................... #


def page_offset(pagination: Mapping[str, Any]) -> int:
    """The offset *pagination* asks for, as an integer (``0`` when it gives none).

    :raises CoreException: ``precondition`` when the offset is not a non-negative integer: a
        negative one would slice from the end, and a backend answers either with a server error.
    """

    raw = pagination.get("offset")

    if raw is None:
        return 0

    try:
        offset = _exact_int(raw)

    except (TypeError, ValueError, ArithmeticError):
        offset = -1

    if offset < 0:
        raise exc.precondition(f"Pagination offset must be a non-negative integer, got {raw!r}.")

    return offset


# ....................... #


def page_limit(pagination: Mapping[str, Any]) -> int | None:
    """The limit *pagination* asks for, as an integer (``None`` when it gives none).

    :raises CoreException: ``precondition`` when the limit is not a non-negative integer: a
        negative one would slice from the end, and a backend answers either with a server error.
    """

    raw = pagination.get("limit")

    if raw is None:
        return None

    try:
        limit = _exact_int(raw)

    except (TypeError, ValueError, ArithmeticError):
        limit = -1

    if limit < 0:
        raise exc.precondition(f"Pagination limit must be a non-negative integer, got {raw!r}.")

    return limit

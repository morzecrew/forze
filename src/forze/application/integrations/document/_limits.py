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


def page_offset(pagination: Mapping[str, Any]) -> int:
    """The offset *pagination* asks for, as an integer (``0`` when it gives none).

    :raises CoreException: ``precondition`` when the offset is not a non-negative integer: a
        negative one would slice from the end, and a backend answers either with a server error.
    """

    raw = pagination.get("offset") or 0

    try:
        offset = int(raw)

    except (TypeError, ValueError):
        offset = -1

    if offset < 0:
        raise exc.precondition(f"Pagination offset must be a non-negative integer, got {raw!r}.")

    return offset

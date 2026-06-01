"""Pagination and stream safety limits for document adapters."""

from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_MAX_SCAN_PAGES = 100_000
DEFAULT_MAX_STREAM_PAGES = 100_000
DEFAULT_MAX_CHUNKED_COMMAND_PAGES = 100_000
DEFAULT_MAX_FETCH_ALL_PAGES = 100_000

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

    if (
        next_cursor is not None
        and prev_cursor is not None
        and next_cursor == prev_cursor
    ):
        raise exc.internal("Cursor pagination did not advance")

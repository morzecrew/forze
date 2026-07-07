"""Query helpers for in-memory mock adapters."""

from __future__ import annotations

from forze_mock.query._types import _MISSING  # pyright: ignore[reportPrivateUsage]
from forze_mock.query.cursors import (
    _mock_cursor_start_and_limit,  # pyright: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # pyright: ignore[reportPrivateUsage]
)
from forze_mock.query.matching import (
    _aggregate_docs,  # pyright: ignore[reportPrivateUsage]
    _match_expr,  # pyright: ignore[reportPrivateUsage]
    _match_field,  # pyright: ignore[reportPrivateUsage]
    _match_filters,  # pyright: ignore[reportPrivateUsage]
    _path_get,  # pyright: ignore[reportPrivateUsage]
    _path_text,  # pyright: ignore[reportPrivateUsage]
    _project,  # pyright: ignore[reportPrivateUsage]
    _sort_docs,  # pyright: ignore[reportPrivateUsage]
)

__all__ = [
    "_MISSING",
    "_path_get",
    "_path_text",
    "_match_field",
    "_match_expr",
    "_match_filters",
    "_project",
    "_sort_docs",
    "_aggregate_docs",
    "_mock_cursor_start_and_limit",
    "_mock_cursor_tokens",
]

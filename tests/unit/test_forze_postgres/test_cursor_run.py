"""Tests for :mod:`forze_postgres.adapters.search._cursor_run`."""

import pytest

from forze.base.exceptions import CoreException

from forze_postgres.adapters.search._cursor_run import parse_search_cursor


def test_parse_search_cursor_defaults() -> None:
    lim, use_after, use_before = parse_search_cursor(None)
    assert lim == 10
    assert use_after is False
    assert use_before is False


def test_parse_search_cursor_after() -> None:
    lim, use_after, use_before = parse_search_cursor({"after": "tok", "limit": 5})
    assert lim == 5
    assert use_after is True
    assert use_before is False


def test_parse_search_cursor_before() -> None:
    lim, use_after, use_before = parse_search_cursor({"before": "tok"})
    assert lim == 10
    assert use_after is False
    assert use_before is True


def test_parse_search_cursor_rejects_both_tokens() -> None:
    with pytest.raises(CoreException, match="at most one"):
        parse_search_cursor({"after": "a", "before": "b"})


def test_parse_search_cursor_rejects_non_positive_limit() -> None:
    with pytest.raises(CoreException, match="positive"):
        parse_search_cursor({"limit": 0})


def test_parse_search_cursor_rejects_non_integer_limit() -> None:
    # A non-integer is a clean 400 via the shared ``resolved_cursor_limit`` — not a raw
    # ``ValueError`` from a bare ``int('abc')``.
    with pytest.raises(CoreException, match="must be an integer"):
        parse_search_cursor({"limit": "abc"})


def test_parse_search_cursor_clamps_oversized_limit() -> None:
    # An enormous client-supplied limit is clamped rather than reaching the backend as an
    # unbounded fetch.
    from forze.application.contracts.querying.pagination.cursor_page import (
        MAX_CURSOR_LIMIT,
    )

    lim, _use_after, _use_before = parse_search_cursor({"limit": 1_000_000_000})
    assert lim == MAX_CURSOR_LIMIT

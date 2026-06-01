"""Tests for document adapter pagination safety limits."""

from __future__ import annotations

import pytest

from forze.application.integrations.document._limits import (
    assert_cursor_advanced,
    check_page_limit,
)
from forze.base.exceptions import CoreException


def test_check_page_limit_raises() -> None:
    with pytest.raises(CoreException, match="max_pages=2"):
        check_page_limit(pages=2, max_pages=2, label="test")


def test_assert_cursor_advanced_raises() -> None:
    with pytest.raises(CoreException, match="did not advance"):
        assert_cursor_advanced(prev_cursor="same", next_cursor="same")


@pytest.mark.asyncio
async def test_stream_stall_detection_via_helper() -> None:
    with pytest.raises(CoreException, match="did not advance"):
        assert_cursor_advanced(prev_cursor="tok-a", next_cursor="tok-a")

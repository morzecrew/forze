"""Unit tests for the offset pagination request DTO bounds."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from forze_kits.dto.paginated import MAX_PAGE_SIZE, Pagination

# ----------------------- #


class TestPaginationBounds:
    def test_defaults_are_valid(self) -> None:
        pagination = Pagination()

        assert pagination.page == 1
        assert pagination.size == 10

    @pytest.mark.parametrize("size", [1, 10, MAX_PAGE_SIZE])
    def test_accepts_in_range_size(self, size: int) -> None:
        assert Pagination(size=size).size == size

    @pytest.mark.parametrize("size", [0, -1, MAX_PAGE_SIZE + 1, 10**9])
    def test_rejects_out_of_range_size(self, size: int) -> None:
        # The size is untrusted boundary input: an over-large value must be a clean
        # validation error, not an unbounded result-set materialization downstream.
        with pytest.raises(ValidationError):
            Pagination(size=size)

    @pytest.mark.parametrize("page", [1, 2, 10**6])
    def test_accepts_valid_page(self, page: int) -> None:
        assert Pagination(page=page).page == page

    @pytest.mark.parametrize("page", [0, -1])
    def test_rejects_non_positive_page(self, page: int) -> None:
        with pytest.raises(ValidationError):
            Pagination(page=page)

    def test_offset_math_stays_one_based(self) -> None:
        limit, offset = Pagination(page=3, size=20).offset_limit

        assert (limit, offset) == (20, 40)

    def test_first_page_has_zero_offset(self) -> None:
        limit, offset = Pagination(page=1, size=MAX_PAGE_SIZE).offset_limit

        assert (limit, offset) == (MAX_PAGE_SIZE, 0)

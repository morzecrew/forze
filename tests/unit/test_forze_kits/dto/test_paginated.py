"""Unit tests for the offset pagination request DTO bounds and response DTO mapping."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from forze.application.contracts.base import CursorPage, Page
from forze_kits.dto.paginated import (
    MAX_PAGE_SIZE,
    CursorPaginated,
    Paginated,
    Pagination,
    ProjectedCursorPaginated,
    ProjectedPaginated,
)

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


# ....................... #


class _Hit(BaseModel):
    id: str


class TestFromPageKeepsAbstention:
    # The response DTOs are the HTTP boundary: a reason the page carries must survive
    # the conversion, or the caller can never see it.

    def test_offset_dtos(self) -> None:
        page = Page[_Hit](hits=[], page=1, size=10, count=0, abstention="not_permitted")

        assert Paginated.from_page(page).abstention == "not_permitted"
        assert ProjectedPaginated.from_page(
            Page(hits=[], page=1, size=10, count=0, abstention="no_match")
        ).abstention == "no_match"

    def test_cursor_dtos(self) -> None:
        page = CursorPage[_Hit](
            hits=[], next_cursor=None, prev_cursor=None, abstention="ambiguous"
        )

        assert CursorPaginated.from_page(page).abstention == "ambiguous"
        assert ProjectedCursorPaginated.from_page(
            CursorPage(hits=[], next_cursor=None, prev_cursor=None, abstention="no_match")
        ).abstention == "no_match"

    def test_default_stays_none(self) -> None:
        page = Page[_Hit](hits=[], page=1, size=10, count=0)

        assert Paginated.from_page(page).abstention is None

    def test_direct_construction_rejects_reason_beside_hits(self) -> None:
        # The same invariant the page value objects enforce, held at the HTTP boundary:
        # a DTO built directly must not serialize a contradictory response.
        with pytest.raises(ValidationError, match="only valid on an empty page"):
            ProjectedPaginated(
                hits=[{"id": "a"}], page=1, size=1, count=1, abstention="no_match"
            )

        with pytest.raises(ValidationError, match="only valid on an empty page"):
            ProjectedCursorPaginated(
                hits=[{"id": "a"}],
                next_cursor=None,
                prev_cursor=None,
                abstention="not_permitted",
            )

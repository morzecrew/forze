"""Unit tests for application DTOs used by FastAPI-mounted endpoints (e.g. Pagination)."""

import pytest

from forze.application.dto import Pagination


# ----------------------- #


class TestPagination:
    """Tests for Pagination DTO (used by search/list request bodies)."""

    def test_attrs_frozen(self) -> None:
        """Pagination is frozen and has page/size."""
        p = Pagination(page=2, size=20)
        assert p.page == 2
        assert p.size == 20

    def test_immutable(self) -> None:
        """Pagination cannot be mutated."""
        p = Pagination(page=1, size=10)
        with pytest.raises(Exception):
            p.page = 2  # type: ignore[misc]

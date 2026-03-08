"""Unit tests for forze_fastapi.routing.params."""

import pytest

from forze_fastapi.routing.params import Pagination, pagination


# ----------------------- #


class TestPagination:
    """Tests for Pagination."""

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


class TestPaginationDependency:
    """Tests for pagination dependency function."""

    def test_returns_pagination(self) -> None:
        """pagination returns Pagination instance."""
        result = pagination(page=1, size=10)
        assert isinstance(result, Pagination)
        assert result.page == 1
        assert result.size == 10

    def test_defaults(self) -> None:
        """pagination returns Pagination when called with defaults."""
        result = pagination()
        assert isinstance(result, Pagination)
        # When called outside FastAPI, Query defaults may be used; explicit args work
        explicit = pagination(page=1, size=10)
        assert explicit.page == 1
        assert explicit.size == 10

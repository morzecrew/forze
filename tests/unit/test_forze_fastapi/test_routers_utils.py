"""Unit tests for forze_fastapi.endpoints._utils."""

from forze_fastapi.endpoints._utils import path_coerce

# ----------------------- #


class TestPathCoerce:
    """Tests for path_coerce."""

    def test_adds_leading_slash(self) -> None:
        assert path_coerce("items") == "/items"

    def test_preserves_leading_slash(self) -> None:
        assert path_coerce("/items") == "/items"

    def test_strips_trailing_slash(self) -> None:
        assert path_coerce("/items/") == "/items"
        assert path_coerce("items/") == "/items"

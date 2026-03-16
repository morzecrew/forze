from psycopg import sql

from forze_postgres.kernel.query.utils import PsycopgPositionalBinder


class TestPsycopgPositionalBinder:
    """Tests for PsycopgPositionalBinder."""

    def test_initialization(self) -> None:
        """Test that the binder initializes with empty params."""
        binder = PsycopgPositionalBinder()
        assert binder.params == []
        assert binder.values() == []

    def test_add_single_param(self) -> None:
        """Test adding a single parameter."""
        binder = PsycopgPositionalBinder()
        placeholder = binder.add(1)

        assert isinstance(placeholder, sql.Placeholder)
        assert binder.params == [1]
        assert binder.values() == [1]

    def test_add_multiple_params(self) -> None:
        """Test adding multiple parameters of different types."""
        binder = PsycopgPositionalBinder()
        binder.add("first")
        binder.add(2)
        binder.add(None)
        binder.add({"key": "value"})

        expected = ["first", 2, None, {"key": "value"}]
        assert binder.params == expected
        assert binder.values() == expected

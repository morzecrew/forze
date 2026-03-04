"""Unit tests for forze.base.primitives.buffer."""

import pytest

from forze.base.primitives.buffer import ContextualBuffer

# ----------------------- #


class TestContextualBuffer:
    """Tests for ContextualBuffer."""

    def test_peek_empty_returns_empty_list(self) -> None:
        buf = ContextualBuffer[int]()
        assert buf.peek() == []

    def test_push_extends_buffer(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1, 2])
        assert buf.peek() == [1, 2]

    def test_push_multiple_extends_in_place(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1])
        buf.push([2, 3])
        assert buf.peek() == [1, 2, 3]

    def test_clear_empties_buffer(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1, 2, 3])
        buf.clear()
        assert buf.peek() == []

    def test_pop_returns_and_clears(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1, 2, 3])
        result = buf.pop()
        assert result == [1, 2, 3]
        assert buf.peek() == []

    def test_scope_clears_on_entry_and_restores_on_exit(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1, 2, 3])

        with buf.scope():
            assert buf.peek() == []
            buf.push([4, 5])
            assert buf.peek() == [4, 5]

        assert buf.peek() == [1, 2, 3]

    def test_scope_restores_after_exception(self) -> None:
        buf = ContextualBuffer[int]()
        buf.push([1])

        with pytest.raises(ValueError):
            with buf.scope():
                buf.push([2])
                raise ValueError("oops")

        assert buf.peek() == [1]

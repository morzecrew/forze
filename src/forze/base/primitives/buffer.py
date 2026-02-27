from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Sequence, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ContextualBuffer[T]:
    """Generic class for buffering objects during a task execution."""

    __buffer: ContextVar[list[T]] = attrs.field(
        factory=lambda: ContextVar("buffer"),
        init=False,
        repr=False,
    )

    # ....................... #

    def peek(self) -> list[T]:
        """Get current buffered objects without clearing the buffer.

        Returns:
            list[T]: List of currently buffered objects.
        """

        return self.__buffer.get([])

    # ....................... #

    def clear(self) -> None:
        """Clear all buffered objects from the context variable."""

        self.__buffer.set([])

    # ....................... #

    def push(self, e: Sequence[T]) -> None:
        """Add objects to the buffer.

        Extends the current buffer with the provided objects.

        Args:
            e (list[T]): List of objects to add to the buffer.
        """

        buf = self.peek()
        buf.extend(list(e))

        self.__buffer.set(buf)

    # ....................... #

    def pop(self) -> list[T]:
        """Get all buffered objects and clear the buffer.

        Returns:
            list[T]: List of all buffered objects (buffer is cleared after this call).
        """

        buf = self.peek()
        self.clear()

        return buf

    # ....................... #

    @contextmanager
    def scope(self) -> Iterator[None]:
        """Context manager for the buffer"""

        token = self.__buffer.set([])

        try:
            yield

        finally:
            self.__buffer.reset(token)

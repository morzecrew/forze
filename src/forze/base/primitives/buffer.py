"""Context-scoped buffer for collecting objects during a logical task.

Uses :class:`contextvars.ContextVar` so each async task or thread has its own
buffer. Use :meth:`ContextualBuffer.scope` to isolate buffers within nested
operations.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Sequence, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ContextualBuffer[T]:
    """Context-scoped buffer for collecting objects during task execution.

    Each async task or thread gets its own buffer via a :class:`~contextvars.ContextVar`.
    Use :meth:`scope` to create a nested scope that clears on exit.
    """

    __buffer: ContextVar[list[T]] = attrs.field(
        factory=lambda: ContextVar("buffer"),
        init=False,
        repr=False,
    )

    # ....................... #

    def peek(self) -> list[T]:
        """Return currently buffered objects without clearing the buffer."""
        return self.__buffer.get([])

    # ....................... #

    def clear(self) -> None:
        """Clear all buffered objects from the context variable."""

        self.__buffer.set([])

    # ....................... #

    def push(self, e: Sequence[T]) -> None:
        """Append objects to the buffer. Extends the current list in place."""
        buf = self.peek()
        buf.extend(list(e))

        self.__buffer.set(buf)

    # ....................... #

    def pop(self) -> list[T]:
        """Return all buffered objects and clear the buffer."""
        buf = self.peek()
        self.clear()

        return buf

    # ....................... #

    @contextmanager
    def scope(self) -> Iterator[None]:
        """Context manager that provides an isolated buffer scope.

        On entry, the buffer is cleared. On exit, the previous buffer state
        is restored.
        """
        token = self.__buffer.set([])

        try:
            yield

        finally:
            self.__buffer.reset(token)

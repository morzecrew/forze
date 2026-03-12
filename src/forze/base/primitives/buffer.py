"""Context-scoped buffer for collecting objects during a logical task.

Uses :class:`contextvars.ContextVar` so each async task or thread has its own
buffer. Use :meth:`ContextualBuffer.scope` to isolate buffers within nested
operations.
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Sequence, final

import attrs

from ..logging import log_section

# ----------------------- #

logger = logging.getLogger(__name__)

# ....................... #


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

        logger.debug("Clearing contextual buffer")

        self.__buffer.set([])

    # ....................... #

    def push(self, e: Sequence[T]) -> None:
        """Append objects to the buffer. Extends the current list in place."""

        logger.debug("Pushing %d item(s) to contextual buffer", len(e))

        buf = self.peek()
        buf.extend(list(e))

        self.__buffer.set(buf)

    # ....................... #

    def pop(self) -> list[T]:
        """Return all buffered objects and clear the buffer."""

        buf = self.peek()

        logger.debug("Popping %d item(s) from contextual buffer", len(buf))

        self.clear()

        return buf

    # ....................... #

    @contextmanager
    def scope(self) -> Iterator[None]:
        """Context manager that provides an isolated buffer scope.

        On entry, the buffer is cleared. On exit, the previous buffer state
        is restored.
        """

        logger.debug("Entering contextual buffer scope")

        with log_section():
            token = self.__buffer.set([])

            try:
                yield

            finally:
                buf = self.peek()

                logger.debug(
                    "Leaving contextual buffer scope (%d buffered item(s))",
                    len(buf),
                )

                self.__buffer.reset(token)

"""Context-scoped buffer for collecting objects during a logical task."""

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import final

import attrs

from .._logger import logger

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ContextualBuffer[T]:
    """Context-scoped buffer for collecting objects during task execution.

    Each async task or thread gets its own buffer via a :class:`~contextvars.ContextVar`.
    Use :meth:`scope` to create a nested scope that clears on exit.

    The per-instance :class:`~contextvars.ContextVar` is intentional and safe under the
    usage contract that one buffer instance lives for the whole runtime scope (e.g. one
    per outbox route per execution context). CPython recommends module-level
    ``ContextVar``s because variables created in churning objects cannot be removed from
    still-referenced ``Context`` objects — hence the constraint: never instantiate a
    ``ContextualBuffer`` per request or per operation.
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

        logger.trace("Clearing contextual buffer")

        self.__buffer.set([])

    # ....................... #

    def push(self, e: Sequence[T]) -> None:
        """Append objects to the buffer. Extends the current list in place."""

        logger.trace("Pushing %s item(s) to contextual buffer", len(e))

        buf = self.peek()
        buf.extend(list(e))

        self.__buffer.set(buf)

    # ....................... #

    def pop(self) -> list[T]:
        """Return all buffered objects and clear the buffer."""

        buf = self.peek()

        logger.trace("Popping %s item(s) from contextual buffer", len(buf))

        self.clear()

        return buf

    # ....................... #

    @contextmanager
    def scope(self) -> Iterator[None]:
        """Context manager that provides an isolated buffer scope.

        On entry, the buffer is cleared. On exit, the previous buffer state
        is restored.
        """

        logger.trace("Entering contextual buffer scope")
        token = self.__buffer.set([])

        try:
            yield

        finally:
            buf = self.peek()

            logger.trace(
                "Leaving contextual buffer scope (%s buffered item(s))",
                len(buf),
            )

            self.__buffer.reset(token)


# ....................... #


@attrs.define(slots=True)
class ContextVarTrace[T]:
    """Lazily-created, per-task trace value stored in a :class:`~contextvars.ContextVar`.

    A fresh ``T`` is built via *factory* on first access within a task and reused
    for the remainder of that task. Backs the recording deps tracers.
    """

    factory: Callable[[], T]
    """Builds a new trace value when one does not yet exist for the task."""

    var_name: str
    """Name for the underlying :class:`~contextvars.ContextVar`."""

    _var: ContextVar[T | None] = attrs.field(
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self._var = ContextVar(self.var_name, default=None)

    # ....................... #

    def init_task(self) -> None:
        """Ensure a trace value exists for the current task."""

        if self._var.get() is None:
            self._var.set(self.factory())

    # ....................... #

    def get_or_create(self) -> T:
        """Return the task trace value, creating it when absent."""

        trace = self._var.get()

        if trace is None:
            trace = self.factory()
            self._var.set(trace)

        return trace

    # ....................... #

    def snapshot(self) -> T | None:
        """Return the current task trace value, or ``None`` when unset."""

        return self._var.get()

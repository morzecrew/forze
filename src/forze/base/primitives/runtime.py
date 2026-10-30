"""Thread-safe runtime variables for application-wide singletons."""

from collections.abc import Iterator
from contextlib import contextmanager
from threading import Lock, RLock
from typing import final

import attrs

from .._logger import logger
from ..exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, eq=False)
class ScopeFallback[T]:
    """Values the open runtime scopes hold process-wide: the fallback of a context binding none.

    A scope binds its values in context variables, which reach its own task and the tasks it
    starts, never a task created from another context — and an ASGI server runs the lifespan
    holding the scope in a task of its own, then creates every request task from the server's
    context. A lookup that finds its context variable unset reads :meth:`held` instead.
    """

    __lock: Lock = attrs.field(factory=Lock, init=False)
    __held: tuple[tuple[object, T], ...] = attrs.field(default=(), init=False)

    # ....................... #

    @contextmanager
    def hold(self, value: T) -> Iterator[None]:
        """Hold *value* for the block, released on exit whatever order the holders exit in."""

        entry = (object(), value)  # by identity: two holders may hold equal values

        with self.__lock:
            self.__held = (*self.__held, entry)

        try:
            yield

        finally:
            with self.__lock:
                self.__held = tuple(e for e in self.__held if e is not entry)

    # ....................... #

    def held(self) -> tuple[T, ...]:
        """The values held now, oldest holder first."""

        return tuple(value for _, value in self.__held)


# ....................... #


@attrs.define(slots=True)
class RuntimeVar[T: object]:
    """Thread-safe runtime variable that can be set once and accessed globally.

    Used to store application-wide runtime values (e.g. an ``AppContext``)
    initialized during startup and accessed throughout the application lifecycle.
    Raises :exc:`~forze.base.errors.exc.internal` on invalid operations.
    """

    name: str
    """Name identifier for the runtime variable (used in error messages)."""

    # Non initable fields
    __lock: RLock = attrs.field(factory=RLock, init=False)
    """Thread lock for thread-safe operations."""

    __value: T | None = attrs.field(default=None, init=False)
    """The stored value (``None`` until set)."""

    # ....................... #

    def set_once(self, value: T) -> None:
        """Set the runtime value once. Thread-safe; subsequent calls raise :exc:`exc.internal`."""

        if value is None:
            raise exc.internal(f"Value cannot be None for '{self.name}'")

        logger.trace(
            "Setting runtime variable '%s' with value type %s",
            self.name,
            type(value).__name__,
        )

        with self.__lock:
            if self.__value is not None:
                raise exc.internal(f"Value is already set for runtime variable '{self.name}'")

            self.__value = value

    # ....................... #

    def get(self) -> T:
        """Return the stored value. Raises :exc:`exc.internal` if not yet set."""

        if self.__value is None:
            raise exc.internal(f"Value is not set for '{self.name}'")

        return self.__value

    # ....................... #

    def reset(self) -> None:
        """Clear the stored value so it can be set again. Thread-safe. Useful for testing."""

        logger.trace("Resetting runtime variable '%s'", self.name)

        with self.__lock:
            self.__value = None

"""Thread-safe runtime variables for application-wide singletons."""

from threading import RLock
from typing import Optional

import attrs

from ..errors import CoreError

# ----------------------- #


@attrs.define(slots=True)
class RuntimeVar[T: object]:
    """Thread-safe runtime variable that can be set once and accessed globally.

    Used to store application-wide runtime values (e.g. an ``AppContext``)
    initialized during startup and accessed throughout the application lifecycle.
    Raises :exc:`~forze.base.errors.CoreError` on invalid operations.

    Example::

        app_rt_var: RuntimeVar[AppContext] = RuntimeVar("app_rt")

        # During startup
        app_rt_var.set_once(context)

        # Later, anywhere in the application
        ctx = app_rt_var.get()
    """

    name: str
    """Name identifier for the runtime variable (used in error messages)."""

    # Non initable fields
    __lock: RLock = attrs.field(factory=RLock, init=False)
    """Thread lock for thread-safe operations."""

    __value: Optional[T] = attrs.field(default=None, init=False)
    """The stored value (``None`` until set)."""

    # ....................... #

    def set_once(self, value: T) -> None:
        """Set the runtime value once. Thread-safe; subsequent calls raise :exc:`CoreError`."""

        if value is None:
            raise CoreError(f"Value cannot be None for {self.name}")

        with self.__lock:
            if self.__value is not None:
                raise CoreError(
                    f"Value is already set for runtime variable {self.name}"
                )

            self.__value = value

    # ....................... #

    def get(self) -> T:
        """Return the stored value. Raises :exc:`CoreError` if not yet set."""

        if self.__value is None:
            raise CoreError(f"Value is not set for {self.name}")

        return self.__value

    # ....................... #

    def reset(self) -> None:
        """Clear the stored value so it can be set again. Thread-safe. Useful for testing."""

        with self.__lock:
            self.__value = None

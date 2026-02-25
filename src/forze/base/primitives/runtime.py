from threading import RLock
from typing import Optional

import attrs

from ..errors import CoreError

# ----------------------- #


@attrs.define(slots=True)
class RuntimeVar[T: object]:
    """Thread-safe runtime variable that can be set once and accessed globally.

    Used to store application-wide runtime values (for example an
    ``AppContext``) that are initialized during application startup and
    accessed throughout the application lifecycle.

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
    """The stored value (None until set)."""

    # ....................... #

    def set_once(self, value: T) -> None:
        """Set the runtime value once.

        Thread-safe operation that ensures the value can only be set once.
        Subsequent calls will raise RuntimeError.

        Args:
            value: The value to store.

        Raises:
            CoreError: If the value has already been set or nullable value is provided.
        """

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
        """Get the stored runtime value.

        Returns:
            The stored value.

        Raises:
            CoreError: If the value has not been set yet.
        """

        if self.__value is None:
            raise CoreError(f"Value is not set for {self.name}")

        return self.__value

    # ....................... #

    def reset(self) -> None:
        """Reset the runtime value to None.

        Thread-safe operation that clears the stored value, allowing it to be
        set again (useful for testing or cleanup).
        """

        with self.__lock:
            self.__value = None
